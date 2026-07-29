package main

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
)

// ── 按用户模型配置（BYO 模型）─────────────────────────────────────────────
//
// 前端把用户的 embedding/rerank 配置（含用户自己的 API key）整体加密后写入
// user_model_configs（AES-256-GCM，密钥 = SHA-256(MODEL_CONFIG_SECRET)，
// 密文布局 base64(nonce[12] || ciphertext||tag)，与前端 lib/model-config-crypto
// 保持一致）。relay 解密后以 model_config 参数注入 LCE 工具调用（同 tenant_id
// 的注入方式），LCE 侧按请求生效。
//
// 换模型必须重建索引：fingerprint 是配置身份的明文指纹，applied_fingerprint
// 是 relay 已完成"清租户索引"的指纹。两者不一致时（含恢复平台默认），relay
// 在下一个请求上惰性调用 codebase_clear_index 清空租户索引并推进
// applied_fingerprint；插件随后的 find_missing 会看到空索引并自动全量重传。
//
// MODEL_CONFIG_SECRET 未设置时整个特性关闭（不查表、不注入）。

const (
	modelConfigCacheTTL   = 5 * time.Minute
	modelConfigApplyLock  = 5 * time.Minute
	modelConfigNoRowValue = "0"
)

var modelConfigKey []byte // nil = 特性关闭

func initModelConfigKey() {
	secret := getEnv("MODEL_CONFIG_SECRET", "")
	if secret == "" {
		log.Println("[MODELCFG] MODEL_CONFIG_SECRET not set; per-user model config disabled")
		return
	}
	sum := sha256.Sum256([]byte(secret))
	modelConfigKey = sum[:]
}

func migrateModelConfigTables() error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS user_model_configs (
			user_id VARCHAR(255) PRIMARY KEY,
			config_enc TEXT,
			fingerprint VARCHAR(80) NOT NULL,
			applied_fingerprint VARCHAR(80),
			updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to migrate user_model_configs table: %w", err)
	}
	return nil
}

type userModelConfigRow struct {
	Enc         string `json:"enc"`
	Fingerprint string `json:"fp"`
	Applied     string `json:"applied"`
}

// getUserModelConfigRow 带 Redis 缓存读取配置行；无配置返回 nil。
// 前端保存配置后会删除 modelcfg:{user} 缓存使其立即可见。
func getUserModelConfigRow(ctx context.Context, userID string) *userModelConfigRow {
	cacheKey := "modelcfg:" + userID
	if v, err := redisClient.Get(ctx, cacheKey).Result(); err == nil {
		if v == modelConfigNoRowValue {
			return nil
		}
		var row userModelConfigRow
		if json.Unmarshal([]byte(v), &row) == nil {
			return &row
		}
	}

	var enc, applied sql.NullString
	var fingerprint string
	err := db.QueryRow(
		`SELECT config_enc, fingerprint, applied_fingerprint FROM user_model_configs WHERE user_id = $1`,
		userID,
	).Scan(&enc, &fingerprint, &applied)
	switch {
	case err == sql.ErrNoRows:
		redisClient.Set(ctx, cacheKey, modelConfigNoRowValue, modelConfigCacheTTL)
		return nil
	case err != nil:
		log.Printf("[MODELCFG] lookup failed (user=%s): %v", userID, err)
		return nil
	}

	row := &userModelConfigRow{Enc: enc.String, Fingerprint: fingerprint, Applied: applied.String}
	if data, err := json.Marshal(row); err == nil {
		redisClient.Set(ctx, cacheKey, string(data), modelConfigCacheTTL)
	}
	return row
}

func decryptModelConfig(enc string) (map[string]interface{}, error) {
	data, err := base64.StdEncoding.DecodeString(enc)
	if err != nil {
		return nil, fmt.Errorf("base64 decode: %w", err)
	}
	block, err := aes.NewCipher(modelConfigKey)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(data) < gcm.NonceSize()+gcm.Overhead() {
		return nil, fmt.Errorf("ciphertext too short")
	}
	plain, err := gcm.Open(nil, data[:gcm.NonceSize()], data[gcm.NonceSize():], nil)
	if err != nil {
		return nil, fmt.Errorf("gcm open: %w", err)
	}
	var cfg map[string]interface{}
	if err := json.Unmarshal(plain, &cfg); err != nil {
		return nil, fmt.Errorf("config json: %w", err)
	}
	return cfg, nil
}

func releaseModelConfigApplyLock(ctx context.Context, lockKey, token string) {
	const compareAndDelete = `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		end
		return 0
	`
	if err := redisClient.Eval(ctx, compareAndDelete, []string{lockKey}, token).Err(); err != nil {
		log.Printf("[MODELCFG] apply lock release failed (user=%s): %v", userIDFromModelConfigLockKey(lockKey), err)
	}
}

func userIDFromModelConfigLockKey(lockKey string) string {
	const prefix = "modelcfg:apply:"
	if len(lockKey) >= len(prefix) && lockKey[:len(prefix)] == prefix {
		return lockKey[len(prefix):]
	}
	return lockKey
}

// applyModelConfigChange clears both LCE data and relay snapshots while holding
// the same per-user database lock used by indexing jobs.
func applyModelConfigChange(ctx context.Context, userID string, row *userModelConfigRow) error {
	lockKey := "modelcfg:apply:" + userID
	lockToken := uuid.NewString()
	ok, err := redisClient.SetNX(ctx, lockKey, lockToken, modelConfigApplyLock).Result()
	if err != nil {
		return fmt.Errorf("model config apply lock failed: %w", err)
	}
	if !ok {
		return fmt.Errorf("model config update is already in progress; retry shortly")
	}
	defer releaseModelConfigApplyLock(context.Background(), lockKey, lockToken)

	// 与 handleClearIndex 同一原则：不能让持 advisory 锁的 DB 事务横跨 LCE
	// 网络调用（120s 超时会占死连接池），也不能"先清 LCE 再提交 relay"——
	// Commit 失败会留下 LCE 空/relay 快照满的永久不一致。顺序：
	//   1) 事务A 清 relay 快照并提交（可自愈：只会引发一次全量重传）；
	//   2) 调 LCE 清租户索引；
	//   3) 事务B 重新加锁推进 applied_fingerprint（带指纹复查防并发改配置）。
	// 若 2/3 失败，applied_fingerprint 保持不一致，下个请求会重试整个流程。
	tx, err := beginLockedIndexUserTx(ctx, userID)
	if err != nil {
		return fmt.Errorf("model config index lock failed: %w", err)
	}
	if err := clearUserIndexStateTx(ctx, tx, userID); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("model config relay snapshot reset failed: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("model config snapshot reset commit failed: %w", err)
	}

	result, err := lce.callTool(ctx, "codebase_clear_index", map[string]interface{}{"tenant_id": userID})
	if err != nil || (result != nil && result.IsError) {
		detail := ""
		if err != nil {
			detail = err.Error()
		} else if result != nil {
			detail = string(result.Content)
		}
		log.Printf("[MODELCFG] tenant index clear failed (user=%s): %s", userID, detail)
		return fmt.Errorf("model config index reset failed: %s", detail)
	}

	tx2, err := beginLockedIndexUserTx(ctx, userID)
	if err != nil {
		return fmt.Errorf("model config index lock failed: %w", err)
	}
	defer tx2.Rollback()
	updateResult, err := tx2.ExecContext(ctx,
		`UPDATE user_model_configs
		 SET applied_fingerprint = $2, updated_at = NOW()
		 WHERE user_id = $1 AND fingerprint = $2`,
		userID, row.Fingerprint,
	)
	if err != nil {
		return fmt.Errorf("model config fingerprint update failed: %w", err)
	}
	rows, err := updateResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("model config fingerprint result failed: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("model config changed while applying; retry")
	}
	if err := tx2.Commit(); err != nil {
		return fmt.Errorf("model config apply commit failed: %w", err)
	}

	row.Applied = row.Fingerprint
	if err := redisClient.Del(ctx, "modelcfg:"+userID).Err(); err != nil {
		log.Printf("[MODELCFG] cache invalidation failed (user=%s): %v", userID, err)
	}
	log.Printf("[MODELCFG] model config applied for user %s: LCE and relay indexes cleared (fingerprint=%s)", userID, row.Fingerprint)
	return nil
}

// resolveModelConfigArg 返回要注入 LCE 工具调用的 model_config 参数
// （nil = 使用平台默认），并在配置变化时惰性触发租户索引重建。
func resolveModelConfigArg(ctx context.Context, userID string) (map[string]interface{}, error) {
	if modelConfigKey == nil {
		return nil, nil
	}
	row := getUserModelConfigRow(ctx, userID)
	if row == nil {
		return nil, nil
	}
	if row.Applied != row.Fingerprint {
		if err := applyModelConfigChange(ctx, userID, row); err != nil {
			return nil, err
		}
	}
	if row.Enc == "" {
		return nil, nil // 已恢复平台默认
	}
	cfg, err := decryptModelConfig(row.Enc)
	if err != nil {
		log.Printf("[MODELCFG] decrypt failed (user=%s): %v", userID, err)
		return nil, fmt.Errorf("model config decrypt failed")
	}
	return cfg, nil
}
