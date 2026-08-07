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
// applied_fingerprint；随后一次 codebase_index start 会看到空快照并返回全量待上传文件。
//
// MODEL_CONFIG_SECRET 未设置时整个特性关闭（不查表、不注入）。

const (
	modelConfigCacheTTL   = 5 * time.Minute
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
func getUserModelConfigRow(ctx context.Context, userID string) (*userModelConfigRow, error) {
	cacheKey := "modelcfg:" + userID
	if v, err := redisClient.Get(ctx, cacheKey).Result(); err == nil {
		if v == modelConfigNoRowValue {
			return nil, nil
		}
		var row userModelConfigRow
		if json.Unmarshal([]byte(v), &row) == nil {
			return &row, nil
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
		return nil, nil
	case err != nil:
		log.Printf("[MODELCFG] lookup failed (user=%s): %v", userID, err)
		return nil, fmt.Errorf("model config lookup failed: %w", err)
	}

	row := &userModelConfigRow{Enc: enc.String, Fingerprint: fingerprint, Applied: applied.String}
	if data, err := json.Marshal(row); err == nil {
		redisClient.Set(ctx, cacheKey, string(data), modelConfigCacheTTL)
	}
	return row, nil
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

// applyModelConfigChangeUnderLease clears both LCE data and service snapshots.
// The caller must hold the user's exclusive index-operation lease across this
// function and the operation that consumes the newly applied configuration.
func applyModelConfigChangeUnderLease(ctx context.Context, userID string, row *userModelConfigRow) error {
	// 与 handleClearIndex 同一原则：不能让持 advisory 锁的 DB 事务横跨 LCE
	// 网络调用会长期等待，不能占住连接池；也不能"先清 LCE 再提交 relay"——
	// Commit 失败会留下 LCE 空/服务端快照满的永久不一致。顺序：
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
		return fmt.Errorf("model config service snapshot reset failed: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("model config snapshot reset commit failed: %w", err)
	}

	result, err := lce.callToolWithTimeout(
		ctx, "codebase_clear_index", map[string]interface{}{"tenant_id": userID}, remoteIndexMCPCallTimeout,
	)
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

// loadModelConfigArg returns the current config snapshot and identity without
// changing index state. A non-nil row with Applied != Fingerprint is pending.
func loadModelConfigArg(ctx context.Context, userID string) (map[string]interface{}, *userModelConfigRow, error) {
	if modelConfigKey == nil {
		return nil, nil, nil
	}
	row, err := getUserModelConfigRow(ctx, userID)
	if err != nil {
		return nil, nil, err
	}
	if row == nil {
		return nil, nil, nil
	}
	var cfg map[string]interface{}
	if row.Enc != "" {
		cfg, err = decryptModelConfig(row.Enc)
		if err != nil {
			log.Printf("[MODELCFG] decrypt failed (user=%s): %v", userID, err)
			return nil, nil, fmt.Errorf("model config decrypt failed")
		}
	}
	return cfg, row, nil
}

// resolveModelConfigUnderExclusiveLease applies a pending configuration while
// the caller holds an exclusive lease, and returns the immutable job identity.
func resolveModelConfigUnderExclusiveLease(ctx context.Context, userID string) (map[string]interface{}, string, error) {
	cfg, row, err := loadModelConfigArg(ctx, userID)
	if err != nil {
		return nil, "", err
	}
	if row == nil {
		return nil, "", nil
	}
	if row.Applied != row.Fingerprint {
		if err := applyModelConfigChangeUnderLease(ctx, userID, row); err != nil {
			return nil, "", err
		}
	}
	return cfg, row.Fingerprint, nil
}

// acquireModelConfigOperation holds an index lease until the caller has
// finished the LCE request. If a model change is pending, the shared lease is
// upgraded by release/reacquire and the resulting exclusive lease is retained,
// so no operation can enter between reset and first use of the new model.
func acquireModelConfigOperation(ctx context.Context, userID, kind string) (*indexOperationLease, map[string]interface{}, error) {
	lease, err := acquireSharedIndexOperation(ctx, userID, "model-call:"+uuid.NewString(), kind)
	if err != nil {
		return nil, nil, err
	}
	cfg, row, err := loadModelConfigArg(lease.Context(), userID)
	if err != nil {
		lease.Release()
		return nil, nil, err
	}
	if row == nil || row.Applied == row.Fingerprint {
		return lease, cfg, nil
	}

	lease.Release()
	exclusive, err := acquireExclusiveIndexOperation(ctx, userID, "apply-model-config:"+kind)
	if err != nil {
		return nil, nil, err
	}
	cfg, _, err = resolveModelConfigUnderExclusiveLease(exclusive.Context(), userID)
	if err != nil {
		exclusive.Release()
		return nil, nil, err
	}
	return exclusive, cfg, nil
}
