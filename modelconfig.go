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
	modelConfigApplyLock  = time.Minute
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

// applyModelConfigChange 配置指纹变化时清租户索引并推进 applied_fingerprint。
// Redis 锁防止并发请求重复清理；失败释放锁以便下个请求重试。
func applyModelConfigChange(ctx context.Context, userID string, row *userModelConfigRow) {
	lockKey := "modelcfg:apply:" + userID
	ok, err := redisClient.SetNX(ctx, lockKey, "1", modelConfigApplyLock).Result()
	if err != nil || !ok {
		return
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
		redisClient.Del(ctx, lockKey)
		return
	}

	if _, err := db.Exec(
		`UPDATE user_model_configs SET applied_fingerprint = $2, updated_at = NOW() WHERE user_id = $1`,
		userID, row.Fingerprint,
	); err != nil {
		log.Printf("[MODELCFG] applied_fingerprint update failed (user=%s): %v", userID, err)
		redisClient.Del(ctx, lockKey)
		return
	}

	row.Applied = row.Fingerprint
	redisClient.Del(ctx, "modelcfg:"+userID)
	log.Printf("[MODELCFG] model config applied for user %s: tenant index cleared (fingerprint=%s)", userID, row.Fingerprint)
}

// resolveModelConfigArg 返回要注入 LCE 工具调用的 model_config 参数
//（nil = 使用平台默认），并在配置变化时惰性触发租户索引重建。
func resolveModelConfigArg(ctx context.Context, userID string) map[string]interface{} {
	if modelConfigKey == nil {
		return nil
	}
	row := getUserModelConfigRow(ctx, userID)
	if row == nil {
		return nil
	}
	if row.Applied != row.Fingerprint {
		applyModelConfigChange(ctx, userID, row)
	}
	if row.Enc == "" {
		return nil // 已恢复平台默认
	}
	cfg, err := decryptModelConfig(row.Enc)
	if err != nil {
		// 解密失败按平台默认放行并告警：通常是 MODEL_CONFIG_SECRET 两侧不一致
		log.Printf("[MODELCFG] decrypt failed (user=%s): %v", userID, err)
		return nil
	}
	return cfg
}
