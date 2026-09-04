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
)

// ── 按用户 Rerank 配置────────────────────────────────────────────────────
//
// 前端把用户的 rerank 配置（含用户自己的 API key）加密后写入
// user_model_configs（AES-256-GCM，密钥 = SHA-256(MODEL_CONFIG_SECRET)，
// 密文布局 base64(nonce[12] || ciphertext||tag)，与前端 lib/model-config-crypto
// 保持一致）。relay 只转发 rerank；云端 embedding 和向量空间由 LCE 服务端固定。
//
// MODEL_CONFIG_SECRET 未设置时整个特性关闭（不查表、不注入）。

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
			config_enc TEXT NOT NULL,
			updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to migrate user_model_configs table: %w", err)
	}
	return nil
}

type userModelConfigRow struct {
	Enc string `json:"enc"`
}

// getUserModelConfigRow reads the primary row on every request. Personal model
// credentials are security-sensitive configuration: a best-effort Redis DEL
// cannot make save/reset strongly consistent because a failed invalidation can
// resurrect a stale API key until TTL expiry. The primary-key lookup is cheap
// and guarantees that clearing the row immediately falls back to platform
// configuration on every Relay instance.
func getUserModelConfigRow(ctx context.Context, userID string) (*userModelConfigRow, error) {
	var enc sql.NullString
	err := db.QueryRowContext(
		ctx,
		`SELECT config_enc FROM user_model_configs WHERE user_id = $1`,
		userID,
	).Scan(&enc)
	switch {
	case err == sql.ErrNoRows:
		return nil, nil
	case err != nil:
		log.Printf("[MODELCFG] lookup failed (user=%s): %v", userID, err)
		return nil, fmt.Errorf("model config lookup failed: %w", err)
	}

	return &userModelConfigRow{Enc: enc.String}, nil
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

// loadRerankModelConfigArg returns the request-local rerank override.
func loadRerankModelConfigArg(ctx context.Context, userID string) (map[string]interface{}, error) {
	if modelConfigKey == nil {
		return nil, nil
	}
	row, err := getUserModelConfigRow(ctx, userID)
	if err != nil {
		return nil, err
	}
	if row == nil || row.Enc == "" {
		return nil, nil
	}
	cfg, err := decryptModelConfig(row.Enc)
	if err != nil {
		log.Printf("[MODELCFG] decrypt failed (user=%s): %v", userID, err)
		return nil, fmt.Errorf("model config decrypt failed")
	}
	rerank, ok := cfg["rerank"]
	if !ok || rerank == nil {
		return nil, nil
	}
	return map[string]interface{}{"rerank": rerank}, nil
}
