package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// ── 访问控制（受信控制台 + 账号封禁）──────────────────────────────────────
//
// Relay 只经配额做控制面；本文件负责两块访问控制：
//   - 受信控制台请求：前端管理接口用共享密钥派生的 token 访问 relay 管理端点；
//   - 账号封禁：前端写 banned_users 表并删除 banned:{user} 缓存使其立即生效。

const (
	consoleTokenHeader  = "X-LCE-Console-Token"
	consoleTokenContext = "acemcp-relay-console:"
)

var trustedConsoleToken string

func configureTrustedConsole(secret string) {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		trustedConsoleToken = ""
		return
	}

	hash := sha256.Sum256([]byte(consoleTokenContext + secret))
	trustedConsoleToken = hex.EncodeToString(hash[:])
}

func isTrustedConsoleRequest(c *gin.Context) bool {
	if trustedConsoleToken == "" {
		return false
	}

	path := c.Request.URL.Path
	method := c.Request.Method
	allowed := (method == http.MethodGet && path == "/mcp/tenant-stats") ||
		(method == http.MethodPost && path == "/mcp/clear-index")
	if !allowed {
		return false
	}

	token := strings.TrimSpace(c.GetHeader(consoleTokenHeader))
	if len(token) != len(trustedConsoleToken) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(trustedConsoleToken)) == 1
}

func migrateAccessControlTables() error {
	if _, err := db.Exec(`
		DROP TABLE IF EXISTS device_alerts;
		DROP TABLE IF EXISTS devices;
	`); err != nil {
		return fmt.Errorf("failed to remove retired device access-control tables: %w", err)
	}

	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS banned_users (
			user_id VARCHAR(255) PRIMARY KEY,
			reason TEXT,
			created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to migrate access control tables: %w", err)
	}
	return nil
}

// isUserBanned 检查账号是否被管理员封禁（前端写 banned_users 表并删除
// banned:{user} 缓存使其立即生效）。DB 故障时放行，避免误伤。
func isUserBanned(userID string) bool {
	ctx := context.Background()
	cacheKey := "banned:" + userID
	if v, err := redisClient.Get(ctx, cacheKey).Result(); err == nil {
		return v == "1"
	}

	var banned bool
	err := db.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM banned_users WHERE user_id = $1)`,
		userID,
	).Scan(&banned)
	if err != nil {
		log.Printf("[BAN] lookup failed (user=%s): %v", userID, err)
		return false
	}

	if banned {
		redisClient.Set(ctx, cacheKey, "1", bannedCacheTTL)
	} else {
		redisClient.Set(ctx, cacheKey, "0", bannedCacheTTL)
	}
	return banned
}
