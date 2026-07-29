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
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

// ── 设备绑定（防账号共用）──────────────────────────────────────────────────
//
// 前端 /api/auth/device 登录时把 (user_id, device_id) 写入 devices 表（含
// 每用户设备数上限与 LRU 淘汰）；插件端每个请求带 X-Client-Id 头。
//
// DEVICE_BINDING_MODE 三档：
//   off     不校验（仅保留原有 token 鉴权）
//   log     只记录告警，不拦截 —— 灰度期默认，老客户端不受影响
//   enforce 未携带/未注册的设备一律 401
//
// 同一 (user, device) 在滑动窗口内出现过多来源 IP 时写 device_alerts 告警，
// 覆盖 token 和 device_id 一起被复制的场景。Redis 键与前端约定：
// 前端注册/淘汰设备时会删除 device:reg:{user}:{device} 使 relay 立即生效。

const (
	DeviceModeOff     = "off"
	DeviceModeLog     = "log"
	DeviceModeEnforce = "enforce"

	deviceNegCacheTTL   = time.Minute
	deviceTouchInterval = time.Minute
	deviceAlertCooldown = 30 * time.Minute

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

func migrateDeviceTables() error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS devices (
			user_id VARCHAR(255) NOT NULL,
			device_id VARCHAR(128) NOT NULL,
			device_name VARCHAR(255),
			created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			last_seen_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			last_ip VARCHAR(45),
			PRIMARY KEY (user_id, device_id)
		);

		CREATE TABLE IF NOT EXISTS device_alerts (
			id SERIAL PRIMARY KEY,
			user_id VARCHAR(255) NOT NULL,
			device_id VARCHAR(128),
			kind VARCHAR(32) NOT NULL,
			detail TEXT,
			created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		);
		CREATE INDEX IF NOT EXISTS idx_device_alerts_user_created ON device_alerts(user_id, created_at DESC);

		CREATE TABLE IF NOT EXISTS banned_users (
			user_id VARCHAR(255) PRIMARY KEY,
			reason TEXT,
			created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		);
	`)
	if err != nil {
		return fmt.Errorf("failed to migrate device tables: %w", err)
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
		log.Printf("[DEVICE] ban lookup failed (user=%s): %v", userID, err)
		return false
	}

	if banned {
		redisClient.Set(ctx, cacheKey, "1", deviceCacheTTL)
	} else {
		redisClient.Set(ctx, cacheKey, "0", deviceCacheTTL)
	}
	return banned
}

// checkDeviceBinding 校验请求的设备标识，返回 (deviceID, 是否放行)。
func checkDeviceBinding(c *gin.Context, userID string) (string, bool) {
	if deviceBindingMode == DeviceModeOff {
		return "", true
	}

	deviceID := strings.TrimSpace(c.GetHeader("X-Client-Id"))
	if deviceID == "" {
		deviceAlertAsync(userID, "", "missing_client_id",
			fmt.Sprintf("path=%s ip=%s", c.Request.URL.Path, c.ClientIP()))
		return "", deviceBindingMode != DeviceModeEnforce
	}
	if len(deviceID) > 128 {
		// 超长 ID 是畸形/伪造客户端的信号，和"没带 ID"的老客户端要分开统计。
		deviceAlertAsync(userID, deviceID[:128], "invalid_client_id",
			fmt.Sprintf("len=%d path=%s ip=%s", len(deviceID), c.Request.URL.Path, c.ClientIP()))
		return "", deviceBindingMode != DeviceModeEnforce
	}

	if isDeviceRegistered(userID, deviceID) {
		return deviceID, true
	}
	deviceAlertAsync(userID, deviceID, "unregistered_device",
		fmt.Sprintf("path=%s ip=%s", c.Request.URL.Path, c.ClientIP()))
	return deviceID, deviceBindingMode != DeviceModeEnforce
}

func isDeviceRegistered(userID, deviceID string) bool {
	ctx := context.Background()
	cacheKey := "device:reg:" + userID + ":" + deviceID
	if v, err := redisClient.Get(ctx, cacheKey).Result(); err == nil {
		return v == "1"
	}

	var exists bool
	err := db.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM devices WHERE user_id = $1 AND device_id = $2)`,
		userID, deviceID,
	).Scan(&exists)
	if err != nil {
		// DB 故障时放行并只打日志，避免基础设施抖动演变成全站 401。
		log.Printf("[DEVICE] registration lookup failed (user=%s): %v", userID, err)
		return true
	}

	if exists {
		redisClient.Set(ctx, cacheKey, "1", deviceCacheTTL)
	} else {
		redisClient.Set(ctx, cacheKey, "0", deviceNegCacheTTL)
	}
	return exists
}

// recordDeviceActivity 节流更新 last_seen/last_ip，并做同设备多 IP 检测。
func recordDeviceActivity(userID, deviceID, clientIP string) {
	ctx := context.Background()

	touchKey := "device:touch:" + userID + ":" + deviceID
	if ok, err := redisClient.SetNX(ctx, touchKey, "1", deviceTouchInterval).Result(); err == nil && ok {
		if _, err := db.Exec(
			`UPDATE devices SET last_seen_at = NOW(), last_ip = $3 WHERE user_id = $1 AND device_id = $2`,
			userID, deviceID, clientIP,
		); err != nil {
			log.Printf("[DEVICE] touch failed (user=%s device=%s): %v", userID, deviceID, err)
		}
	}

	if clientIP == "" || deviceMaxIPs <= 0 {
		return
	}
	// 滑动窗口用 ZSET（score = 出现时间）：每次先清掉窗口外的成员再计数。
	// 早先的 SET + EXPIRE 方案没有成员级过期，活跃设备的整个 set 的 TTL 会被
	// 每次访问刷新，历史 IP 无限累积，必然误报 multi_ip。
	ipKey := "device:ips:" + userID + ":" + deviceID
	now := time.Now()
	windowStart := now.Add(-deviceIPWindow)
	pipe := redisClient.TxPipeline()
	pipe.ZAdd(ctx, ipKey, redis.Z{Score: float64(now.UnixMilli()), Member: clientIP})
	pipe.ZRemRangeByScore(ctx, ipKey, "-inf", fmt.Sprintf("%d", windowStart.UnixMilli()))
	countCmd := pipe.ZCard(ctx, ipKey)
	pipe.Expire(ctx, ipKey, deviceIPWindow+time.Minute)
	if _, err := pipe.Exec(ctx); err != nil {
		return
	}
	count := countCmd.Val()
	if count <= int64(deviceMaxIPs) {
		return
	}
	ips, _ := redisClient.ZRange(ctx, ipKey, 0, -1).Result()
	deviceAlertAsync(userID, deviceID, "multi_ip",
		fmt.Sprintf("%d ips within %s: %s", count, deviceIPWindow, strings.Join(ips, ",")))
}

// deviceAlertAsync 写入 device_alerts 并打日志；同类告警带冷却窗口防刷屏。
// 整个函数体异步执行：冷却检查那次 Redis 往返也不该阻塞请求路径。
func deviceAlertAsync(userID, deviceID, kind, detail string) {
	go func() {
		ctx := context.Background()
		coolKey := "device:alert:" + userID + ":" + deviceID + ":" + kind
		if ok, err := redisClient.SetNX(ctx, coolKey, "1", deviceAlertCooldown).Result(); err != nil || !ok {
			return
		}
		log.Printf("[DEVICE_ALERT] kind=%s user=%s device=%s %s", kind, userID, deviceID, detail)
		if _, err := db.Exec(
			`INSERT INTO device_alerts (user_id, device_id, kind, detail) VALUES ($1, $2, $3, $4)`,
			userID, deviceID, kind, detail,
		); err != nil {
			log.Printf("[DEVICE] alert insert failed: %v", err)
		}
	}()
}
