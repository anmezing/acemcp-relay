package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
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
)

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
	if deviceID == "" || len(deviceID) > 128 {
		deviceAlertAsync(userID, "", "missing_client_id",
			fmt.Sprintf("path=%s ip=%s", c.Request.URL.Path, c.ClientIP()))
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
	ipKey := "device:ips:" + userID + ":" + deviceID
	added, err := redisClient.SAdd(ctx, ipKey, clientIP).Result()
	if err != nil {
		return
	}
	redisClient.Expire(ctx, ipKey, deviceIPWindow)
	if added == 0 {
		return
	}
	count, err := redisClient.SCard(ctx, ipKey).Result()
	if err != nil || count <= int64(deviceMaxIPs) {
		return
	}
	ips, _ := redisClient.SMembers(ctx, ipKey).Result()
	deviceAlertAsync(userID, deviceID, "multi_ip",
		fmt.Sprintf("%d ips within %s: %s", count, deviceIPWindow, strings.Join(ips, ",")))
}

// deviceAlertAsync 写入 device_alerts 并打日志；同类告警带冷却窗口防刷屏。
func deviceAlertAsync(userID, deviceID, kind, detail string) {
	ctx := context.Background()
	coolKey := "device:alert:" + userID + ":" + deviceID + ":" + kind
	if ok, err := redisClient.SetNX(ctx, coolKey, "1", deviceAlertCooldown).Result(); err != nil || !ok {
		return
	}
	log.Printf("[DEVICE_ALERT] kind=%s user=%s device=%s %s", kind, userID, deviceID, detail)
	go func() {
		if _, err := db.Exec(
			`INSERT INTO device_alerts (user_id, device_id, kind, detail) VALUES ($1, $2, $3, $4)`,
			userID, deviceID, kind, detail,
		); err != nil {
			log.Printf("[DEVICE] alert insert failed: %v", err)
		}
	}()
}
