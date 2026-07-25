package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"
)

// ── 每日请求配额 ──────────────────────────────────────────────────────────
//
// 每用户每日请求上限：user_quotas 表存按用户覆盖值（0 = 不限），无记录时用
// DEFAULT_DAILY_REQUEST_LIMIT（默认 0 = 不限）。计数用 Redis 按 Asia/Shanghai
// 自然日累加（与 leaderboard 口径一致），超限返回 429。
// 前端管理页修改配额后会删除 quota:limit:{user} 缓存，立即生效。

var (
	quotaLocOnce sync.Once
	quotaLoc     *time.Location
)

func quotaLocation() *time.Location {
	quotaLocOnce.Do(func() {
		loc, err := time.LoadLocation(LeaderboardTimezone)
		if err != nil {
			log.Printf("[QUOTA] load timezone failed, falling back to UTC: %v", err)
			loc = time.UTC
		}
		quotaLoc = loc
	})
	return quotaLoc
}

func migrateQuotaTables() error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS user_quotas (
			user_id VARCHAR(255) PRIMARY KEY,
			daily_limit INTEGER NOT NULL,
			updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to migrate user_quotas table: %w", err)
	}
	return nil
}

// getUserDailyLimit 返回该用户生效的每日上限（<=0 表示不限）。
func getUserDailyLimit(userID string) int {
	ctx := context.Background()
	cacheKey := "quota:limit:" + userID
	if v, err := redisClient.Get(ctx, cacheKey).Result(); err == nil {
		if n, convErr := strconv.Atoi(v); convErr == nil {
			return n
		}
	}

	limit := defaultDailyRequestLimit
	var dbLimit int
	err := db.QueryRow(`SELECT daily_limit FROM user_quotas WHERE user_id = $1`, userID).Scan(&dbLimit)
	switch {
	case err == nil:
		limit = dbLimit
	case err != sql.ErrNoRows:
		// DB 故障时用默认值且不写缓存，恢复后自动回到正确值
		log.Printf("[QUOTA] limit lookup failed (user=%s): %v", userID, err)
		return limit
	}

	redisClient.Set(ctx, cacheKey, strconv.Itoa(limit), deviceCacheTTL)
	return limit
}

// checkRequestQuota 累加当日计数并判断是否超限。Redis 故障时放行。
func checkRequestQuota(userID string) (bool, int) {
	limit := getUserDailyLimit(userID)
	if limit <= 0 {
		return true, limit
	}

	ctx := context.Background()
	day := time.Now().In(quotaLocation()).Format("20060102")
	key := "quota:used:" + userID + ":" + day
	used, err := redisClient.Incr(ctx, key).Result()
	if err != nil {
		return true, limit
	}
	if used == 1 {
		redisClient.Expire(ctx, key, 48*time.Hour)
	}
	return used <= int64(limit), limit
}
