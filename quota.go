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

// quotaCacheTTL 是配额上限缓存的独立 TTL。此前复用 deviceCacheTTL，调设备
// 缓存参数会意外改变配额生效延迟。
const quotaCacheTTL = 5 * time.Minute

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

	redisClient.Set(ctx, cacheKey, strconv.Itoa(limit), quotaCacheTTL)
	return limit
}

// ── 每日索引字节配额 ──────────────────────────────────────────────────────
//
// codebase_index 的每个 MCP 调用都会计入常规请求数配额，但请求数无法反映
// embedding 成本：同样一次 upload 可以携带几字节，也可以携带一整批源码。
//
// 因此索引还要单独按"当日累计上传字节"计费，计的是实际送进 LCE 的内容长度，
// 正常增量扫描只传变更文件、消耗很小，而批量灌数据会立刻撞上限。
// DAILY_INDEX_BYTES_LIMIT=0 表示不限（仅建议在自用部署上这么配）。

func indexBytesKey(userID string) string {
	day := time.Now().In(quotaLocation()).Format("20060102")
	return "quota:indexbytes:" + userID + ":" + day
}

func quotaRetryAfterHeader(now time.Time) string {
	localNow := now.In(quotaLocation())
	year, month, day := localNow.Date()
	nextWindow := time.Date(year, month, day+1, 0, 0, 0, 0, quotaLocation())
	seconds := int64(nextWindow.Sub(localNow) / time.Second)
	if nextWindow.Sub(localNow)%time.Second != 0 {
		seconds++
	}
	if seconds < 1 {
		seconds = 1
	}
	return strconv.FormatInt(seconds, 10)
}

// chargeIndexBytes 累加当日已索引字节并判断是否超限，返回 (是否放行, 已用, 上限)。
// 与请求数配额一致：Redis 故障时放行，避免基础设施抖动演变成全站不可索引。
func chargeIndexBytes(userID string, bytes int64) (bool, int64, int64) {
	if dailyIndexBytesLimit <= 0 || bytes <= 0 {
		return true, 0, dailyIndexBytesLimit
	}

	ctx := context.Background()
	key := indexBytesKey(userID)
	const chargeScript = `
		local current = tonumber(redis.call('GET', KEYS[1]) or '0')
		local requested = tonumber(ARGV[1])
		local limit = tonumber(ARGV[2])
		if current + requested > limit then
			return {0, current}
		end
		local used = redis.call('INCRBY', KEYS[1], requested)
		if current == 0 then
			redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
		end
		return {1, used}
	`
	value, err := redisClient.Eval(ctx, chargeScript, []string{key}, bytes, dailyIndexBytesLimit, int64((48*time.Hour)/time.Second)).Result()
	if err != nil {
		log.Printf("[QUOTA] index byte accounting failed (user=%s): %v", userID, err)
		return true, 0, dailyIndexBytesLimit
	}
	parts, ok := value.([]interface{})
	if !ok || len(parts) != 2 {
		log.Printf("[QUOTA] invalid index byte accounting response (user=%s): %#v", userID, value)
		return true, 0, dailyIndexBytesLimit
	}
	allowed, allowedOK := parts[0].(int64)
	used, usedOK := parts[1].(int64)
	if !allowedOK || !usedOK {
		log.Printf("[QUOTA] invalid index byte accounting values (user=%s): %#v", userID, parts)
		return true, 0, dailyIndexBytesLimit
	}
	return allowed == 1, used, dailyIndexBytesLimit
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
