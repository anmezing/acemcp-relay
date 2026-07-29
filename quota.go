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
// 请求数配额挡不住索引通道：创建 job 只算 1 次请求，之后的 /relay/remote-index
// 批次上传全部豁免（它们是同一次扫描的内部步骤，按请求数计费会把一次正常扫描
// 记成上千次）。但真正产生 embedding 成本的恰恰是这些批次，且 manifest 上限
// 10 万文件、每请求可达 100MB —— 换算下来 1 次请求配额能驱动上百 GB 的
// embedding 调用。
//
// 因此索引单独按"当日累计上传字节"计费，计的是实际送进 LCE 的内容长度，
// 正常增量扫描只传变更文件、消耗很小，而批量灌数据会立刻撞上限。
// DAILY_INDEX_BYTES_LIMIT=0 表示不限（仅建议在自用部署上这么配）。

func indexBytesKey(userID string) string {
	day := time.Now().In(quotaLocation()).Format("20060102")
	return "quota:indexbytes:" + userID + ":" + day
}

// chargeIndexBytes 累加当日已索引字节并判断是否超限，返回 (是否放行, 已用, 上限)。
// 与请求数配额一致：Redis 故障时放行，避免基础设施抖动演变成全站不可索引。
func chargeIndexBytes(userID string, bytes int64) (bool, int64, int64) {
	if dailyIndexBytesLimit <= 0 || bytes <= 0 {
		return true, 0, dailyIndexBytesLimit
	}

	ctx := context.Background()
	key := indexBytesKey(userID)
	used, err := redisClient.IncrBy(ctx, key, bytes).Result()
	if err != nil {
		log.Printf("[QUOTA] index byte accounting failed (user=%s): %v", userID, err)
		return true, 0, dailyIndexBytesLimit
	}
	// 首次写入才设过期：IncrBy 从 0 起算，used == bytes 即本日第一次。
	if used == bytes {
		redisClient.Expire(ctx, key, 48*time.Hour)
	}
	return used <= dailyIndexBytesLimit, used, dailyIndexBytesLimit
}

// refundIndexBytes 返还 chargeIndexBytes 预扣的字节。计费点在调用 LCE 之前，
// 后续任何失败（batch 校验失败、LCE 出错、事务提交失败）都意味着这些字节
// 没有真正产生 embedding 成本，必须退回，否则重试会双重计费直至误撞配额。
// best-effort：Redis 故障只打日志（与扣费侧的故障放行策略一致）。
func refundIndexBytes(userID string, bytes int64) {
	if dailyIndexBytesLimit <= 0 || bytes <= 0 {
		return
	}
	ctx := context.Background()
	if err := redisClient.DecrBy(ctx, indexBytesKey(userID), bytes).Err(); err != nil {
		log.Printf("[QUOTA] index byte refund failed (user=%s bytes=%d): %v", userID, bytes, err)
	}
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
