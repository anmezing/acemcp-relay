package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
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

// quotaCacheTTL 是配额上限缓存的独立 TTL，避免封禁缓存配置影响配额生效延迟。
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
	// org_quotas 由平台管理员写入（前端仓库也会幂等建同一张表）。
	// 无记录的组织回退到调用者 tier 的默认限额；<=0 = 不限。
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS org_quotas (
			org_id TEXT PRIMARY KEY,
			daily_request_limit BIGINT NOT NULL DEFAULT 0,
			daily_index_bytes_limit BIGINT NOT NULL DEFAULT 0,
			updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create org_quotas table: %w", err)
	}
	_, err = db.Exec(`
		ALTER TABLE org_quotas
			ALTER COLUMN daily_request_limit DROP NOT NULL,
			ALTER COLUMN daily_request_limit DROP DEFAULT,
			ALTER COLUMN daily_index_bytes_limit DROP NOT NULL,
			ALTER COLUMN daily_index_bytes_limit DROP DEFAULT
	`)
	if err != nil {
		return fmt.Errorf("failed to relax org_quotas defaults: %w", err)
	}
	// 组织 owner 设置的成员上限必须按 (org, user) 隔离；user_quotas 仍只用于
	// 个人租户，避免同一用户加入多个组织时互相覆盖。
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS org_member_quotas (
			org_id TEXT NOT NULL,
			user_id VARCHAR(255) NOT NULL,
			daily_limit INTEGER NOT NULL,
			updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			PRIMARY KEY (org_id, user_id)
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to migrate org_member_quotas table: %w", err)
	}
	return nil
}

// tierDailyRequestLimit 返回该 tier 的默认每日请求上限（<=0 = 不限）。
// user_quotas 的按用户覆盖值仍优先于 tier 默认值。
func tierDailyRequestLimit(tier string) int {
	if normalizeTier(tier) == tierPro {
		return proDailyRequestLimit
	}
	return defaultDailyRequestLimit
}

// tierIndexBytesLimit 返回该 tier 的每日索引字节上限（<=0 = 不限）。
func tierIndexBytesLimit(tier string) int64 {
	if normalizeTier(tier) == tierPro {
		return proDailyIndexBytesLimit
	}
	return dailyIndexBytesLimit
}

// getUserDailyLimit 返回该用户生效的每日上限（<=0 表示不限）。
// tier 只影响无按用户覆盖时的默认值；缓存存的是解析后的最终值，
// tier 变更的生效延迟受 quotaCacheTTL 约束（≤5 分钟）。
func getUserDailyLimit(userID, tier string) int {
	ctx := context.Background()
	cacheKey := "quota:limit:" + userID
	if v, err := redisClient.Get(ctx, cacheKey).Result(); err == nil {
		if n, convErr := strconv.Atoi(v); convErr == nil {
			return n
		}
	}

	limit := tierDailyRequestLimit(tier)
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

// ── 组织配额 ──────────────────────────────────────────────────────────────
//
// 租户归并后配额分两层：
//   - 个人租户（org_id 空）：完全沿用旧逻辑（user_quotas 覆盖 → tier 默认，
//     Redis key 也不变），存量行为零回归。
//   - 组织租户：先查成员个人上限（org_member_quotas 有该组织成员的行才检查；计数按
//     (org, user) 维度，同一用户的个人密钥用量不计入），再查组织池
//     （org_quotas 有行用其值，否则回退调用者 tier 的默认；计数按 org 共享）。

const quotaCacheNoRow = "none"

func memberQuotaLimitCacheKey(orgID, userID string) string {
	digest := sha256.Sum256([]byte(orgID + "\x00" + userID))
	return fmt.Sprintf("quota:limit:member:%x", digest)
}

// getMemberDailyLimit 查询 org_member_quotas 是否给该组织成员设了个人上限。
// 缓存键与个人路径的 quota:limit:{user} 分开：两者语义不同（这里要区分
// "无记录"），且键含 (org, user)，不会跨组织串扰。
func getMemberDailyLimit(orgID, userID string) (int, bool) {
	ctx := context.Background()
	cacheKey := memberQuotaLimitCacheKey(orgID, userID)
	if v, err := redisClient.Get(ctx, cacheKey).Result(); err == nil {
		if v == quotaCacheNoRow {
			return 0, false
		}
		if n, convErr := strconv.Atoi(v); convErr == nil {
			return n, true
		}
	}
	var dbLimit int
	err := db.QueryRow(
		`SELECT daily_limit FROM org_member_quotas WHERE org_id = $1 AND user_id = $2`,
		orgID,
		userID,
	).Scan(&dbLimit)
	switch {
	case err == sql.ErrNoRows:
		redisClient.Set(ctx, cacheKey, quotaCacheNoRow, quotaCacheTTL)
		return 0, false
	case err != nil:
		// DB 故障：跳过成员上限（与请求配额一致的 fail-open），不写缓存
		log.Printf("[QUOTA] member limit lookup failed (org=%s user=%s): %v", orgID, userID, err)
		return 0, false
	}
	redisClient.Set(ctx, cacheKey, strconv.Itoa(dbLimit), quotaCacheTTL)
	return dbLimit, true
}

type orgQuotaLimits struct {
	Request       int64
	RequestSet    bool
	IndexBytes    int64
	IndexBytesSet bool
}

// org_quotas 的两个维度可独立配置：NULL 表示该维度继承调用者 tier 默认值，
// 0 表示不限，正数表示显式上限。
func getOrgQuotaLimits(orgID string) orgQuotaLimits {
	ctx := context.Background()
	cacheKey := "quota:limit:orgq:" + orgID
	if v, err := redisClient.Get(ctx, cacheKey).Result(); err == nil {
		if v == quotaCacheNoRow {
			return orgQuotaLimits{}
		}
		if limits, ok := parseOrgQuotaCache(v); ok {
			return limits
		}
	}
	var reqLimit, bytesLimit sql.NullInt64
	err := db.QueryRow(
		`SELECT daily_request_limit, daily_index_bytes_limit FROM org_quotas WHERE org_id = $1`,
		orgID,
	).Scan(&reqLimit, &bytesLimit)
	switch {
	case err == sql.ErrNoRows:
		redisClient.Set(ctx, cacheKey, quotaCacheNoRow, quotaCacheTTL)
		return orgQuotaLimits{}
	case err != nil:
		// DB 故障：回退 tier 默认且不写缓存，恢复后自动回到正确值
		log.Printf("[QUOTA] org quota lookup failed (org=%s): %v", orgID, err)
		return orgQuotaLimits{}
	}
	limits := orgQuotaLimits{
		Request:       reqLimit.Int64,
		RequestSet:    reqLimit.Valid,
		IndexBytes:    bytesLimit.Int64,
		IndexBytesSet: bytesLimit.Valid,
	}
	redisClient.Set(ctx, cacheKey, formatOrgQuotaCache(limits), quotaCacheTTL)
	return limits
}

func formatOrgQuotaCache(limits orgQuotaLimits) string {
	encode := func(value int64, configured bool) string {
		if !configured {
			return "n"
		}
		return strconv.FormatInt(value, 10)
	}
	return "v1," + encode(limits.Request, limits.RequestSet) + "," +
		encode(limits.IndexBytes, limits.IndexBytesSet)
}

func parseOrgQuotaCache(value string) (orgQuotaLimits, bool) {
	parts := strings.Split(value, ",")
	// 滚动部署兼容：旧缓存的 "request,bytes" 表示两个维度都已配置。
	if len(parts) == 2 {
		req, err1 := strconv.ParseInt(parts[0], 10, 64)
		bytes, err2 := strconv.ParseInt(parts[1], 10, 64)
		if err1 != nil || err2 != nil {
			return orgQuotaLimits{}, false
		}
		return orgQuotaLimits{Request: req, RequestSet: true, IndexBytes: bytes, IndexBytesSet: true}, true
	}
	if len(parts) != 3 || parts[0] != "v1" {
		return orgQuotaLimits{}, false
	}
	decode := func(raw string) (int64, bool, bool) {
		if raw == "n" {
			return 0, false, true
		}
		value, err := strconv.ParseInt(raw, 10, 64)
		return value, true, err == nil
	}
	req, reqSet, reqOK := decode(parts[1])
	bytes, bytesSet, bytesOK := decode(parts[2])
	if !reqOK || !bytesOK {
		return orgQuotaLimits{}, false
	}
	return orgQuotaLimits{Request: req, RequestSet: reqSet, IndexBytes: bytes, IndexBytesSet: bytesSet}, true
}

// ── 每日索引字节配额 ──────────────────────────────────────────────────────
//
// codebase_index 的每个 MCP 调用都会计入常规请求数配额，但请求数无法反映
// embedding 成本：同样一次 upload 可以携带几字节，也可以携带一整批源码。
//
// 因此索引还要单独按"当日累计上传字节"计费，计的是实际送进 LCE 的内容长度，
// 正常增量扫描只传变更文件、消耗很小，而批量灌数据会立刻撞上限。
// DAILY_INDEX_BYTES_LIMIT=0 表示不限（仅建议在自用部署上这么配）。

// indexBytesKey 按租户计：个人租户 tenantID = userID（key 与旧版完全一致），
// 组织租户天然共享同一个池。
func indexBytesKey(tenantID string) string {
	day := time.Now().In(quotaLocation()).Format("20060102")
	return "quota:indexbytes:" + tenantID + ":" + day
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
// 放行的字节同时累加进 relay_index_bytes_total（全局 counter，含 fail-open 路径：
// 指标口径是"实际接收的索引字节"，不是"Redis 记账成功的字节"）。
// 组织租户（orgID 非空）优先用 org_quotas 的字节上限，无记录回退调用者 tier 默认；
// user_quotas 没有按用户的字节列，因此成员个人上限只约束请求数、不约束字节。
func chargeIndexBytes(tenantID, orgID, tier string, bytes int64) (bool, int64, int64) {
	allowed, used, limit := chargeIndexBytesQuota(tenantID, orgID, tier, bytes)
	if allowed && bytes > 0 {
		metricIndexBytes.Add(float64(bytes))
	}
	if !allowed {
		logEvent("index_quota_rejected",
			"user_id", tenantID,
			"tenant", tenantID,
			"org", orgID,
			"tier", normalizeTier(tier),
			"bytes", strconv.FormatInt(bytes, 10),
			"used", strconv.FormatInt(used, 10),
			"limit", strconv.FormatInt(limit, 10),
		)
	}
	return allowed, used, limit
}

func chargeIndexBytesQuota(tenantID, orgID, tier string, bytes int64) (bool, int64, int64) {
	limit := tierIndexBytesLimit(tier)
	if orgID != "" {
		if limits := getOrgQuotaLimits(orgID); limits.IndexBytesSet {
			limit = limits.IndexBytes
		}
	}
	if limit <= 0 || bytes <= 0 {
		return true, 0, limit
	}

	ctx := context.Background()
	key := indexBytesKey(tenantID)
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
	value, err := redisClient.Eval(ctx, chargeScript, []string{key}, bytes, limit, int64((48*time.Hour)/time.Second)).Result()
	if err != nil {
		log.Printf("[QUOTA] index byte accounting failed (tenant=%s): %v", tenantID, err)
		return true, 0, limit
	}
	parts, ok := value.([]interface{})
	if !ok || len(parts) != 2 {
		log.Printf("[QUOTA] invalid index byte accounting response (tenant=%s): %#v", tenantID, value)
		return true, 0, limit
	}
	allowed, allowedOK := parts[0].(int64)
	used, usedOK := parts[1].(int64)
	if !allowedOK || !usedOK {
		log.Printf("[QUOTA] invalid index byte accounting values (tenant=%s): %#v", tenantID, parts)
		return true, 0, limit
	}
	return allowed == 1, used, limit
}

// checkRequestQuota 累加当日计数并判断是否超限。Redis 故障时放行。
//
// 个人租户（orgID 空）：完全沿用旧路径（getUserDailyLimit + quota:used:{user}）。
// 组织租户：先检查成员个人上限（org_member_quotas 有行才检查；计数键按 (org, user)，
// 与该用户个人密钥的 quota:used:{user} 互不影响），再检查组织共享池
// （org_quotas 有行用其请求上限，否则回退调用者 tier 默认；计数键按 org）。
func checkRequestQuota(userID, orgID, tier string) (bool, int) {
	ctx := context.Background()
	day := time.Now().In(quotaLocation()).Format("20060102")

	if orgID == "" {
		limit := getUserDailyLimit(userID, tier)
		if limit <= 0 {
			return true, limit
		}
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

	// 成员个人上限：只约束该用户在这个组织内的用量。
	if memberLimit, exists := getMemberDailyLimit(orgID, userID); exists && memberLimit > 0 {
		key := "quota:used:org:" + orgID + ":" + userID + ":" + day
		used, err := redisClient.Incr(ctx, key).Result()
		if err == nil {
			if used == 1 {
				redisClient.Expire(ctx, key, 48*time.Hour)
			}
			if used > int64(memberLimit) {
				return false, memberLimit
			}
		}
	}

	orgLimit := int64(tierDailyRequestLimit(tier))
	if limits := getOrgQuotaLimits(orgID); limits.RequestSet {
		orgLimit = limits.Request
	}
	if orgLimit <= 0 {
		return true, int(orgLimit)
	}
	key := "quota:used:" + orgID + ":" + day
	used, err := redisClient.Incr(ctx, key).Result()
	if err != nil {
		return true, int(orgLimit)
	}
	if used == 1 {
		redisClient.Expire(ctx, key, 48*time.Hour)
	}
	return used <= orgLimit, int(orgLimit)
}
