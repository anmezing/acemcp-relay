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
// 每用户每日请求数与索引字节上限：user_quotas 表存按用户覆盖值（0 = 不限，
// NULL = 继承 tier 默认），无记录时使用 DEFAULT_*/PRO_*。计数用 Redis 按
// Asia/Shanghai 自然日累加（与 leaderboard 口径一致），超限返回 429。
// 前端修改配额后会删除对应 quota:limit:* 缓存，立即生效。

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
			daily_limit INTEGER,
			daily_index_bytes_limit BIGINT,
			updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to migrate user_quotas table: %w", err)
	}
	_, err = db.Exec(`
		ALTER TABLE user_quotas
			ALTER COLUMN daily_limit DROP NOT NULL,
			ADD COLUMN IF NOT EXISTS daily_index_bytes_limit BIGINT
	`)
	if err != nil {
		return fmt.Errorf("failed to migrate user quota index byte column: %w", err)
	}
	// 套餐购买由前端写入；Relay 只读取购买时冻结的权益快照。expires_at
	// 直接参与查询，套餐过期后自动回落，不依赖清理任务或 tier 回写。
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS user_subscriptions (
			user_id VARCHAR(255) PRIMARY KEY,
			plan_id TEXT NOT NULL,
			plan_name VARCHAR(120) NOT NULL,
			tier TEXT NOT NULL CHECK (tier IN ('free', 'pro')),
			daily_request_limit BIGINT NOT NULL CHECK (daily_request_limit >= 0),
			daily_index_bytes_limit BIGINT NOT NULL CHECK (daily_index_bytes_limit >= 0),
			subaccount_limit INTEGER NOT NULL CHECK (subaccount_limit >= 0),
			starts_at TIMESTAMP WITH TIME ZONE NOT NULL,
			expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
			source_order_id TEXT NOT NULL UNIQUE,
			updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to migrate user subscriptions table: %w", err)
	}
	// org_quotas 由平台管理员写入（前端仓库也会幂等建同一张表）。
	// NULL/无记录继承 canonical owner 的套餐或基础 tier；<=0 = 不限。
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

type planQuotaLimits struct {
	Request    int64
	IndexBytes int64
	ExpiresAt  time.Time
	Found      bool
	Failed     bool
}

// getUserPlanQuotaLimits 读取仍在有效期内的已购买权益。这里不缓存数据库错误；
// 正常结果由外层最终配额缓存承接，前端在付款成功时会主动删除对应缓存。
func getUserPlanQuotaLimits(userID string) planQuotaLimits {
	var limits planQuotaLimits
	err := db.QueryRow(`
		SELECT daily_request_limit, daily_index_bytes_limit, expires_at
		FROM user_subscriptions
		WHERE user_id = $1 AND starts_at <= NOW() AND expires_at > NOW()
	`, userID).Scan(&limits.Request, &limits.IndexBytes, &limits.ExpiresAt)
	switch {
	case err == nil:
		limits.Found = true
	case err != sql.ErrNoRows:
		limits.Failed = true
		log.Printf("[QUOTA] active plan lookup failed (user=%s): %v", userID, err)
	}
	return limits
}

// getOrgOwnerQuotaLimits 与前端子账号口径一致：按成员创建时间和 id 选择最早
// owner 作为组织权益拥有者，避免多个 owner 通过取最大值叠加套餐权益。
// 有有效套餐时读取购买时冻结的额度；否则读取 owner 的基础 tier。组织成员
// 自己的 tier 不参与组织共享池计算，避免受邀 Pro 成员意外抬高整个组织额度。
func getOrgOwnerQuotaLimits(orgID string) planQuotaLimits {
	var limits planQuotaLimits
	var requestLimit, indexBytesLimit sql.NullInt64
	var expiresAt sql.NullTime
	var ownerTier string
	err := db.QueryRow(`
		SELECT subscriptions.daily_request_limit,
		       subscriptions.daily_index_bytes_limit,
		       subscriptions.expires_at,
		       COALESCE(owner_key.tier, 'free') AS owner_tier
		FROM (
			SELECT owners."userId"
			FROM "member" AS owners
			WHERE owners."organizationId" = $1
			  AND (',' || regexp_replace(owners.role, '\s', '', 'g') || ',') LIKE '%,owner,%'
			ORDER BY owners."createdAt", owners.id
			LIMIT 1
		) AS canonical_owner
		LEFT JOIN LATERAL (
			SELECT active.daily_request_limit,
			       active.daily_index_bytes_limit,
			       active.expires_at
			FROM user_subscriptions AS active
			WHERE active.user_id = canonical_owner."userId"
			  AND active.starts_at <= NOW()
			  AND active.expires_at > NOW()
			LIMIT 1
		) AS subscriptions ON TRUE
		LEFT JOIN LATERAL (
			SELECT keys.tier
			FROM api_keys AS keys
			WHERE keys.user_id = canonical_owner."userId"
			ORDER BY (keys.org_id IS NULL) DESC, keys.created_at, keys.id
			LIMIT 1
		) AS owner_key ON TRUE
	`, orgID).Scan(&requestLimit, &indexBytesLimit, &expiresAt, &ownerTier)
	switch {
	case err == sql.ErrNoRows:
		return limits
	case err != nil:
		limits.Failed = true
		log.Printf("[QUOTA] organization plan lookup failed (org=%s): %v", orgID, err)
		return limits
	}
	if requestLimit.Valid && indexBytesLimit.Valid && expiresAt.Valid {
		limits.Request = requestLimit.Int64
		limits.IndexBytes = indexBytesLimit.Int64
		limits.ExpiresAt = expiresAt.Time
		limits.Found = true
		return limits
	}
	limits.Request = int64(tierDailyRequestLimit(ownerTier))
	limits.IndexBytes = tierIndexBytesLimit(ownerTier)
	limits.Found = true
	return limits
}

// planCacheTTL 保证最终配额缓存绝不跨过订阅到期时间。付款成功会主动清缓存，
// 因而购买/续费即时生效；到期则无需等待固定的 quotaCacheTTL。
func planCacheTTL(expiresAt time.Time) time.Duration {
	remaining := time.Until(expiresAt)
	if remaining <= 0 {
		return 0
	}
	if remaining < quotaCacheTTL {
		return remaining
	}
	return quotaCacheTTL
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
	cacheTTL := quotaCacheTTL
	var dbLimit sql.NullInt64
	err := db.QueryRow(`SELECT daily_limit FROM user_quotas WHERE user_id = $1`, userID).Scan(&dbLimit)
	switch {
	case err == nil && dbLimit.Valid:
		limit = int(dbLimit.Int64)
	case err == nil || err == sql.ErrNoRows:
		plan := getUserPlanQuotaLimits(userID)
		if plan.Failed {
			return limit
		}
		if plan.Found {
			limit = int(plan.Request)
			cacheTTL = planCacheTTL(plan.ExpiresAt)
		}
	case err != nil:
		// DB 故障时用默认值且不写缓存，恢复后自动回到正确值
		log.Printf("[QUOTA] limit lookup failed (user=%s): %v", userID, err)
		return limit
	}

	if cacheTTL > 0 {
		redisClient.Set(ctx, cacheKey, strconv.Itoa(limit), cacheTTL)
	}
	return limit
}

// getUserIndexBytesLimit 返回个人租户生效的每日索引字节上限（<=0 表示不限）。
// NULL/无记录继承 tier 默认值；显式 0 表示不限。
func getUserIndexBytesLimit(userID, tier string) int64 {
	ctx := context.Background()
	cacheKey := "quota:limit:indexbytes:" + userID
	if v, err := redisClient.Get(ctx, cacheKey).Result(); err == nil {
		if n, convErr := strconv.ParseInt(v, 10, 64); convErr == nil {
			return n
		}
	}

	limit := tierIndexBytesLimit(tier)
	cacheTTL := quotaCacheTTL
	// 启动早期或隔离测试尚未注入数据库时，保持既有 tier 默认行为。
	// 正常服务在注册路由前已完成数据库连接与迁移。
	if db == nil {
		return limit
	}
	var dbLimit sql.NullInt64
	err := db.QueryRow(
		`SELECT daily_index_bytes_limit FROM user_quotas WHERE user_id = $1`,
		userID,
	).Scan(&dbLimit)
	switch {
	case err == nil && dbLimit.Valid:
		limit = dbLimit.Int64
	case err == nil || err == sql.ErrNoRows:
		plan := getUserPlanQuotaLimits(userID)
		if plan.Failed {
			return limit
		}
		if plan.Found {
			limit = plan.IndexBytes
			cacheTTL = planCacheTTL(plan.ExpiresAt)
		}
	case err != nil:
		log.Printf("[QUOTA] index byte limit lookup failed (user=%s): %v", userID, err)
		return limit
	}

	if cacheTTL > 0 {
		redisClient.Set(ctx, cacheKey, strconv.FormatInt(limit, 10), cacheTTL)
	}
	return limit
}

// ── 组织配额 ──────────────────────────────────────────────────────────────
//
// 租户归并后配额分两层：
//   - 个人租户（org_id 空）：完全沿用旧逻辑（user_quotas 覆盖 → tier 默认，
//     Redis key 也不变），存量行为零回归。
//   - 组织租户：先查成员个人上限（org_member_quotas 有该组织成员的行才检查；计数按
//     (org, user) 维度，同一用户的个人密钥用量不计入），再查组织池
//     （管理员覆盖 → canonical owner 套餐 → owner 基础 tier → Free 默认；
//     计数按 org 共享）。

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

// org_quotas 的两个维度可独立配置：NULL 表示该维度继承 canonical owner
// 的有效套餐或基础 tier。owner 缺失时按 Free 默认，0 表示不限，正数表示显式上限。
// 返回值在数据库正常时始终是两个维度都已解析的最终生效额度。
func getOrgQuotaLimits(orgID string) orgQuotaLimits {
	ctx := context.Background()
	cacheKey := "quota:limit:orgq:" + orgID
	if v, err := redisClient.Get(ctx, cacheKey).Result(); err == nil {
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
	case err != nil:
		// DB 故障：回退 Free 默认且不写缓存，恢复后自动回到正确值。
		log.Printf("[QUOTA] org quota lookup failed (org=%s): %v", orgID, err)
		return orgQuotaLimits{}
	}
	limits := orgQuotaLimits{
		Request:       reqLimit.Int64,
		RequestSet:    reqLimit.Valid,
		IndexBytes:    bytesLimit.Int64,
		IndexBytesSet: bytesLimit.Valid,
	}
	cacheTTL := quotaCacheTTL
	if !limits.RequestSet || !limits.IndexBytesSet {
		owner := getOrgOwnerQuotaLimits(orgID)
		if owner.Failed {
			return limits
		}
		if !owner.Found {
			owner.Request = int64(tierDailyRequestLimit(tierFree))
			owner.IndexBytes = tierIndexBytesLimit(tierFree)
		}
		if !limits.RequestSet {
			limits.Request = owner.Request
			limits.RequestSet = true
		}
		if !limits.IndexBytesSet {
			limits.IndexBytes = owner.IndexBytes
			limits.IndexBytesSet = true
		}
		if !owner.ExpiresAt.IsZero() {
			cacheTTL = planCacheTTL(owner.ExpiresAt)
		}
	}
	if cacheTTL > 0 {
		redisClient.Set(ctx, cacheKey, formatOrgQuotaCache(limits), cacheTTL)
	}
	return limits
}

func formatOrgQuotaCache(limits orgQuotaLimits) string {
	return "v2," + strconv.FormatInt(limits.Request, 10) + "," +
		strconv.FormatInt(limits.IndexBytes, 10)
}

func parseOrgQuotaCache(value string) (orgQuotaLimits, bool) {
	parts := strings.Split(value, ",")
	if len(parts) != 3 || parts[0] != "v2" {
		return orgQuotaLimits{}, false
	}
	requestLimit, requestErr := strconv.ParseInt(parts[1], 10, 64)
	indexBytesLimit, indexBytesErr := strconv.ParseInt(parts[2], 10, 64)
	if requestErr != nil || indexBytesErr != nil {
		return orgQuotaLimits{}, false
	}
	return orgQuotaLimits{
		Request:       requestLimit,
		RequestSet:    true,
		IndexBytes:    indexBytesLimit,
		IndexBytesSet: true,
	}, true
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
// 个人租户优先用 user_quotas 的字节覆盖；组织租户使用已解析的 owner 最终权益。
// 组织成员个人上限仍只约束请求数。
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
	if orgID == "" {
		limit = getUserIndexBytesLimit(tenantID, tier)
	} else {
		limit = tierIndexBytesLimit(tierFree)
		limits := getOrgQuotaLimits(orgID)
		if limits.IndexBytesSet {
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
// （管理员覆盖 → canonical owner 套餐/基础 tier → Free 默认；计数键按 org）。
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

	orgLimit := int64(tierDailyRequestLimit(tierFree))
	limits := getOrgQuotaLimits(orgID)
	if limits.RequestSet {
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
