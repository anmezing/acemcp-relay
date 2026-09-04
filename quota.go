package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
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
	// 组织密钥在每次请求上校验当前成员关系和 owner 的有效席位。Better Auth
	// 表可能尚未由前端初始化，因此通过 to_regclass 条件安装热路径索引；已有生产
	// 库会立即补齐，全新库稍后仍由前端 initDB 创建同名索引。
	_, err = db.Exec(`
		DO $$
		BEGIN
			IF to_regclass('public.member') IS NOT NULL THEN
				CREATE INDEX IF NOT EXISTS member_user_org_idx
					ON "member" ("userId", "organizationId");
				CREATE INDEX IF NOT EXISTS member_org_created_idx
					ON "member" ("organizationId", "createdAt", "id");
			END IF;
		END;
		$$
	`)
	if err != nil {
		return fmt.Errorf("failed to migrate organization authorization indexes: %w", err)
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
	OwnerID    string
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
// owner 的管理员覆盖、有效套餐和基础 tier 依次生效；组织成员自己的 tier 不参与
// 共享池计算，避免受邀 Pro 成员意外抬高整个组织额度。
func getOrgOwnerQuotaLimits(orgID string) planQuotaLimits {
	var limits planQuotaLimits
	var requestOverride, indexBytesOverride sql.NullInt64
	var planRequestLimit, planIndexBytesLimit sql.NullInt64
	var expiresAt sql.NullTime
	var ownerTier string
	err := db.QueryRow(`
		SELECT canonical_owner."userId",
		       overrides.daily_limit,
		       overrides.daily_index_bytes_limit,
		       subscriptions.daily_request_limit,
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
		LEFT JOIN user_quotas AS overrides
		  ON overrides.user_id = canonical_owner."userId"
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
			  AND (keys.org_id IS NULL OR keys.org_id = $1)
			ORDER BY (keys.org_id IS NULL) DESC, keys.created_at, keys.id
			LIMIT 1
		) AS owner_key ON TRUE
	`, orgID).Scan(
		&limits.OwnerID,
		&requestOverride,
		&indexBytesOverride,
		&planRequestLimit,
		&planIndexBytesLimit,
		&expiresAt,
		&ownerTier,
	)
	switch {
	case err == sql.ErrNoRows:
		return limits
	case err != nil:
		limits.Failed = true
		log.Printf("[QUOTA] organization plan lookup failed (org=%s): %v", orgID, err)
		return limits
	}
	limits.Request = int64(tierDailyRequestLimit(ownerTier))
	limits.IndexBytes = tierIndexBytesLimit(ownerTier)
	if planRequestLimit.Valid && planIndexBytesLimit.Valid && expiresAt.Valid {
		limits.Request = planRequestLimit.Int64
		limits.IndexBytes = planIndexBytesLimit.Int64
		limits.ExpiresAt = expiresAt.Time
	}
	if requestOverride.Valid {
		limits.Request = requestOverride.Int64
	}
	if indexBytesOverride.Valid {
		limits.IndexBytes = indexBytesOverride.Int64
	}
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
func getMemberDailyLimit(orgID, userID string) (int, bool, error) {
	ctx := context.Background()
	cacheKey := memberQuotaLimitCacheKey(orgID, userID)
	if v, err := redisClient.Get(ctx, cacheKey).Result(); err == nil {
		if v == quotaCacheNoRow {
			return 0, false, nil
		}
		if n, convErr := strconv.Atoi(v); convErr == nil {
			return n, true, nil
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
		return 0, false, nil
	case err != nil:
		// 成员上限是组织授权的一部分。查询失败时不能把“未知”解释为
		// “未配置”，否则会绕过管理员设置的成员上限。由 HTTP 边界返回 503。
		log.Printf("[QUOTA] member limit lookup failed (org=%s user=%s): %v", orgID, userID, err)
		return 0, false, err
	}
	redisClient.Set(ctx, cacheKey, strconv.Itoa(dbLimit), quotaCacheTTL)
	return dbLimit, true, nil
}

type orgQuotaLimits struct {
	Request          int64
	RequestSet       bool
	RequestPoolID    string
	IndexBytes       int64
	IndexBytesSet    bool
	IndexBytesPoolID string
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
		// DB 故障时不能返回未设置的零值：调用方会把它解释为“不限”，形成
		// 组织请求/索引配额的 fail-open。回退到独立的 Free 组织池且不写缓存，
		// 数据库恢复后下一次请求会重新解析 canonical owner 权益。
		log.Printf("[QUOTA] org quota lookup failed (org=%s): %v", orgID, err)
		return freeOrgQuotaLimits(orgID)
	}
	limits := orgQuotaLimits{
		Request:          reqLimit.Int64,
		RequestSet:       reqLimit.Valid,
		RequestPoolID:    orgID,
		IndexBytes:       bytesLimit.Int64,
		IndexBytesSet:    bytesLimit.Valid,
		IndexBytesPoolID: orgID,
	}
	cacheTTL := quotaCacheTTL
	if !limits.RequestSet || !limits.IndexBytesSet {
		owner := getOrgOwnerQuotaLimits(orgID)
		if owner.Failed {
			// 保留已经成功读取的组织级维度，只对无法继承的维度使用 Free
			// 默认。这样既不覆盖显式管理员配置，也不会因 owner 查询故障而放开。
			defaults := freeOrgQuotaLimits(orgID)
			if !limits.RequestSet {
				limits.Request = defaults.Request
				limits.RequestSet = true
			}
			if !limits.IndexBytesSet {
				limits.IndexBytes = defaults.IndexBytes
				limits.IndexBytesSet = true
			}
			return limits
		}
		if !owner.Found {
			owner.Request = int64(tierDailyRequestLimit(tierFree))
			owner.IndexBytes = tierIndexBytesLimit(tierFree)
		}
		if !limits.RequestSet {
			limits.Request = owner.Request
			limits.RequestSet = true
			if owner.OwnerID != "" {
				limits.RequestPoolID = owner.OwnerID
			}
		}
		if !limits.IndexBytesSet {
			limits.IndexBytes = owner.IndexBytes
			limits.IndexBytesSet = true
			if owner.OwnerID != "" {
				limits.IndexBytesPoolID = owner.OwnerID
			}
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

func freeOrgQuotaLimits(orgID string) orgQuotaLimits {
	return orgQuotaLimits{
		Request:          int64(tierDailyRequestLimit(tierFree)),
		RequestSet:       true,
		RequestPoolID:    orgID,
		IndexBytes:       tierIndexBytesLimit(tierFree),
		IndexBytesSet:    true,
		IndexBytesPoolID: orgID,
	}
}

func formatOrgQuotaCache(limits orgQuotaLimits) string {
	encode := base64.RawURLEncoding.EncodeToString
	return "v3," + strconv.FormatInt(limits.Request, 10) + "," +
		strconv.FormatInt(limits.IndexBytes, 10) + "," +
		encode([]byte(limits.RequestPoolID)) + "," +
		encode([]byte(limits.IndexBytesPoolID))
}

func parseOrgQuotaCache(value string) (orgQuotaLimits, bool) {
	parts := strings.Split(value, ",")
	if len(parts) != 5 || parts[0] != "v3" {
		return orgQuotaLimits{}, false
	}
	requestLimit, requestErr := strconv.ParseInt(parts[1], 10, 64)
	indexBytesLimit, indexBytesErr := strconv.ParseInt(parts[2], 10, 64)
	requestPoolID, requestPoolErr := base64.RawURLEncoding.DecodeString(parts[3])
	indexBytesPoolID, indexBytesPoolErr := base64.RawURLEncoding.DecodeString(parts[4])
	if requestErr != nil || indexBytesErr != nil || requestPoolErr != nil || indexBytesPoolErr != nil ||
		len(requestPoolID) == 0 || len(indexBytesPoolID) == 0 {
		return orgQuotaLimits{}, false
	}
	return orgQuotaLimits{
		Request:          requestLimit,
		RequestSet:       true,
		RequestPoolID:    string(requestPoolID),
		IndexBytes:       indexBytesLimit,
		IndexBytesSet:    true,
		IndexBytesPoolID: string(indexBytesPoolID),
	}, true
}

// ── 每日索引字节配额 ──────────────────────────────────────────────────────
//
// codebase_index 的每个 MCP 调用都会计入常规请求数配额，但请求数无法反映
// embedding 成本：同样一次 upload 可以携带几字节，也可以携带一整批源码。
//
// 索引配额按“当日首次提交的 root/路径/内容”累计。计费预约仍发生在上游调用前，
// 以便成本保护 fail closed；但同一文件内容因网络、provider 或任务重建而重放时，
// Redis 会原子去重，不会再次挤占日配额。DAILY_INDEX_BYTES_LIMIT=0 表示不限。

// indexBytesKey 按租户计：个人租户 tenantID = userID（key 与旧版完全一致），
// 组织租户天然共享同一个池。
func indexBytesKey(tenantID string) string {
	return indexBytesKeyAt(tenantID, time.Now())
}

func indexBytesKeyAt(tenantID string, now time.Time) string {
	day := now.In(quotaLocation()).Format("20060102")
	return "quota:indexbytes:" + tenantID + ":" + day
}

// indexFileChargesKey 保存一个 root 当天已经计费的逻辑文件内容。job_id 和批次边界
// 故意不参与身份计算：失败后新建 job、重新分批仍应命中同一条计费记录。
func indexFileChargesKey(tenantID, rootID string) string {
	return indexFileChargesKeyAt(tenantID, rootID, time.Now())
}

func indexFileChargesKeyAt(tenantID, rootID string, now time.Time) string {
	day := now.In(quotaLocation()).Format("20060102")
	rootDigest := sha256.Sum256([]byte(rootID))
	return fmt.Sprintf("quota:indexfiles:%s:%s:%x", tenantID, day, rootDigest)
}

func quotaRetryAfterHeader(now time.Time) string {
	remaining := quotaResetAt(now).Sub(now)
	seconds := int64(remaining / time.Second)
	if remaining%time.Second != 0 {
		seconds++
	}
	if seconds < 1 {
		seconds = 1
	}
	return strconv.FormatInt(seconds, 10)
}

func quotaResetAt(now time.Time) time.Time {
	localNow := now.In(quotaLocation())
	year, month, day := localNow.Date()
	return time.Date(year, month, day+1, 0, 0, 0, 0, quotaLocation())
}

// indexQuotaDecision 区分正常拒绝与记账基础设施故障。调用方必须把后者作为
// 临时服务故障处理，不能把未记账的昂贵索引请求放给上游。
type indexQuotaDecision struct {
	Allowed     bool
	Unavailable bool
	Used        int64
	Limit       int64
	// Charged 是本次真正新增到日配额的字节数；幂等重放允许为 0。
	Charged int64
}

type indexQuotaFile struct {
	Path  string
	Hash  string
	Bytes int64
}

type indexQuotaPool struct {
	Limit  int64
	PoolID string
}

func resolveIndexBytesQuota(tenantID, orgID, tier string) indexQuotaPool {
	if orgID == "" {
		return indexQuotaPool{Limit: getUserIndexBytesLimit(tenantID, tier), PoolID: tenantID}
	}
	pool := indexQuotaPool{Limit: tierIndexBytesLimit(tierFree), PoolID: orgID}
	limits := getOrgQuotaLimits(orgID)
	if limits.IndexBytesSet {
		pool.Limit = limits.IndexBytes
	}
	if limits.IndexBytesPoolID != "" {
		pool.PoolID = limits.IndexBytesPoolID
	}
	return pool
}

// chargeIndexBytes 保留给非文件级调用和配额单元测试；上传路径使用下面的
// chargeIndexFiles，以便跨任务、跨批次对相同文件内容做幂等计费。
func chargeIndexBytes(tenantID, orgID, tier string, bytes int64) indexQuotaDecision {
	decision := chargeIndexBytesQuota(tenantID, orgID, tier, bytes)
	if decision.Allowed && decision.Charged > 0 {
		metricIndexBytes.Add(float64(decision.Charged))
	}
	if decision.Unavailable {
		logEvent("index_quota_unavailable",
			"user_id", tenantID,
			"tenant", tenantID,
			"org", orgID,
			"tier", normalizeTier(tier),
			"bytes", strconv.FormatInt(bytes, 10),
		)
	} else if !decision.Allowed {
		logEvent("index_quota_rejected",
			"user_id", tenantID,
			"tenant", tenantID,
			"org", orgID,
			"tier", normalizeTier(tier),
			"bytes", strconv.FormatInt(bytes, 10),
			"used", strconv.FormatInt(decision.Used, 10),
			"limit", strconv.FormatInt(decision.Limit, 10),
		)
	}
	return decision
}

func chargeIndexBytesQuota(tenantID, orgID, tier string, bytes int64) indexQuotaDecision {
	quota := resolveIndexBytesQuota(tenantID, orgID, tier)
	limit := quota.Limit
	if limit <= 0 || bytes <= 0 {
		return indexQuotaDecision{Allowed: true, Limit: limit, Charged: max(bytes, 0)}
	}

	ctx := context.Background()
	key := indexBytesKey(quota.PoolID)
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
		return indexQuotaDecision{Unavailable: true, Limit: limit}
	}
	parts, ok := value.([]interface{})
	if !ok || len(parts) != 2 {
		log.Printf("[QUOTA] invalid index byte accounting response (tenant=%s): %#v", tenantID, value)
		return indexQuotaDecision{Unavailable: true, Limit: limit}
	}
	allowed, allowedOK := parts[0].(int64)
	used, usedOK := parts[1].(int64)
	if !allowedOK || !usedOK {
		log.Printf("[QUOTA] invalid index byte accounting values (tenant=%s): %#v", tenantID, parts)
		return indexQuotaDecision{Unavailable: true, Limit: limit}
	}
	charged := int64(0)
	if allowed == 1 {
		charged = bytes
	}
	return indexQuotaDecision{Allowed: allowed == 1, Used: used, Limit: limit, Charged: charged}
}

// chargeIndexFiles 原子检查配额并记录当天已计费的文件身份。相同 root、路径和
// 内容哈希的失败重试只会得到 Charged=0；内容改变后会作为新的 embedding 输入计费。
func chargeIndexFiles(tenantID, orgID, tier, rootID string, files []indexQuotaFile) indexQuotaDecision {
	requested := int64(0)
	for _, file := range files {
		if file.Bytes > 0 {
			requested += file.Bytes
		}
	}
	quota := resolveIndexBytesQuota(tenantID, orgID, tier)
	limit := quota.Limit
	if requested <= 0 {
		return indexQuotaDecision{Allowed: true, Limit: limit}
	}
	if limit <= 0 {
		metricIndexBytes.Add(float64(requested))
		return indexQuotaDecision{Allowed: true, Limit: limit, Charged: requested}
	}

	args := make([]interface{}, 0, 3+len(files)*2)
	args = append(args, limit, int64((48*time.Hour)/time.Second), len(files))
	for _, file := range files {
		bytes := max(file.Bytes, 0)
		identity := strings.Join([]string{
			normalizeIndexPath(file.Path),
			strings.ToLower(strings.TrimSpace(file.Hash)),
			strconv.FormatInt(bytes, 10),
		}, "\x00")
		token := sha256.Sum256([]byte(identity))
		args = append(args, fmt.Sprintf("%x", token), bytes)
	}

	const reserveFilesScript = `
		local current = tonumber(redis.call('GET', KEYS[1]) or '0')
		local limit = tonumber(ARGV[1])
		local ttl = tonumber(ARGV[2])
		local count = tonumber(ARGV[3])
		local newly_charged = 0
		local unseen = {}
		for i = 1, count do
			local token_index = 4 + (i - 1) * 2
			local token = ARGV[token_index]
			local bytes = tonumber(ARGV[token_index + 1])
			if bytes > 0 and unseen[token] == nil and redis.call('HEXISTS', KEYS[2], token) == 0 then
				unseen[token] = bytes
				newly_charged = newly_charged + bytes
			end
		end
		if current + newly_charged > limit then
			return {0, current, 0}
		end
		if newly_charged > 0 then
			for token, bytes in pairs(unseen) do
				redis.call('HSET', KEYS[2], token, bytes)
			end
			current = redis.call('INCRBY', KEYS[1], newly_charged)
			if redis.call('TTL', KEYS[1]) < 0 then redis.call('EXPIRE', KEYS[1], ttl) end
			if redis.call('TTL', KEYS[2]) < 0 then redis.call('EXPIRE', KEYS[2], ttl) end
		end
		return {1, current, newly_charged}
	`
	now := time.Now()
	value, err := redisClient.Eval(
		context.Background(),
		reserveFilesScript,
		[]string{indexBytesKeyAt(quota.PoolID, now), indexFileChargesKeyAt(tenantID, rootID, now)},
		args...,
	).Result()
	if err != nil {
		log.Printf("[QUOTA] idempotent index byte accounting failed (tenant=%s root=%s): %v", tenantID, rootID, err)
		return indexQuotaDecision{Unavailable: true, Limit: limit}
	}
	parts, ok := value.([]interface{})
	if !ok || len(parts) != 3 {
		log.Printf("[QUOTA] invalid idempotent index byte accounting response (tenant=%s root=%s): %#v", tenantID, rootID, value)
		return indexQuotaDecision{Unavailable: true, Limit: limit}
	}
	allowed, allowedOK := parts[0].(int64)
	used, usedOK := parts[1].(int64)
	charged, chargedOK := parts[2].(int64)
	if !allowedOK || !usedOK || !chargedOK {
		log.Printf("[QUOTA] invalid idempotent index byte accounting values (tenant=%s root=%s): %#v", tenantID, rootID, parts)
		return indexQuotaDecision{Unavailable: true, Limit: limit}
	}
	decision := indexQuotaDecision{Allowed: allowed == 1, Used: used, Limit: limit, Charged: charged}
	if decision.Allowed && decision.Charged > 0 {
		metricIndexBytes.Add(float64(decision.Charged))
	}
	if !decision.Allowed {
		logEvent("index_quota_rejected",
			"user_id", tenantID,
			"tenant", tenantID,
			"org", orgID,
			"tier", normalizeTier(tier),
			"root_id", rootID,
			"bytes", strconv.FormatInt(requested, 10),
			"used", strconv.FormatInt(decision.Used, 10),
			"limit", strconv.FormatInt(decision.Limit, 10),
		)
	} else if decision.Charged < requested {
		logEvent("index_quota_replay_deduplicated",
			"user_id", tenantID,
			"tenant", tenantID,
			"org", orgID,
			"root_id", rootID,
			"bytes", strconv.FormatInt(requested, 10),
			"charged", strconv.FormatInt(decision.Charged, 10),
		)
	}
	return decision
}

type requestQuotaDecision struct {
	Allowed     bool
	Unavailable bool
	Used        int64
	Limit       int
	Scope       string
}

const reserveRequestQuotaScript = `
	local member_limit = tonumber(ARGV[1])
	local pool_limit = tonumber(ARGV[2])
	local ttl = tonumber(ARGV[3])
	local member_used = 0
	local pool_used = 0

	if member_limit > 0 then
		member_used = tonumber(redis.call('GET', KEYS[1]) or '0')
		if member_used >= member_limit then
			return {0, member_used, member_limit, 1}
		end
	end
	if pool_limit > 0 then
		pool_used = tonumber(redis.call('GET', KEYS[2]) or '0')
		if pool_used >= pool_limit then
			return {0, pool_used, pool_limit, 2}
		end
	end

	if member_limit > 0 then
		member_used = redis.call('INCR', KEYS[1])
		if member_used == 1 then redis.call('EXPIRE', KEYS[1], ttl) end
	end
	if pool_limit > 0 then
		pool_used = redis.call('INCR', KEYS[2])
		if pool_used == 1 then redis.call('EXPIRE', KEYS[2], ttl) end
	end

	if pool_limit > 0 then return {1, pool_used, pool_limit, 2} end
	if member_limit > 0 then return {1, member_used, member_limit, 1} end
	return {1, 0, 0, 0}
`

func reserveRequestQuota(
	ctx context.Context,
	memberKey string,
	memberLimit int,
	poolKey string,
	poolLimit int,
) requestQuotaDecision {
	if memberLimit <= 0 && poolLimit <= 0 {
		return requestQuotaDecision{Allowed: true}
	}
	value, err := redisClient.Eval(
		ctx,
		reserveRequestQuotaScript,
		[]string{memberKey, poolKey},
		memberLimit,
		poolLimit,
		int64((48*time.Hour)/time.Second),
	).Result()
	if err != nil {
		log.Printf("[QUOTA] request accounting failed: %v", err)
		return requestQuotaDecision{Unavailable: true}
	}
	parts, ok := value.([]interface{})
	if !ok || len(parts) != 4 {
		log.Printf("[QUOTA] invalid request accounting response: %#v", value)
		return requestQuotaDecision{Unavailable: true}
	}
	allowed, allowedOK := parts[0].(int64)
	used, usedOK := parts[1].(int64)
	limit, limitOK := parts[2].(int64)
	scopeID, scopeOK := parts[3].(int64)
	if !allowedOK || !usedOK || !limitOK || !scopeOK {
		log.Printf("[QUOTA] invalid request accounting values: %#v", parts)
		return requestQuotaDecision{Unavailable: true}
	}
	scope := "personal"
	if scopeID == 1 && poolKey != memberKey {
		scope = "member"
	} else if scopeID == 2 && poolKey != memberKey {
		scope = "organization"
	}
	return requestQuotaDecision{
		Allowed: allowed == 1,
		Used:    used,
		Limit:   int(limit),
		Scope:   scope,
	}
}

// checkRequestQuotaDetailed 原子预占当日请求额度。达到上限后不会继续增加计数；
// 组织成员上限与组织共享池在同一 Lua 脚本中检查和扣减，避免组织池拒绝后仍消耗
// 成员额度。Redis 故障时返回 Unavailable，由 HTTP 边界 fail closed。
func checkRequestQuotaDetailed(userID, orgID, tier string) requestQuotaDecision {
	ctx := context.Background()
	day := time.Now().In(quotaLocation()).Format("20060102")

	if orgID == "" {
		limit := getUserDailyLimit(userID, tier)
		key := "quota:used:" + userID + ":" + day
		return reserveRequestQuota(ctx, key, 0, key, limit)
	}

	memberLimit := 0
	configured, exists, memberErr := getMemberDailyLimit(orgID, userID)
	if memberErr != nil {
		return requestQuotaDecision{Unavailable: true}
	}
	if exists {
		memberLimit = configured
	}
	orgLimit := int64(tierDailyRequestLimit(tierFree))
	limits := getOrgQuotaLimits(orgID)
	if limits.RequestSet {
		orgLimit = limits.Request
	}
	memberKey := "quota:used:org:" + orgID + ":" + userID + ":" + day
	poolID := orgID
	if limits.RequestPoolID != "" {
		poolID = limits.RequestPoolID
	}
	orgKey := "quota:used:" + poolID + ":" + day
	return reserveRequestQuota(ctx, memberKey, memberLimit, orgKey, int(orgLimit))
}

// checkRequestQuota 保留既有内部调用契约；需要向客户端返回已用量和作用域时使用
// checkRequestQuotaDetailed。
//
// 个人租户（orgID 空）：沿用 getUserDailyLimit + quota:used:{user}。
// 组织租户：先检查按 (org, user) 隔离的成员上限，再检查权益池。显式 org_quotas
// 使用独立组织池；继承额度则使用 canonical owner 的同一计数池，因此 owner 的
// 个人请求及其名下多个组织不会把同一份套餐重复放大。
func checkRequestQuota(userID, orgID, tier string) (bool, int) {
	decision := checkRequestQuotaDetailed(userID, orgID, tier)
	return decision.Allowed, decision.Limit
}
