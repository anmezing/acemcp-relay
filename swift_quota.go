package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const (
	swiftSyncToolName           = "codebase_swift_sync"
	swiftSyncPartBytes          = 262144
	swiftSyncMaxParts           = 512
	swiftSyncRequestsPerMinute  = 600
	contextRequestQuotaDeferred = "request_quota_deferred"
	swiftPartChargeRetention    = 48 * time.Hour
)

func admitDailyRequestQuota(c *gin.Context) bool {
	userID, orgID, tier := c.GetString(ContextKeyUserID), c.GetString(ContextKeyOrgID), c.GetString(ContextKeyUserTier)
	quota := checkRequestQuotaDetailed(userID, orgID, tier)
	if quota.Unavailable {
		logEvent("quota_unavailable", "user_id", userID, "tenant", requestTenantID(c), "tier", tier, "path", c.Request.URL.Path)
		c.Header("Retry-After", "5")
		c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "quota accounting temporarily unavailable", "code": "QUOTA_ACCOUNTING_UNAVAILABLE"})
		return false
	}
	if !quota.Allowed {
		now := time.Now()
		logEvent("quota_rejected", "user_id", userID, "tenant", requestTenantID(c), "tier", tier, "path", c.Request.URL.Path, "used", strconv.FormatInt(quota.Used, 10), "limit", strconv.Itoa(quota.Limit), "scope", quota.Scope)
		c.Header("Retry-After", quotaRetryAfterHeader(now))
		c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": fmt.Sprintf("daily request quota exceeded (%d/day)", quota.Limit), "code": "PLAN_QUOTA_EXHAUSTED", "resource": "requests", "scope": quota.Scope, "used": quota.Used, "limit": quota.Limit, "reset_at": quotaResetAt(now).Format(time.RFC3339)})
		return false
	}
	return true
}

func isSwiftSynchronizationRequest(rpc jsonRPCRequest) bool {
	if rpc.Method != "tools/call" {
		return false
	}
	var params struct {
		Name      string                 `json:"name"`
		Arguments map[string]interface{} `json:"arguments"`
	}
	if json.Unmarshal(rpc.Params, &params) != nil || strings.TrimSpace(params.Name) != swiftSyncToolName || validateChatMCPToolArgs(swiftSyncToolName, params.Arguments) != nil {
		return false
	}
	operation, _ := params.Arguments["operation"].(string)
	switch operation {
	case "manifest", "plan", "begin", "upload", "publish", "status", "abort":
		return true
	}
	return false
}

func admitDeferredMCPQuota(c *gin.Context, rpc jsonRPCRequest) bool {
	if !c.GetBool(contextRequestQuotaDeferred) {
		return true
	}
	c.Set(contextRequestQuotaDeferred, false)
	if !isSwiftSynchronizationRequest(rpc) {
		return admitDailyRequestQuota(c)
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()
	if redisClient == nil {
		c.Header("Retry-After", "5")
		c.AbortWithStatusJSON(503, gin.H{"error": "Synchronization accounting unavailable", "code": "QUOTA_ACCOUNTING_UNAVAILABLE"})
		return false
	}
	const rateScript = `local used=tonumber(redis.call('GET',KEYS[1]) or '0')
  if used>=tonumber(ARGV[1]) then return 0 end
  redis.call('INCR',KEYS[1]); if used==0 then redis.call('EXPIRE',KEYS[1],60) end
  return 1`
	allowed, err := redisClient.Eval(ctx, rateScript, []string{"quota:swift-rate:" + requestTenantID(c)}, swiftSyncRequestsPerMinute).Int64()
	if err != nil {
		c.Header("Retry-After", "5")
		c.AbortWithStatusJSON(503, gin.H{"error": "Synchronization accounting unavailable", "code": "QUOTA_ACCOUNTING_UNAVAILABLE"})
		return false
	}
	if allowed != 1 {
		c.Header("Retry-After", "60")
		c.AbortWithStatusJSON(429, gin.H{"error": "Synchronization rate limit exceeded", "code": "SYNC_RATE_LIMITED"})
		return false
	}
	return true
}

type swiftQuotaError struct{ Code, Message string }

func reserveSwiftUpload(ctx context.Context, tenantID, orgID, tier string, args map[string]interface{}) *swiftQuotaError {
	if args["operation"] != "upload" {
		return nil
	}
	data, ok := args["data"].(string)
	jobID, jobOK := args["job_id"].(string)
	job, jobErr := uuid.Parse(jobID)
	part, partOK := numberAsInt64(args["part"])
	partNumber, _ := args["part"].(float64)
	root, rootOK := args["root_id"].(string)
	if !ok || !jobOK || jobErr != nil || !partOK || math.IsNaN(partNumber) || math.IsInf(partNumber, 0) || math.Trunc(partNumber) != partNumber || part < 0 || part >= swiftSyncMaxParts || !rootOK || strings.TrimSpace(root) == "" || len(data) > base64.StdEncoding.EncodedLen(swiftSyncPartBytes) {
		return &swiftQuotaError{"SWIFT_SYNC_INVALID", "Invalid Swift upload identity or part size"}
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(data)
	if err != nil || len(decoded) == 0 || len(decoded) > swiftSyncPartBytes || base64.StdEncoding.EncodeToString(decoded) != data {
		return &swiftQuotaError{"SWIFT_SYNC_INVALID", "Swift upload requires canonical base64 and a bounded nonempty part"}
	}
	if redisClient == nil {
		return &swiftQuotaError{"QUOTA_ACCOUNTING_UNAVAILABLE", "Swift upload byte accounting unavailable; retry later"}
	}
	quota := resolveIndexBytesQuota(tenantID, orgID, tier)
	decision, conflict := chargeSwiftPart(ctx, quota, tenantID, strings.TrimSpace(root), job.String(), part, decoded, time.Now())
	if conflict {
		return &swiftQuotaError{"SWIFT_SYNC_INVALID", "Swift part identity cannot be reused with different content; start a new upload job"}
	}
	if decision.Unavailable {
		return &swiftQuotaError{"QUOTA_ACCOUNTING_UNAVAILABLE", "Swift upload byte accounting unavailable; retry later"}
	}
	if !decision.Allowed {
		return &swiftQuotaError{"PLAN_QUOTA_EXHAUSTED", fmt.Sprintf("Daily index byte quota exceeded (used=%d limit=%d); retry after %s seconds", decision.Used, decision.Limit, quotaRetryAfterHeader(time.Now()))}
	}
	return nil
}

// Part reservations outlive the 30-minute uploading lifecycle and span midnight.
// One field per part bounds each job ledger at 512 entries. Even unlimited plans
// deduplicate; a missing response never causes a second reservation.
func chargeSwiftPart(ctx context.Context, quota indexQuotaPool, tenantID, rootID, jobID string, part int64, data []byte, now time.Time) (indexQuotaDecision, bool) {
	if redisClient == nil {
		return indexQuotaDecision{Unavailable: true, Limit: quota.Limit}, false
	}
	identity := sha256.Sum256([]byte(rootID + "\x00" + jobID))
	digest := sha256.Sum256(data)
	token := fmt.Sprintf("%x:%d", digest, len(data))
	ledger := fmt.Sprintf("quota:swift-parts:%s:%x", tenantID, identity)
	const script = `local current=tonumber(redis.call('GET',KEYS[1]) or '0')
  local previous=redis.call('HGET',KEYS[2],ARGV[1])
  if previous then
   if previous~=ARGV[2] then return {-1,current,0} end
   redis.call('EXPIRE',KEYS[2],ARGV[5]); return {1,current,0}
  end
  local bytes=tonumber(ARGV[3]); local limit=tonumber(ARGV[4])
  if limit>0 and current+bytes>limit then return {0,current,0} end
  current=redis.call('INCRBY',KEYS[1],bytes)
  redis.call('HSET',KEYS[2],ARGV[1],ARGV[2])
  redis.call('EXPIRE',KEYS[1],ARGV[5]); redis.call('EXPIRE',KEYS[2],ARGV[5])
  return {1,current,bytes}`
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	result, err := redisClient.Eval(bounded, script, []string{indexBytesKeyAt(quota.PoolID, now), ledger}, part, token, len(data), quota.Limit, int64(swiftPartChargeRetention/time.Second)).Int64Slice()
	if err != nil || len(result) != 3 {
		return indexQuotaDecision{Unavailable: true, Limit: quota.Limit}, false
	}
	decision := indexQuotaDecision{Allowed: result[0] == 1, Used: result[1], Charged: result[2], Limit: quota.Limit}
	if decision.Charged > 0 {
		metricIndexBytes.Add(float64(decision.Charged))
	}
	return decision, result[0] == -1
}
