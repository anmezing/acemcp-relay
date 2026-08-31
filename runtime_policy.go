package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// Runtime policy defaults live in one place so deployment-specific limits,
// timeouts, cache lifetimes and scheduler intervals can be tuned without
// editing request handlers. Protocol identifiers, security invariants and
// schema versions remain compile-time constants in their owning modules.
const (
	defaultServerAddr = "127.0.0.1:3009"
	defaultLCEMCPURL  = "http://127.0.0.1:3000/mcp"
	defaultPprofAddr  = "127.0.0.1:6060"

	defaultIndexJobHeartbeatTimeout = 10 * time.Minute
	defaultIndexJobSweepInterval    = time.Minute
	defaultIndexJobRenewCallTimeout = 15 * time.Second
	defaultMaxIndexManifestFiles    = 100000
	defaultMaxIndexBatchFiles       = 50
	defaultMaxIndexFileBytes        = 512 << 10
	defaultMaxIndexBatchBytes       = 512 << 10
	defaultMaxLCEMCPRequestBody     = 4 << 20
	defaultMaxClientMCPRequestBody  = 32 << 20
	defaultMaxIndexPathBytes        = 4096
	defaultMaxIndexFailureBytes     = 2000
	defaultIndexStartMinInterval    = 30 * time.Second
	defaultIndexStartSeenMaxEntries = 4096

	defaultMCPCallTimeout            = 120 * time.Second
	defaultRemoteIndexMCPCallTimeout = 330 * time.Second
	defaultMCPInitSessionTimeout     = 10 * time.Second
	defaultMCPHTTPIdleConnTimeout    = 90 * time.Second
	defaultMCPHTTPMaxIdleConns       = 50
	defaultMCPHTTPMaxIdlePerHost     = 50
	defaultMCPSessionTTL             = 30 * time.Minute
	defaultMCPSessionSweepInterval   = time.Minute
	defaultMCPToolsCacheTTL          = 5 * time.Minute
	defaultMCPMaxSessions            = 1000
	defaultMCPMaxSessionsPerUser     = 16

	defaultPlatformConfigBodyLimit   = 64 << 10
	defaultPlatformConfigReadTimeout = 15 * time.Second
	defaultPlatformDiscoveryTimeout  = 30 * time.Second
	defaultPlatformValidationTimeout = 35 * time.Second
	defaultPlatformConfigBarrierWait = 10 * time.Second
	defaultPlatformRelayClearTimeout = 30 * time.Second
	defaultPlatformSimpleSaveTimeout = 15 * time.Second
	defaultPlatformResetSaveTimeout  = 60 * time.Second
	defaultPlatformConfigLockPoll    = 50 * time.Millisecond

	defaultLeaderboardUpdateInterval = 30 * time.Minute
	defaultLeaderboardTopN           = 10
	defaultLeaderboardTimezone       = "Asia/Shanghai"
	defaultHealthCheckInterval       = 2 * time.Minute
	defaultHealthCheckTimeout        = 30 * time.Second

	defaultDBHost                = "localhost"
	defaultDBPort                = 5432
	defaultDBUser                = "postgres"
	defaultDBName                = "postgres"
	defaultRedisHost             = "localhost"
	defaultRedisPort             = 6379
	defaultFreeDailyRequestLimit = 0
	defaultDailyIndexBytes       = int64(2 << 30)
	defaultProRequestLimit       = 0
	defaultProIndexBytes         = int64(0)
	defaultDebugCaptureBytes     = 4096

	defaultDBMaxOpenConns    = 40
	defaultDBMaxIdleConns    = 40
	defaultDBConnMaxLifetime = 30 * time.Minute
	defaultDBConnMaxIdleTime = 5 * time.Minute

	defaultBannedCacheTTL        = 5 * time.Minute
	defaultAuthCachePositiveTTL  = 30 * time.Second
	defaultAuthCacheNegativeTTL  = 5 * time.Second
	defaultAuthCacheMaxEntries   = 10000
	defaultModelConfigCacheTTL   = 5 * time.Minute
	defaultQuotaCacheTTL         = 5 * time.Minute
	defaultQuotaCounterTTL       = 48 * time.Hour
	defaultRequestLogStaleAfter  = 15 * time.Minute
	defaultRequestLogReconcile   = 5 * time.Minute
	defaultClearIndexCooldown    = 72 * time.Hour
	defaultDeleteRootMinInterval = time.Minute
	defaultDeleteRootSeenEntries = 4096

	defaultIndexOperationLeaseDuration = 2 * time.Minute
	defaultIndexOperationRenewInterval = 30 * time.Second
	defaultIndexOperationAcquirePoll   = 100 * time.Millisecond
	defaultIndexOperationDBTimeout     = 5 * time.Second
)

var (
	serverAddr = defaultServerAddr
	lceMCPURL  = defaultLCEMCPURL
	pprofAddr  = defaultPprofAddr

	indexJobHeartbeatTimeout  = defaultIndexJobHeartbeatTimeout
	indexJobSweepInterval     = defaultIndexJobSweepInterval
	indexJobRenewCallTimeout  = defaultIndexJobRenewCallTimeout
	maxIndexManifestFiles     = defaultMaxIndexManifestFiles
	maxIndexBatchFiles        = defaultMaxIndexBatchFiles
	maxIndexFileBytes         = defaultMaxIndexFileBytes
	maxIndexBatchBytes        = defaultMaxIndexBatchBytes
	maxLCEMCPRequestBodyBytes = defaultMaxLCEMCPRequestBody
	maxClientMCPBody          = defaultMaxClientMCPRequestBody
	maxIndexPathBytes         = defaultMaxIndexPathBytes
	maxIndexFailureBytes      = defaultMaxIndexFailureBytes
	indexStartMinInterval     = defaultIndexStartMinInterval
	indexStartSeenMaxEntries  = defaultIndexStartSeenMaxEntries

	mcpCallTimeout             = defaultMCPCallTimeout
	remoteIndexMCPCallTimeout  = defaultRemoteIndexMCPCallTimeout
	mcpInitSessionTimeout      = defaultMCPInitSessionTimeout
	mcpHTTPIdleConnTimeout     = defaultMCPHTTPIdleConnTimeout
	mcpHTTPMaxIdleConns        = defaultMCPHTTPMaxIdleConns
	mcpHTTPMaxIdlePerHost      = defaultMCPHTTPMaxIdlePerHost
	mcpSessionTTL              = defaultMCPSessionTTL
	mcpSessionSweepInterval    = defaultMCPSessionSweepInterval
	toolsCacheTTL              = defaultMCPToolsCacheTTL
	mcpMaxSessions             = defaultMCPMaxSessions
	mcpMaxSessionsPerUser      = defaultMCPMaxSessionsPerUser
	maxPlatformModelConfigBody = defaultPlatformConfigBodyLimit

	platformModelConfigReadTimeout       = defaultPlatformConfigReadTimeout
	platformModelDiscoveryTimeout        = defaultPlatformDiscoveryTimeout
	platformModelConfigValidationTimeout = defaultPlatformValidationTimeout
	platformModelConfigBarrierWait       = defaultPlatformConfigBarrierWait
	platformModelConfigRelayClearTimeout = defaultPlatformRelayClearTimeout
	platformModelConfigSimpleSaveTimeout = defaultPlatformSimpleSaveTimeout
	platformModelConfigResetSaveTimeout  = defaultPlatformResetSaveTimeout
	platformModelConfigLockPollInterval  = defaultPlatformConfigLockPoll

	leaderboardUpdateInterval = defaultLeaderboardUpdateInterval
	leaderboardTopN           = defaultLeaderboardTopN
	leaderboardTimezone       = defaultLeaderboardTimezone
	healthCheckInterval       = defaultHealthCheckInterval
	healthCheckTimeout        = defaultHealthCheckTimeout

	dbHost                   = defaultDBHost
	dbPort                   = defaultDBPort
	dbUser                   = defaultDBUser
	dbPassword               string
	dbName                   = defaultDBName
	redisHost                = defaultRedisHost
	redisPort                = defaultRedisPort
	defaultDailyRequestLimit = defaultFreeDailyRequestLimit
	dailyIndexBytesLimit     = defaultDailyIndexBytes
	proDailyRequestLimit     = defaultProRequestLimit
	proDailyIndexBytesLimit  = defaultProIndexBytes
	debugCaptureMaxBytes     = defaultDebugCaptureBytes

	dbMaxOpenConns    = defaultDBMaxOpenConns
	dbMaxIdleConns    = defaultDBMaxIdleConns
	dbConnMaxLifetime = defaultDBConnMaxLifetime
	dbConnMaxIdleTime = defaultDBConnMaxIdleTime

	bannedCacheTTL              = defaultBannedCacheTTL
	authCachePositiveTTL        = defaultAuthCachePositiveTTL
	authCacheNegativeTTL        = defaultAuthCacheNegativeTTL
	authCacheMaxEntries         = defaultAuthCacheMaxEntries
	modelConfigCacheTTL         = defaultModelConfigCacheTTL
	quotaCacheTTL               = defaultQuotaCacheTTL
	quotaCounterTTL             = defaultQuotaCounterTTL
	staleRequestLogAfter        = defaultRequestLogStaleAfter
	requestLogReconcileInterval = defaultRequestLogReconcile
	clearIndexCooldown          = defaultClearIndexCooldown
	deleteRootMinInterval       = defaultDeleteRootMinInterval
	deleteRootSeenMaxEntry      = defaultDeleteRootSeenEntries
	indexOperationLeaseDuration = defaultIndexOperationLeaseDuration
	indexOperationRenewInterval = defaultIndexOperationRenewInterval
	indexOperationAcquirePoll   = defaultIndexOperationAcquirePoll
	indexOperationDBTimeout     = defaultIndexOperationDBTimeout
)

func loadRuntimePolicy() {
	serverAddr = nonEmptyStringEnv("SERVER_ADDR", defaultServerAddr)
	lceMCPURL = strings.TrimRight(nonEmptyStringEnv("LCE_MCP_URL", defaultLCEMCPURL), "/")
	pprofAddr = nonEmptyStringEnv("PPROF_ADDR", defaultPprofAddr)

	indexJobHeartbeatTimeout = positiveDurationEnv("INDEX_JOB_HEARTBEAT_TIMEOUT", defaultIndexJobHeartbeatTimeout)
	indexJobSweepInterval = positiveDurationEnv("INDEX_JOB_SWEEP_INTERVAL", defaultIndexJobSweepInterval)
	indexJobRenewCallTimeout = positiveDurationEnv("INDEX_JOB_RENEW_TIMEOUT", defaultIndexJobRenewCallTimeout)
	maxIndexManifestFiles = positiveIntEnv("INDEX_MAX_MANIFEST_FILES", defaultMaxIndexManifestFiles)
	maxIndexBatchFiles = positiveIntEnv("INDEX_MAX_BATCH_FILES", defaultMaxIndexBatchFiles)
	maxIndexFileBytes = positiveIntEnv("INDEX_MAX_FILE_BYTES", defaultMaxIndexFileBytes)
	maxIndexBatchBytes = positiveIntEnv("INDEX_MAX_BATCH_BYTES", defaultMaxIndexBatchBytes)
	maxLCEMCPRequestBodyBytes = positiveIntEnv("LCE_MCP_REQUEST_BODY_LIMIT_BYTES", defaultMaxLCEMCPRequestBody)
	maxClientMCPBody = positiveIntEnv("MCP_CLIENT_REQUEST_BODY_LIMIT_BYTES", defaultMaxClientMCPRequestBody)
	maxIndexPathBytes = positiveIntEnv("INDEX_MAX_PATH_BYTES", defaultMaxIndexPathBytes)
	maxIndexFailureBytes = positiveIntEnv("INDEX_MAX_FAILURE_BYTES", defaultMaxIndexFailureBytes)
	indexStartMinInterval = positiveDurationEnv("INDEX_START_MIN_INTERVAL", defaultIndexStartMinInterval)
	indexStartSeenMaxEntries = positiveIntEnv("INDEX_START_MEMORY_ENTRIES", defaultIndexStartSeenMaxEntries)

	mcpCallTimeout = positiveDurationEnv("MCP_CALL_TIMEOUT", defaultMCPCallTimeout)
	remoteIndexMCPCallTimeout = positiveDurationEnv("INDEX_MCP_CALL_TIMEOUT", defaultRemoteIndexMCPCallTimeout)
	mcpInitSessionTimeout = positiveDurationEnv("MCP_INIT_SESSION_TIMEOUT", defaultMCPInitSessionTimeout)
	mcpHTTPIdleConnTimeout = positiveDurationEnv("MCP_HTTP_IDLE_CONN_TIMEOUT", defaultMCPHTTPIdleConnTimeout)
	mcpHTTPMaxIdleConns = positiveIntEnv("MCP_HTTP_MAX_IDLE_CONNS", defaultMCPHTTPMaxIdleConns)
	mcpHTTPMaxIdlePerHost = positiveIntEnv("MCP_HTTP_MAX_IDLE_CONNS_PER_HOST", defaultMCPHTTPMaxIdlePerHost)
	mcpSessionTTL = positiveDurationEnv("MCP_SESSION_TTL", defaultMCPSessionTTL)
	mcpSessionSweepInterval = positiveDurationEnv("MCP_SESSION_SWEEP_INTERVAL", defaultMCPSessionSweepInterval)
	toolsCacheTTL = positiveDurationEnv("MCP_TOOLS_CACHE_TTL", defaultMCPToolsCacheTTL)
	mcpMaxSessions = positiveIntEnv("MCP_MAX_SESSIONS", defaultMCPMaxSessions)
	mcpMaxSessionsPerUser = positiveIntEnv("MCP_MAX_SESSIONS_PER_USER", defaultMCPMaxSessionsPerUser)

	maxPlatformModelConfigBody = positiveIntEnv("PLATFORM_MODEL_CONFIG_BODY_LIMIT_BYTES", defaultPlatformConfigBodyLimit)
	platformModelConfigReadTimeout = positiveDurationEnv("PLATFORM_MODEL_CONFIG_READ_TIMEOUT", defaultPlatformConfigReadTimeout)
	platformModelDiscoveryTimeout = positiveDurationEnv("PLATFORM_MODEL_DISCOVERY_TIMEOUT", defaultPlatformDiscoveryTimeout)
	platformModelConfigValidationTimeout = positiveDurationEnv("PLATFORM_MODEL_CONFIG_VALIDATION_TIMEOUT", defaultPlatformValidationTimeout)
	platformModelConfigBarrierWait = positiveDurationEnv("PLATFORM_MODEL_CONFIG_BARRIER_WAIT", defaultPlatformConfigBarrierWait)
	platformModelConfigRelayClearTimeout = positiveDurationEnv("PLATFORM_MODEL_CONFIG_CLEAR_TIMEOUT", defaultPlatformRelayClearTimeout)
	platformModelConfigSimpleSaveTimeout = positiveDurationEnv("PLATFORM_MODEL_CONFIG_SAVE_TIMEOUT", defaultPlatformSimpleSaveTimeout)
	platformModelConfigResetSaveTimeout = positiveDurationEnv("PLATFORM_MODEL_CONFIG_RESET_SAVE_TIMEOUT", defaultPlatformResetSaveTimeout)
	platformModelConfigLockPollInterval = positiveDurationEnv("PLATFORM_MODEL_CONFIG_LOCK_POLL_INTERVAL", defaultPlatformConfigLockPoll)

	leaderboardUpdateInterval = positiveDurationEnv("LEADERBOARD_UPDATE_INTERVAL", defaultLeaderboardUpdateInterval)
	leaderboardTopN = positiveIntEnv("LEADERBOARD_TOP_N", defaultLeaderboardTopN)
	leaderboardTimezone = nonEmptyStringEnv("LEADERBOARD_TIMEZONE", defaultLeaderboardTimezone)
	healthCheckInterval = positiveDurationEnv("HEALTH_CHECK_INTERVAL", defaultHealthCheckInterval)
	healthCheckTimeout = positiveDurationEnv("HEALTH_CHECK_TIMEOUT", defaultHealthCheckTimeout)

	dbHost = nonEmptyStringEnv("DB_HOST", defaultDBHost)
	dbPort = positiveIntEnv("DB_PORT", defaultDBPort)
	dbUser = nonEmptyStringEnv("DB_USER", defaultDBUser)
	dbPassword = os.Getenv("DB_PASSWORD")
	dbName = nonEmptyStringEnv("DB_NAME", defaultDBName)
	redisHost = nonEmptyStringEnv("REDIS_HOST", defaultRedisHost)
	redisPort = positiveIntEnv("REDIS_PORT", defaultRedisPort)
	defaultDailyRequestLimit = nonNegativeIntEnv("DEFAULT_DAILY_REQUEST_LIMIT", defaultFreeDailyRequestLimit)
	dailyIndexBytesLimit = nonNegativeInt64Env("DAILY_INDEX_BYTES_LIMIT", defaultDailyIndexBytes)
	proDailyRequestLimit = nonNegativeIntEnv("PRO_DAILY_REQUEST_LIMIT", defaultProRequestLimit)
	proDailyIndexBytesLimit = nonNegativeInt64Env("PRO_DAILY_INDEX_BYTES_LIMIT", defaultProIndexBytes)
	debugCaptureMaxBytes = positiveIntEnv("DEBUG_CAPTURE_MAX_BYTES", defaultDebugCaptureBytes)

	dbMaxOpenConns = positiveIntEnv("DB_MAX_OPEN_CONNS", defaultDBMaxOpenConns)
	dbMaxIdleConns = positiveIntEnv("DB_MAX_IDLE_CONNS", defaultDBMaxIdleConns)
	dbConnMaxLifetime = positiveDurationEnv("DB_CONN_MAX_LIFETIME", defaultDBConnMaxLifetime)
	dbConnMaxIdleTime = positiveDurationEnv("DB_CONN_MAX_IDLE_TIME", defaultDBConnMaxIdleTime)

	bannedCacheTTL = positiveDurationEnv("BANNED_CACHE_TTL", defaultBannedCacheTTL)
	authCachePositiveTTL = positiveDurationEnv("AUTH_CACHE_POSITIVE_TTL", defaultAuthCachePositiveTTL)
	authCacheNegativeTTL = positiveDurationEnv("AUTH_CACHE_NEGATIVE_TTL", defaultAuthCacheNegativeTTL)
	authCacheMaxEntries = positiveIntEnv("AUTH_CACHE_MAX_ENTRIES", defaultAuthCacheMaxEntries)
	modelConfigCacheTTL = positiveDurationEnv("MODEL_CONFIG_CACHE_TTL", defaultModelConfigCacheTTL)
	quotaCacheTTL = positiveDurationEnv("QUOTA_CACHE_TTL", defaultQuotaCacheTTL)
	quotaCounterTTL = positiveDurationEnv("QUOTA_COUNTER_TTL", defaultQuotaCounterTTL)
	staleRequestLogAfter = positiveDurationEnv("REQUEST_LOG_STALE_AFTER", defaultRequestLogStaleAfter)
	requestLogReconcileInterval = positiveDurationEnv("REQUEST_LOG_RECONCILE_INTERVAL", defaultRequestLogReconcile)
	clearIndexCooldown = positiveDurationEnv("CLEAR_INDEX_COOLDOWN", defaultClearIndexCooldown)
	deleteRootMinInterval = positiveDurationEnv("DELETE_ROOT_MIN_INTERVAL", defaultDeleteRootMinInterval)
	deleteRootSeenMaxEntry = positiveIntEnv("DELETE_ROOT_MEMORY_ENTRIES", defaultDeleteRootSeenEntries)

	indexOperationLeaseDuration = positiveDurationEnv("INDEX_OPERATION_LEASE_DURATION", defaultIndexOperationLeaseDuration)
	indexOperationRenewInterval = positiveDurationEnv("INDEX_OPERATION_RENEW_INTERVAL", defaultIndexOperationRenewInterval)
	indexOperationAcquirePoll = positiveDurationEnv("INDEX_OPERATION_ACQUIRE_POLL_INTERVAL", defaultIndexOperationAcquirePoll)
	indexOperationDBTimeout = positiveDurationEnv("INDEX_OPERATION_DB_TIMEOUT", defaultIndexOperationDBTimeout)

	if maxIndexFileBytes > maxIndexBatchBytes {
		log.Printf("[CONFIG] INDEX_MAX_FILE_BYTES=%d exceeds INDEX_MAX_BATCH_BYTES=%d; clamping file limit to batch limit", maxIndexFileBytes, maxIndexBatchBytes)
		maxIndexFileBytes = maxIndexBatchBytes
	}
	if mcpMaxSessionsPerUser > mcpMaxSessions {
		log.Printf("[CONFIG] MCP_MAX_SESSIONS_PER_USER=%d exceeds MCP_MAX_SESSIONS=%d; clamping per-user limit", mcpMaxSessionsPerUser, mcpMaxSessions)
		mcpMaxSessionsPerUser = mcpMaxSessions
	}
	if dbMaxIdleConns > dbMaxOpenConns {
		log.Printf("[CONFIG] DB_MAX_IDLE_CONNS=%d exceeds DB_MAX_OPEN_CONNS=%d; clamping idle connections", dbMaxIdleConns, dbMaxOpenConns)
		dbMaxIdleConns = dbMaxOpenConns
	}
	if indexOperationRenewInterval >= indexOperationLeaseDuration {
		adjusted := indexOperationLeaseDuration / 2
		if adjusted <= 0 {
			adjusted = defaultIndexOperationRenewInterval
		}
		log.Printf("[CONFIG] INDEX_OPERATION_RENEW_INTERVAL=%s must be shorter than INDEX_OPERATION_LEASE_DURATION=%s; using %s", indexOperationRenewInterval, indexOperationLeaseDuration, adjusted)
		indexOperationRenewInterval = adjusted
	}
}

func positiveIntEnv(key string, fallback int) int {
	value, err := parsePositiveInt(os.Getenv(key), fallback)
	if err != nil {
		log.Printf("[CONFIG] invalid %s=%q: %v; falling back to %d", key, os.Getenv(key), err, fallback)
	}
	return value
}

func positiveDurationEnv(key string, fallback time.Duration) time.Duration {
	value, err := parsePositiveDuration(os.Getenv(key), fallback)
	if err != nil {
		log.Printf("[CONFIG] invalid %s=%q: %v; falling back to %s", key, os.Getenv(key), err, fallback)
	}
	return value
}

func nonNegativeIntEnv(key string, fallback int) int {
	value, err := parseNonNegativeInt(os.Getenv(key), fallback)
	if err != nil {
		log.Printf("[CONFIG] invalid %s=%q: %v; falling back to %d", key, os.Getenv(key), err, fallback)
	}
	return value
}

func nonNegativeInt64Env(key string, fallback int64) int64 {
	value, err := parseNonNegativeInt64(os.Getenv(key), fallback)
	if err != nil {
		log.Printf("[CONFIG] invalid %s=%q: %v; falling back to %d", key, os.Getenv(key), err, fallback)
	}
	return value
}

func nonEmptyStringEnv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func parsePositiveInt(raw string, fallback int) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback, fmt.Errorf("must be a positive integer")
	}
	return value, nil
}

func parseNonNegativeInt(raw string, fallback int) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return fallback, fmt.Errorf("must be a non-negative integer")
	}
	return value, nil
}

func parseNonNegativeInt64(raw string, fallback int64) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		return fallback, fmt.Errorf("must be a non-negative 64-bit integer")
	}
	return value, nil
}

func parsePositiveDuration(raw string, fallback time.Duration) (time.Duration, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return fallback, fmt.Errorf("must be a positive duration")
	}
	return value, nil
}

func maxIndexEstimatedChunks() int {
	return (maxIndexFileBytes + estimatedIndexChunkBytes - 1) / estimatedIndexChunkBytes
}

func byteLimitMessage(prefix string, limit int) string {
	return fmt.Sprintf("%s exceeds %d bytes", prefix, limit)
}
