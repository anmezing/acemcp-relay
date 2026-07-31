package main

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/andybalholm/brotli"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

// ── 配置 ──────────────────────────────────────────────────────────────────

var (
	serverAddr               string
	lceMCPURL                string
	lceHealthURL             string
	tenantAssertions         *tenantAssertionSigner
	dbHost                   string
	dbPort                   int
	dbUser                   string
	dbPassword               string
	dbName                   string
	redisHost                string
	redisPort                int
	deviceBindingMode        string
	deviceCacheTTL           time.Duration
	deviceIPWindow           time.Duration
	deviceMaxIPs             int
	defaultDailyRequestLimit int
	dailyIndexBytesLimit     int64
	debugCapturePaths        map[string]bool
	debugCaptureMaxBytes     int
)

const (
	ContextKeyUserID     = "user_id"
	ContextKeyStartTime  = "start_time"
	ContextKeyLogID      = "log_id"
	ContextKeyInsertDone = "insert_done"

	StatusPending   = "pending"
	StatusCompleted = "completed"

	LeaderboardUpdateInterval = 30 * time.Minute
	// 检索有两个入口：MCP tools/call（记录时被归一化成下面这个路径）和 REST 端点
	// /relay/agents/codebase-retrieval。排行榜必须双路径统计，否则 REST 用户不被计入。
	LeaderboardPath     = "/mcp/tools/call/codebase-retrieval"
	LeaderboardRESTPath = "/relay/agents/codebase-retrieval"
	LeaderboardTopN     = 10
	LeaderboardTimezone = "Asia/Shanghai"

	HealthCheckInterval = 2 * time.Minute
	HealthCheckTimeout  = 30 * time.Second
)

func loadConfig() {
	_ = godotenv.Load()

	serverAddr = getEnv("SERVER_ADDR", "127.0.0.1:8080")
	lceMCPURL = getEnv("LCE_MCP_URL", "http://127.0.0.1:3000/mcp")
	// LCE 的存活探针：不建 session、不占并发额度，因此可以高频探测。
	// 它只说明进程活着，功能性判断仍由 tools/list 负责。
	// TrimRight 防止 LCE_MCP_URL 带尾斜杠时拼出 "…//health"。
	lceHealthURL = strings.TrimRight(lceMCPURL, "/") + "/health"
	// 与 LCE 共享的租户断言密钥。未配置时不附带断言：LCE 在 loopback 且未配密钥时
	// 不强制校验，本地开发照旧；LCE 一旦配上密钥，未带断言的租户调用会被拒绝。
	signer, err := newTenantAssertionSigner(os.Getenv("LCE_TENANT_ASSERTION_SECRET"))
	if err != nil {
		log.Fatalf("[CONFIG] %v", err)
	}
	tenantAssertions = signer
	if tenantAssertions == nil {
		log.Println("[CONFIG] LCE_TENANT_ASSERTION_SECRET 未配置：不签发租户断言。若 LCE 已开启校验，租户调用会被拒绝")
	}
	dbHost = getEnv("DB_HOST", "localhost")
	dbPort = getEnvInt("DB_PORT", 5432)
	dbUser = getEnv("DB_USER", "postgres")
	dbPassword = getEnv("DB_PASSWORD", "")
	dbName = getEnv("DB_NAME", "postgres")
	redisHost = getEnv("REDIS_HOST", "localhost")
	redisPort = getEnvInt("REDIS_PORT", 6379)
	deviceBindingMode = strings.ToLower(getEnv("DEVICE_BINDING_MODE", DeviceModeLog))
	if deviceBindingMode != DeviceModeOff && deviceBindingMode != DeviceModeLog && deviceBindingMode != DeviceModeEnforce {
		log.Printf("[CONFIG] invalid DEVICE_BINDING_MODE %q, falling back to %q", deviceBindingMode, DeviceModeLog)
		deviceBindingMode = DeviceModeLog
	}
	deviceCacheTTL = getEnvDuration("DEVICE_CACHE_TTL", 5*time.Minute)
	deviceIPWindow = getEnvDuration("DEVICE_IP_WINDOW", 10*time.Minute)
	deviceMaxIPs = getEnvInt("DEVICE_MAX_IPS", 3)
	configureTrustedConsole(getEnv("CONSOLE_API_SECRET", ""))
	defaultDailyRequestLimit = getEnvInt("DEFAULT_DAILY_REQUEST_LIMIT", 0)
	// 索引通道按字节计费，与请求数配额分开：一次 job 创建只算 1 个请求，但随后
	// 的批次上传可以推送任意多的内容，请求数配额对这条路径几乎没有约束力。
	dailyIndexBytesLimit = getEnvInt64("DAILY_INDEX_BYTES_LIMIT", defaultDailyIndexBytes)
	initModelConfigKey()
	debugCapturePaths = parsePathSet(getEnv("DEBUG_CAPTURE_PATHS", ""))
	debugCaptureMaxBytes = getEnvInt("DEBUG_CAPTURE_MAX_BYTES", 4096)
}

func parsePathSet(value string) map[string]bool {
	out := make(map[string]bool)
	for _, raw := range strings.Split(value, ",") {
		path := strings.TrimSpace(raw)
		if path == "" {
			continue
		}
		if path != "*" && !strings.HasPrefix(path, "/") {
			path = "/" + path
		}
		out[path] = true
	}
	return out
}

func shouldDebugCapture(path string) bool {
	return debugCapturePaths != nil && (debugCapturePaths["*"] || debugCapturePaths[path])
}

func previewBytesForLog(data []byte, maxBytes int) string {
	limit := maxBytes
	if limit <= 0 {
		limit = 4096
	}
	if len(data) < limit {
		limit = len(data)
	}
	preview := string(data[:limit])
	if len(data) > limit {
		preview += fmt.Sprintf("...[truncated %d bytes]", len(data)-limit)
	}
	return strconv.Quote(preview)
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}

// getEnvInt64 用于字节数这类可能超出 32 位 int 的配置：Atoi 在 32 位平台上
// 解析 2GiB 会失败并静默退回默认值，那种降级在配额上是危险的。
func getEnvInt64(key string, defaultValue int64) int64 {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
			return intVal
		}
		log.Printf("[CONFIG] invalid %s=%q, falling back to %d", key, value, defaultValue)
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}

// ── MCP 客户端 ────────────────────────────────────────────────────────────

type mcpClient struct {
	mu        sync.RWMutex
	sessionID string
	nextID    atomic.Int64
	http      *http.Client
}

var lce = &mcpClient{
	http: &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        50,
			MaxIdleConnsPerHost: 50,
			IdleConnTimeout:     90 * time.Second,
		},
	},
}

const (
	defaultMCPCallTimeout     = 120 * time.Second
	remoteIndexMCPCallTimeout = 330 * time.Second
)

func (m *mcpClient) ensureSession(ctx context.Context) (string, error) {
	m.mu.RLock()
	sid := m.sessionID
	m.mu.RUnlock()
	if sid != "" {
		return sid, nil
	}
	return m.initSession(ctx)
}

// initSessionTimeout 限制 initSession 持写锁期间两次网络调用的总时长。
// 这里所有并发请求的 ensureSession 都会阻塞在写锁上，不能让调用方自带的
// 120s 超时决定锁的持有时间。
const initSessionTimeout = 10 * time.Second

func (m *mcpClient) initSession(ctx context.Context) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sessionID != "" {
		return m.sessionID, nil
	}

	ctx, cancel := context.WithTimeout(ctx, initSessionTimeout)
	defer cancel()

	initBody, _ := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      m.nextID.Add(1),
		"method":  "initialize",
		"params": map[string]interface{}{
			"protocolVersion": "2025-03-26",
			"capabilities":    map[string]interface{}{},
			"clientInfo": map[string]interface{}{
				"name":    "acemcp-relay",
				"version": "1.0.0",
			},
		},
	})

	req, _ := http.NewRequestWithContext(ctx, "POST", lceMCPURL, bytes.NewReader(initBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")

	resp, err := m.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("MCP initialize: %w", err)
	}
	io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("MCP initialize returned %d", resp.StatusCode)
	}

	sid := resp.Header.Get("Mcp-Session-Id")
	if sid == "" {
		return "", fmt.Errorf("MCP initialize: missing session ID header")
	}

	notifBody, _ := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "notifications/initialized",
	})
	nReq, _ := http.NewRequestWithContext(ctx, "POST", lceMCPURL, bytes.NewReader(notifBody))
	nReq.Header.Set("Content-Type", "application/json")
	nReq.Header.Set("Mcp-Session-Id", sid)
	nResp, err := m.http.Do(nReq)
	if err != nil {
		// LCE 侧的 session 已经建立：通知失败时必须归还名额，否则每次
		// 失败重试都会泄漏一个 LCE session，最终把自己挡在 503 外面。
		go m.deleteRemoteSession(sid)
		return "", fmt.Errorf("MCP initialized notification: %w", err)
	}
	io.ReadAll(nResp.Body)
	nResp.Body.Close()

	m.sessionID = sid
	log.Printf("[MCP] Session initialized: %s", sid)
	return sid, nil
}

func (m *mcpClient) invalidateSession() {
	m.mu.Lock()
	sid := m.sessionID
	m.sessionID = ""
	m.mu.Unlock()
	if sid != "" {
		go m.deleteRemoteSession(sid)
	}
}

// deleteRemoteSession best-effort 释放 LCE 侧的 session 名额（LCE 的 /mcp 支持
// DELETE + Mcp-Session-Id）。失败只打日志：名额最终由 LCE 的 TTL 回收。
func (m *mcpClient) deleteRemoteSession(sid string) {
	if sid == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), initSessionTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, lceMCPURL, nil)
	if err != nil {
		return
	}
	req.Header.Set("Mcp-Session-Id", sid)
	resp, err := m.http.Do(req)
	if err != nil {
		log.Printf("[MCP] session delete failed (sid=%s): %v", sid, err)
		return
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

type mcpToolResult struct {
	Content []byte
	IsError bool
}

func (m *mcpClient) callTool(ctx context.Context, name string, args map[string]interface{}) (*mcpToolResult, error) {
	return m.callToolWithTimeout(ctx, name, args, defaultMCPCallTimeout)
}

func (m *mcpClient) callToolWithTimeout(ctx context.Context, name string, args map[string]interface{}, timeout time.Duration) (*mcpToolResult, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	sid, err := m.ensureSession(ctx)
	if err != nil {
		return nil, err
	}

	result, retryable, err := m.doCallTool(ctx, sid, name, args)
	if err != nil && retryable {
		m.invalidateSession()
		sid, err = m.initSession(ctx)
		if err != nil {
			return nil, err
		}
		result, _, err = m.doCallTool(ctx, sid, name, args)
	}
	return result, err
}

func (m *mcpClient) doCallTool(ctx context.Context, sid, name string, args map[string]interface{}) (*mcpToolResult, bool, error) {
	body, _ := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      m.nextID.Add(1),
		"method":  "tools/call",
		"params": map[string]interface{}{
			"name":      name,
			"arguments": args,
		},
	})

	req, _ := http.NewRequestWithContext(ctx, "POST", lceMCPURL, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Mcp-Session-Id", sid)
	// 断言从本次调用的 args 派生，而不是由各调用点自己传：签发的租户与请求的租户
	// 因此永远一致，也不存在"某个调用点忘了带"的情况。
	if tenantID := tenantIDFromArgs(args); tenantID != "" {
		header, headerErr := tenantAssertions.authorizationHeader(tenantID)
		if headerErr != nil {
			return nil, false, fmt.Errorf("MCP tenant assertion: %w", headerErr)
		}
		if header != "" {
			req.Header.Set("Authorization", header)
		}
	}

	resp, err := m.http.Do(req)
	if err != nil {
		return nil, false, fmt.Errorf("MCP tools/call: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, false, fmt.Errorf("MCP read response: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, true, fmt.Errorf("MCP session expired")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, false, fmt.Errorf("MCP tools/call returned %d: %s", resp.StatusCode, string(respBody))
	}

	if ct := resp.Header.Get("Content-Type"); strings.HasPrefix(ct, "text/event-stream") {
		respBody = extractSSEData(respBody)
	}

	var rpcResp struct {
		Result *struct {
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
			IsError bool `json:"isError"`
		} `json:"result"`
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.Unmarshal(respBody, &rpcResp); err != nil {
		return nil, false, fmt.Errorf("MCP parse response: %w", err)
	}

	if rpcResp.Error != nil {
		return nil, false, fmt.Errorf("MCP error [%d]: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	if rpcResp.Result == nil || len(rpcResp.Result.Content) == 0 {
		return nil, false, fmt.Errorf("MCP tool returned empty content")
	}

	return &mcpToolResult{
		Content: []byte(rpcResp.Result.Content[0].Text),
		IsError: rpcResp.Result.IsError,
	}, false, nil
}

func (m *mcpClient) toolsList(ctx context.Context) error {
	sid, err := m.ensureSession(ctx)
	if err != nil {
		return err
	}

	body, _ := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      m.nextID.Add(1),
		"method":  "tools/list",
		"params":  map[string]interface{}{},
	})

	req, _ := http.NewRequestWithContext(ctx, "POST", lceMCPURL, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Mcp-Session-Id", sid)

	resp, err := m.http.Do(req)
	if err != nil {
		return fmt.Errorf("MCP tools/list: %w", err)
	}
	io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		m.invalidateSession()
		return fmt.Errorf("MCP session expired")
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("MCP tools/list returned %d", resp.StatusCode)
	}
	return nil
}

func (m *mcpClient) fetchToolsList(ctx context.Context) (json.RawMessage, error) {
	sid, err := m.ensureSession(ctx)
	if err != nil {
		return nil, err
	}

	body, _ := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      m.nextID.Add(1),
		"method":  "tools/list",
		"params":  map[string]interface{}{},
	})

	req, _ := http.NewRequestWithContext(ctx, "POST", lceMCPURL, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Mcp-Session-Id", sid)

	resp, err := m.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("MCP tools/list: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("MCP read response: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		m.invalidateSession()
		return nil, fmt.Errorf("MCP session expired")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("MCP tools/list returned %d", resp.StatusCode)
	}

	if ct := resp.Header.Get("Content-Type"); strings.HasPrefix(ct, "text/event-stream") {
		respBody = extractSSEData(respBody)
	}

	var rpcResp struct {
		Result struct {
			Tools json.RawMessage `json:"tools"`
		} `json:"result"`
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(respBody, &rpcResp); err != nil {
		return nil, fmt.Errorf("MCP parse tools/list: %w", err)
	}
	if rpcResp.Error != nil {
		return nil, fmt.Errorf("MCP tools/list error [%d]: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	return rpcResp.Result.Tools, nil
}

// ── 数据库 & Redis ────────────────────────────────────────────────────────

var db *sql.DB
var redisClient *redis.Client

func initDB() error {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName)

	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		return err
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)

	if err = db.Ping(); err != nil {
		return err
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS request_logs (
			id UUID PRIMARY KEY,
			user_id VARCHAR(255) NOT NULL,
			status VARCHAR(20) NOT NULL DEFAULT 'pending',
			status_code INTEGER,
			request_path VARCHAR(512) NOT NULL,
			request_method VARCHAR(10) NOT NULL,
			request_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			response_duration_ms BIGINT,
			client_ip VARCHAR(45) NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		);
		DROP INDEX IF EXISTS idx_request_logs_user_id;
		CREATE INDEX IF NOT EXISTS idx_request_logs_user_id_timestamp ON request_logs(user_id, request_timestamp DESC);
		CREATE INDEX IF NOT EXISTS idx_request_logs_timestamp ON request_logs(request_timestamp);
		CREATE INDEX IF NOT EXISTS idx_request_logs_status ON request_logs(status);
	`)
	if err != nil {
		return fmt.Errorf("failed to migrate request_logs table: %w", err)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS leaderboard (
			id VARCHAR(32) PRIMARY KEY,
			date_str VARCHAR(10) NOT NULL,
			rank INTEGER NOT NULL,
			user_id VARCHAR(255) NOT NULL,
			request_count BIGINT NOT NULL,
			updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		);
		CREATE INDEX IF NOT EXISTS idx_leaderboard_date ON leaderboard(date_str);

		-- 旧谓词指向早已不存在的 '/agents/codebase-retrieval'，排行榜查询只能全表扫描。
		-- 谓词必须与 updateLeaderboard 的双路径口径保持一致。
		DROP INDEX IF EXISTS idx_request_logs_codebase_retrieval;
		CREATE INDEX IF NOT EXISTS idx_request_logs_codebase_retrieval_v2
			ON request_logs(user_id, request_timestamp)
			WHERE request_path IN ('/mcp/tools/call/codebase-retrieval', '/relay/agents/codebase-retrieval');
	`)
	if err != nil {
		return fmt.Errorf("failed to migrate leaderboard table: %w", err)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS error_details (
			id SERIAL PRIMARY KEY,
			request_id UUID NOT NULL REFERENCES request_logs(id),
			source VARCHAR(20) NOT NULL DEFAULT 'proxy',
			error TEXT NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		);
		CREATE INDEX IF NOT EXISTS idx_error_details_request_id ON error_details(request_id);
	`)
	if err != nil {
		return fmt.Errorf("failed to migrate error_details table: %w", err)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS health_checks (
			id SERIAL PRIMARY KEY,
			status VARCHAR(20) NOT NULL,
			tcp_ping_ms INTEGER,
			codebase_retrieval_ms INTEGER,
			error_message TEXT,
			created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			next_check_at TIMESTAMP WITH TIME ZONE
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to migrate health_checks table: %w", err)
	}

	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_health_checks_created_at ON health_checks(created_at)`)
	if err != nil {
		return fmt.Errorf("failed to create health_checks index: %w", err)
	}

	if err := migrateDeviceTables(); err != nil {
		return err
	}

	if err := migrateQuotaTables(); err != nil {
		return err
	}

	if err := migrateModelConfigTables(); err != nil {
		return err
	}

	return migrateIndexingTables()
}

func initRedis() error {
	redisClient = redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", redisHost, redisPort),
		DB:   0,
	})
	_, err := redisClient.Ping(context.Background()).Result()
	if err != nil {
		return fmt.Errorf("failed to connect to Redis: %w", err)
	}
	return nil
}

// ── 认证 ──────────────────────────────────────────────────────────────────

func authenticateRequest(c *gin.Context) (string, bool) {
	authHeader := c.GetHeader("Authorization")
	if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
		return "", false
	}

	token := strings.TrimPrefix(authHeader, "Bearer ")
	hash := md5.Sum([]byte(token))
	tokenMD5 := hex.EncodeToString(hash[:])
	var userID string
	err := db.QueryRow("SELECT user_id FROM api_keys WHERE id = $1", tokenMD5).Scan(&userID)
	if err != nil {
		return "", false
	}

	return userID, true
}

func authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		startTime := time.Now()
		c.Set(ContextKeyStartTime, startTime)

		userID, ok := authenticateRequest(c)
		if !ok {
			authHeader := c.GetHeader("Authorization")
			if authHeader == "" {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing authorization header"})
			} else {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
			}
			return
		}

		c.Set(ContextKeyUserID, userID)

		if isUserBanned(userID) {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "account banned; contact the administrator"})
			return
		}

		trustedConsole := isTrustedConsoleRequest(c)
		if !trustedConsole {
			// 设备校验在前：被 enforce 拒掉的请求不应烧掉当日请求配额。
			deviceID, deviceOK := checkDeviceBinding(c, userID)
			if !deviceOK {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "device not authorized: this account is signed in on another device; log in again to use it here"})
				return
			}

			if !isIndexQuotaExempt(c.Request.Method, c.Request.URL.Path) {
				if ok, limit := checkRequestQuota(userID); !ok {
					c.Header("Retry-After", quotaRetryAfterHeader(time.Now()))
					c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": fmt.Sprintf("daily request quota exceeded (%d/day)", limit)})
					return
				}
			}

			if deviceID != "" {
				go recordDeviceActivity(userID, deviceID, c.ClientIP())
			}
		}

		if trustedConsole {
			c.Next()
			return
		}

		logID := uuid.New().String()
		c.Set(ContextKeyLogID, logID)

		insertDone := make(chan struct{})
		c.Set(ContextKeyInsertDone, insertDone)

		path := c.Request.URL.Path
		method := c.Request.Method
		clientIP := c.ClientIP()
		go func() {
			defer close(insertDone)
			_, err := db.Exec(`
				INSERT INTO request_logs (id, user_id, status, request_path, request_method, request_timestamp, client_ip)
				VALUES ($1, $2, $3, $4, $5, $6, $7)
			`, logID, userID, StatusPending, path, method, startTime, clientIP)
			if err != nil {
				log.Printf("[ERROR] Failed to insert request log: %v", err)
			}
		}()

		c.Next()
	}
}

func isIndexControlPath(requestPath string) bool {
	return requestPath == "/relay/remote-index" ||
		requestPath == "/relay/index-jobs" ||
		strings.HasPrefix(requestPath, "/relay/index-jobs/")
}

// isIndexQuotaExempt 让一次索引任务只在创建时计 1 次配额：轮询、batch 上传和
// 完成/失败上报都是同一次扫描的内部步骤，不重复计费；但创建不能豁免，否则
// 索引通道可以零成本无限驱动 LCE embedding。
func isIndexQuotaExempt(method, requestPath string) bool {
	if !isIndexControlPath(requestPath) {
		return false
	}
	return !(method == http.MethodPost && requestPath == "/relay/index-jobs")
}

// ── 请求日志 ──────────────────────────────────────────────────────────────

type RequestLogEntry struct {
	LogID            string
	StatusCode       int
	ResponseDuration time.Duration
	InsertDone       <-chan struct{}
}

func completeRequestLogAsync(entry RequestLogEntry) {
	go func() {
		if entry.LogID == "" {
			return
		}
		if entry.InsertDone != nil {
			<-entry.InsertDone
		}
		durationMs := entry.ResponseDuration.Milliseconds()

		result, err := db.Exec(`
			UPDATE request_logs
			SET status = $1, status_code = $2, response_duration_ms = $3, updated_at = NOW()
			WHERE id = $4
		`, StatusCompleted, entry.StatusCode, durationMs, entry.LogID)

		if err != nil {
			log.Printf("[ERROR] Failed to update request log: %v", err)
		} else if rows, _ := result.RowsAffected(); rows == 0 {
			log.Printf("[WARN] Update request log affected 0 rows (id=%s)", entry.LogID)
		}
	}()
}

func saveErrorDetailsAsync(logID string, source string, errorMsg string, insertDone <-chan struct{}) {
	if logID == "" || errorMsg == "" {
		return
	}
	go func() {
		if insertDone != nil {
			<-insertDone
		}
		_, err := db.Exec(`
			INSERT INTO error_details (request_id, source, error)
			VALUES ($1, $2, $3)
		`, logID, source, errorMsg)
		if err != nil {
			log.Printf("[ERROR] Failed to save error details: %v", err)
		}
	}()
}

func getInsertDone(c *gin.Context) <-chan struct{} {
	if v, ok := c.Get(ContextKeyInsertDone); ok {
		if ch, ok := v.(chan struct{}); ok {
			return ch
		}
	}
	return nil
}

func getRequestLogEntry(c *gin.Context, statusCode int) RequestLogEntry {
	startTime, _ := c.Get(ContextKeyStartTime)
	logID, _ := c.Get(ContextKeyLogID)

	startTimeVal, ok := startTime.(time.Time)
	if !ok {
		startTimeVal = time.Now()
	}

	logIDVal, _ := logID.(string)

	return RequestLogEntry{
		LogID:            logIDVal,
		StatusCode:       statusCode,
		ResponseDuration: time.Since(startTimeVal),
		InsertDone:       getInsertDone(c),
	}
}

func updateRequestPathAsync(logID string, newPath string, insertDone <-chan struct{}) {
	if logID == "" {
		return
	}
	go func() {
		if insertDone != nil {
			<-insertDone
		}
		_, err := db.Exec(`UPDATE request_logs SET request_path = $1, updated_at = NOW() WHERE id = $2`, newPath, logID)
		if err != nil {
			log.Printf("[ERROR] Failed to update request path: %v", err)
		}
	}()
}

// ── MCP 服务端 ──────────────────────────────────────────────────────────────

const (
	mcpProtocolVersion      = "2025-03-26"
	mcpRelayName            = "lce-relay"
	mcpRelayVersion         = "1.0.0"
	mcpSessionTTL           = 30 * time.Minute
	mcpSessionSweepInterval = 60 * time.Second
	mcpMaxSessions          = 1000
	mcpMaxSessionsPerUser   = 16
	toolsCacheTTL           = 5 * time.Minute
)

var chatMCPDeniedTools = map[string]struct{}{
	"codebase_clear_index":    {},
	"codebase_remote_index":   {},
	"codebase_git_context":    {},
	"codebase_review_changes": {},
	"codebase_find_missing":   {},
}

func isChatMCPToolAllowed(name string) bool {
	_, denied := chatMCPDeniedTools[strings.TrimSpace(name)]
	return !denied
}

var chatMCPSchemaRewrites = map[string]map[string]struct{}{
	"codebase-retrieval": {
		"repo_path":             {},
		"workspace_config_path": {},
		"tenant_id":             {},
		"include_worktree":      {},
		"freshness_policy":      {},
		"shared_index_path":     {},
		"connector_configs":     {},
		"live_context":          {},
		"profile":               {},
		"bundle_budget":         {},
		"workspace_limits":      {},
	},
	"codebase_symbol_graph": {
		"repo_path": {},
		"tenant_id": {},
	},
	"codebase_tenant_stats": {
		"tenant_id": {},
	},
}

func rewriteToolSchema(toolJSON json.RawMessage) (json.RawMessage, error) {
	var tool map[string]interface{}
	if err := json.Unmarshal(toolJSON, &tool); err != nil {
		return toolJSON, nil
	}

	name, _ := tool["name"].(string)
	fieldsToRemove, needsRewrite := chatMCPSchemaRewrites[name]
	if !needsRewrite {
		return toolJSON, nil
	}

	schemaRaw, ok := tool["inputSchema"]
	if !ok {
		return toolJSON, nil
	}
	schema, ok := schemaRaw.(map[string]interface{})
	if !ok {
		return toolJSON, nil
	}

	if props, ok := schema["properties"].(map[string]interface{}); ok {
		for field := range fieldsToRemove {
			delete(props, field)
		}
	}

	if reqRaw, ok := schema["required"].([]interface{}); ok {
		cleaned := make([]interface{}, 0, len(reqRaw))
		for _, r := range reqRaw {
			if s, ok := r.(string); ok {
				if _, remove := fieldsToRemove[s]; remove {
					continue
				}
			}
			cleaned = append(cleaned, r)
		}
		schema["required"] = cleaned
	}

	delete(schema, "oneOf")

	return json.Marshal(tool)
}

func sanitizeToolCallArgs(toolName string, args map[string]interface{}) {
	for field := range chatMCPSchemaRewrites[toolName] {
		delete(args, field)
	}
	delete(args, "model_config")
}

func filterChatMCPTools(raw json.RawMessage) (json.RawMessage, error) {
	var tools []json.RawMessage
	if err := json.Unmarshal(raw, &tools); err != nil {
		return nil, fmt.Errorf("parse MCP tools: %w", err)
	}
	filtered := make([]json.RawMessage, 0, len(tools))
	for _, tool := range tools {
		var metadata struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(tool, &metadata); err != nil {
			return nil, fmt.Errorf("parse MCP tool metadata: %w", err)
		}
		if !isChatMCPToolAllowed(metadata.Name) {
			continue
		}
		rewritten, err := rewriteToolSchema(tool)
		if err != nil {
			return nil, fmt.Errorf("rewrite MCP tool schema %q: %w", metadata.Name, err)
		}
		filtered = append(filtered, rewritten)
	}
	return json.Marshal(filtered)
}

type mcpServerSession struct {
	userID       string
	lastActivity time.Time
}

var (
	serverSessions   = make(map[string]*mcpServerSession)
	serverSessionsMu sync.RWMutex

	toolsCache     json.RawMessage
	toolsCacheMu   sync.RWMutex
	toolsCacheTime time.Time
	// toolsFetchMu 只保护"缓存过期后的回源"这一段：双检锁防止缓存击穿——
	// 过期瞬间的并发请求只放一个去打 LCE，其余等它填好缓存后直接复用。
	toolsFetchMu sync.Mutex
)

type jsonRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type jsonRPCResultResp struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  interface{}     `json:"result"`
}

type jsonRPCErrorBody struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type jsonRPCErrorResp struct {
	JSONRPC string           `json:"jsonrpc"`
	ID      json.RawMessage  `json:"id"`
	Error   jsonRPCErrorBody `json:"error"`
}

func rpcResult(id json.RawMessage, result interface{}) jsonRPCResultResp {
	return jsonRPCResultResp{JSONRPC: "2.0", ID: id, Result: result}
}

func rpcError(id json.RawMessage, code int, message string) jsonRPCErrorResp {
	if id == nil {
		id = json.RawMessage("null")
	}
	return jsonRPCErrorResp{
		JSONRPC: "2.0",
		ID:      id,
		Error:   jsonRPCErrorBody{Code: code, Message: message},
	}
}

func pruneExpiredMCPSessions(sessions map[string]*mcpServerSession, now time.Time, ttl time.Duration) []string {
	expired := make([]string, 0)
	for id, session := range sessions {
		if now.Sub(session.lastActivity) > ttl {
			delete(sessions, id)
			expired = append(expired, id)
		}
	}
	return expired
}

func evictOldestMCPSession(sessions map[string]*mcpServerSession, userID string) (string, bool) {
	oldestID := ""
	var oldestActivity time.Time
	for id, session := range sessions {
		if userID != "" && session.userID != userID {
			continue
		}
		if oldestID == "" || session.lastActivity.Before(oldestActivity) ||
			(session.lastActivity.Equal(oldestActivity) && id < oldestID) {
			oldestID = id
			oldestActivity = session.lastActivity
		}
	}
	if oldestID == "" {
		return "", false
	}
	delete(sessions, oldestID)
	return oldestID, true
}

func countMCPSessionsForUser(sessions map[string]*mcpServerSession, userID string) int {
	count := 0
	for _, session := range sessions {
		if session.userID == userID {
			count++
		}
	}
	return count
}

func prepareMCPSessionSlot(
	sessions map[string]*mcpServerSession,
	userID string,
	now time.Time,
	ttl time.Duration,
	perUserLimit int,
	globalLimit int,
) (expired []string, evicted []string) {
	expired = pruneExpiredMCPSessions(sessions, now, ttl)
	for perUserLimit > 0 && countMCPSessionsForUser(sessions, userID) >= perUserLimit {
		id, ok := evictOldestMCPSession(sessions, userID)
		if !ok {
			break
		}
		evicted = append(evicted, id)
	}
	for globalLimit > 0 && len(sessions) >= globalLimit {
		id, ok := evictOldestMCPSession(sessions, "")
		if !ok {
			break
		}
		evicted = append(evicted, id)
	}
	return expired, evicted
}

func touchMCPSession(
	sessions map[string]*mcpServerSession,
	sessionID string,
	userID string,
	now time.Time,
) bool {
	session, ok := sessions[sessionID]
	if !ok || session.userID != userID {
		return false
	}
	session.lastActivity = now
	return true
}

func sweepExpiredMCPSessions() {
	serverSessionsMu.Lock()
	defer serverSessionsMu.Unlock()
	for _, id := range pruneExpiredMCPSessions(serverSessions, time.Now(), mcpSessionTTL) {
		log.Printf("[MCP_SERVER] Session expired: %s", id)
	}
}

func startMCPSessionSweeper(ctx context.Context) {
	ticker := time.NewTicker(mcpSessionSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweepExpiredMCPSessions()
		}
	}
}

func readToolsCache() (json.RawMessage, bool) {
	toolsCacheMu.RLock()
	defer toolsCacheMu.RUnlock()
	if toolsCache != nil && time.Since(toolsCacheTime) < toolsCacheTTL {
		return toolsCache, true
	}
	return nil, false
}

func getCachedToolsList(ctx context.Context) (json.RawMessage, error) {
	if cached, ok := readToolsCache(); ok {
		return cached, nil
	}

	toolsFetchMu.Lock()
	defer toolsFetchMu.Unlock()
	// 双检：拿到回源锁时前一个请求可能已经填好缓存。
	if cached, ok := readToolsCache(); ok {
		return cached, nil
	}

	tools, err := lce.fetchToolsList(ctx)
	if err != nil {
		return nil, err
	}
	tools, err = filterChatMCPTools(tools)
	if err != nil {
		return nil, err
	}

	toolsCacheMu.Lock()
	toolsCache = tools
	toolsCacheTime = time.Now()
	toolsCacheMu.Unlock()

	return tools, nil
}

func handleMCPPost(c *gin.Context) {
	userID := c.GetString(ContextKeyUserID)

	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, rpcError(nil, -32700, "failed to read request body"))
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusBadRequest))
		return
	}

	if shouldDebugCapture("/mcp") {
		logIDStr, _ := c.Get(ContextKeyLogID)
		log.Printf("[DEBUG_CAPTURE] mcp request id=%s bytes=%d body=%s",
			logIDStr, len(body), previewBytesForLog(body, debugCaptureMaxBytes))
	}

	var rpc jsonRPCRequest
	if err := json.Unmarshal(body, &rpc); err != nil {
		c.JSON(http.StatusBadRequest, rpcError(nil, -32700, "Parse error"))
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusBadRequest))
		return
	}

	sessionID := c.GetHeader("Mcp-Session-Id")

	if rpc.Method == "initialize" {
		if sessionID != "" {
			c.JSON(http.StatusBadRequest, rpcError(rpc.ID, -32600, "initialize must not include Mcp-Session-Id"))
			completeRequestLogAsync(getRequestLogEntry(c, http.StatusBadRequest))
			return
		}

		serverSessionsMu.Lock()
		now := time.Now()
		expired, evicted := prepareMCPSessionSlot(
			serverSessions,
			userID,
			now,
			mcpSessionTTL,
			mcpMaxSessionsPerUser,
			mcpMaxSessions,
		)
		for _, id := range expired {
			log.Printf("[MCP_SERVER] Session expired during initialize: %s", id)
		}
		for _, id := range evicted {
			log.Printf("[MCP_SERVER] Session evicted during initialize: %s", id)
		}
		newSID := uuid.New().String()
		serverSessions[newSID] = &mcpServerSession{
			userID:       userID,
			lastActivity: now,
		}
		serverSessionsMu.Unlock()

		c.Header("Mcp-Session-Id", newSID)
		c.JSON(http.StatusOK, rpcResult(rpc.ID, map[string]interface{}{
			"protocolVersion": mcpProtocolVersion,
			"capabilities":    map[string]interface{}{"tools": map[string]interface{}{}},
			"serverInfo":      map[string]interface{}{"name": mcpRelayName, "version": mcpRelayVersion},
		}))
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))
		log.Printf("[MCP_SERVER] Session created: %s for user %s", newSID, userID)
		return
	}

	if sessionID == "" {
		c.JSON(http.StatusBadRequest, rpcError(rpc.ID, -32000, "Missing Mcp-Session-Id header"))
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusBadRequest))
		return
	}

	serverSessionsMu.Lock()
	ok := touchMCPSession(serverSessions, sessionID, userID, time.Now())
	serverSessionsMu.Unlock()
	if !ok {
		c.JSON(http.StatusNotFound, rpcError(rpc.ID, -32000, "Invalid or expired session"))
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusNotFound))
		return
	}

	if rpc.Method == "notifications/initialized" {
		c.Status(http.StatusAccepted)
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusAccepted))
		return
	}

	switch rpc.Method {
	case "tools/list":
		tools, err := getCachedToolsList(c.Request.Context())
		if err != nil {
			logIDStr, _ := c.Get(ContextKeyLogID)
			logIDVal, _ := logIDStr.(string)
			saveErrorDetailsAsync(logIDVal, "lce", err.Error(), getInsertDone(c))
			c.JSON(http.StatusOK, rpcError(rpc.ID, -32000, "upstream error: "+err.Error()))
			completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))
			return
		}
		c.JSON(http.StatusOK, rpcResult(rpc.ID, map[string]interface{}{
			"tools": tools,
		}))
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))

	case "tools/call":
		handleMCPToolsCall(c, rpc.ID, rpc.Params, userID)

	default:
		c.JSON(http.StatusOK, rpcError(rpc.ID, -32601, "Method not found: "+rpc.Method))
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))
	}
}

func handleMCPToolsCall(c *gin.Context, id json.RawMessage, params json.RawMessage, userID string) {
	var p struct {
		Name      string                 `json:"name"`
		Arguments map[string]interface{} `json:"arguments"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		c.JSON(http.StatusOK, rpcError(id, -32602, "Invalid params"))
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))
		return
	}
	if !isChatMCPToolAllowed(p.Name) {
		c.JSON(http.StatusOK, rpcError(id, -32601, "Tool not available through chat MCP: "+p.Name))
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))
		return
	}

	logIDStr, _ := c.Get(ContextKeyLogID)
	logIDVal, _ := logIDStr.(string)
	toolPath := "/mcp/tools/call/" + p.Name
	updateRequestPathAsync(logIDVal, toolPath, getInsertDone(c))

	if p.Arguments == nil {
		p.Arguments = make(map[string]interface{})
	}
	sanitizeToolCallArgs(p.Name, p.Arguments)
	p.Arguments["tenant_id"] = userID
	modelLease, cfg, err := acquireModelConfigOperation(c.Request.Context(), userID, "chat-tool")
	if err != nil {
		c.JSON(http.StatusOK, rpcError(id, -32000, err.Error()))
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))
		return
	}
	defer modelLease.Release()
	if cfg != nil {
		p.Arguments["model_config"] = cfg
	}

	result, err := lce.callTool(modelLease.Context(), p.Name, p.Arguments)
	if err != nil {
		if errors.Is(c.Request.Context().Err(), context.Canceled) {
			completeRequestLogAsync(getRequestLogEntry(c, 499))
			return
		}
		saveErrorDetailsAsync(logIDVal, "lce", err.Error(), getInsertDone(c))
		c.JSON(http.StatusOK, rpcError(id, -32000, err.Error()))
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))
		return
	}

	if shouldDebugCapture("/mcp") {
		log.Printf("[DEBUG_CAPTURE] mcp response tool=%s bytes=%d body=%s",
			p.Name, len(result.Content), previewBytesForLog(result.Content, debugCaptureMaxBytes))
	}

	c.JSON(http.StatusOK, rpcResult(id, map[string]interface{}{
		"content": []map[string]interface{}{
			{"type": "text", "text": string(result.Content)},
		},
		"isError": result.IsError,
	}))
	completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))
}

// ── 清除索引（冷却 + 日志清理）──────────────────────────────────────────

const clearIndexCooldownSeconds = 72 * 60 * 60 // 72 hours

func clearIndexCooldownKey(userID string) string {
	return "clear_cooldown:" + userID
}

func checkClearIndexCooldown(ctx context.Context, userID string) error {
	if redisClient == nil {
		return nil
	}
	ttl, err := redisClient.TTL(ctx, clearIndexCooldownKey(userID)).Result()
	if err != nil || ttl <= 0 {
		return nil
	}
	hours := int(ttl.Hours())
	minutes := int(ttl.Minutes()) % 60
	return fmt.Errorf("清除索引冷却中，剩余 %d 小时 %d 分钟后可再次操作", hours, minutes)
}

func setClearIndexCooldown(ctx context.Context, userID string) {
	if redisClient == nil {
		return
	}
	redisClient.Set(ctx, clearIndexCooldownKey(userID), "1", time.Duration(clearIndexCooldownSeconds)*time.Second)
}

func deleteUserLogsAsync(userID string) {
	go func() {
		_, err := db.Exec(`DELETE FROM error_details WHERE request_id IN (SELECT id FROM request_logs WHERE user_id = $1)`, userID)
		if err != nil {
			log.Printf("[ERROR] Failed to delete error_details for user %s: %v", userID, err)
		}
		result, err := db.Exec(`DELETE FROM request_logs WHERE user_id = $1`, userID)
		if err != nil {
			log.Printf("[ERROR] Failed to delete request_logs for user %s: %v", userID, err)
		} else if rows, _ := result.RowsAffected(); rows > 0 {
			log.Printf("[CLEAR_INDEX] Deleted %d request logs for user %s", rows, userID)
		}
	}()
}

func handleCodebaseRetrieval(c *gin.Context) {
	userID, _ := c.Get(ContextKeyUserID)
	userIDStr, _ := userID.(string)
	if userIDStr == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusUnauthorized))
		return
	}

	var req struct {
		InformationRequest string      `json:"information_request"`
		Blobs              interface{} `json:"blobs"`
		MaxOutputLength    int         `json:"max_output_length"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusBadRequest))
		return
	}

	if req.MaxOutputLength <= 0 {
		req.MaxOutputLength = 20000
	}

	// 字段名必须与 LCE 的 codebase-retrieval schema 一致：它必填 information_request
	// 且是 strict 的，发 "query" 会被两头拒——缺必填字段 + 未知键。
	args := map[string]interface{}{
		"tenant_id":           userIDStr,
		"information_request": req.InformationRequest,
	}
	modelLease, cfg, err := acquireModelConfigOperation(c.Request.Context(), userIDStr, "retrieval")
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusServiceUnavailable))
		return
	}
	defer modelLease.Release()
	if cfg != nil {
		args["model_config"] = cfg
	}
	result, err := lce.callTool(modelLease.Context(), "codebase-retrieval", args)
	if err != nil {
		logIDStr, _ := c.Get(ContextKeyLogID)
		logIDVal, _ := logIDStr.(string)
		saveErrorDetailsAsync(logIDVal, "lce", err.Error(), getInsertDone(c))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusInternalServerError))
		return
	}

	if result.IsError {
		c.JSON(http.StatusInternalServerError, gin.H{"error": string(result.Content)})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusInternalServerError))
		return
	}

	content := string(result.Content)
	if len(content) > req.MaxOutputLength {
		content = truncateUTF8(content, req.MaxOutputLength)
	}

	c.JSON(http.StatusOK, gin.H{"formatted_retrieval": content})
	completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))
}

func handleClearIndex(c *gin.Context) {
	userID, _ := c.Get(ContextKeyUserID)
	userIDStr, _ := userID.(string)
	if userIDStr == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusUnauthorized))
		return
	}

	if err := checkClearIndexCooldown(c.Request.Context(), userIDStr); err != nil {
		c.JSON(http.StatusTooManyRequests, gin.H{"error": err.Error()})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusTooManyRequests))
		return
	}
	lease, err := acquireExclusiveIndexOperation(c.Request.Context(), userIDStr, "clear-index")
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "索引正在执行其他操作: " + err.Error()})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusServiceUnavailable))
		return
	}
	defer lease.Release()
	opCtx := lease.Context()

	// 顺序必须是"先清 relay 快照并提交，再清 LCE"：LCE 的 clear 不可回滚，
	// 若先清 LCE 再提交 relay 事务，Commit 失败会留下"LCE 空 / relay 快照满"
	// 的永久不一致（后续 diff 全判未变更，永远不重传）。反过来，relay 已清、
	// LCE 清失败只会导致下一次全量重传覆盖旧数据，可自愈。
	// 事务在调 LCE 之前就已提交并释放连接与 advisory 锁，不会占着连接池等
	// 长时间网络调用。
	if err := clearUserIndexState(opCtx, userIDStr); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "清除 Relay 索引状态失败: " + err.Error()})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusInternalServerError))
		return
	}

	args := map[string]interface{}{"tenant_id": userIDStr}
	result, err := lce.callToolWithTimeout(opCtx, "codebase_clear_index", args, remoteIndexMCPCallTimeout)
	if err != nil {
		logIDStr, _ := c.Get(ContextKeyLogID)
		logIDVal, _ := logIDStr.(string)
		saveErrorDetailsAsync(logIDVal, "lce", err.Error(), getInsertDone(c))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "清除索引失败: " + err.Error()})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusInternalServerError))
		return
	}

	if result.IsError {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "清除索引失败", "detail": string(result.Content)})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusInternalServerError))
		return
	}

	setClearIndexCooldown(c.Request.Context(), userIDStr)
	deleteUserLogsAsync(userIDStr)

	c.JSON(http.StatusOK, gin.H{"success": true, "message": "索引和日志已清除"})
	completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))
}

func handleTenantStats(c *gin.Context) {
	userID, _ := c.Get(ContextKeyUserID)
	userIDStr, _ := userID.(string)
	if userIDStr == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusUnauthorized))
		return
	}

	args := map[string]interface{}{"tenant_id": userIDStr}
	result, err := lce.callTool(c.Request.Context(), "codebase_tenant_stats", args)
	if err != nil {
		logIDStr, _ := c.Get(ContextKeyLogID)
		logIDVal, _ := logIDStr.(string)
		saveErrorDetailsAsync(logIDVal, "lce", err.Error(), getInsertDone(c))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "获取统计失败: " + err.Error()})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusInternalServerError))
		return
	}

	if result.IsError {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "获取统计失败", "detail": string(result.Content)})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusInternalServerError))
		return
	}

	var stats map[string]interface{}
	if err := json.Unmarshal(result.Content, &stats); err != nil {
		c.JSON(http.StatusOK, gin.H{"raw": string(result.Content)})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))
		return
	}

	// 前端把这个字段展示为「索引请求次数」（历史累计完成数），所以这里查的是
	// completed 状态；indexingCount 这个字段名是对外契约，不能改。
	var completedJobCount int
	row := db.QueryRow(
		`SELECT COUNT(*) FROM index_jobs WHERE user_id = $1 AND status = $2`,
		userIDStr,
		indexJobStatusCompleted,
	)
	if err := row.Scan(&completedJobCount); err != nil {
		log.Printf("[STATS] completed index job count failed (user=%s): %v", userIDStr, err)
	}
	stats["indexingCount"] = completedJobCount

	c.JSON(http.StatusOK, stats)
	completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))
}

func handleMCPDelete(c *gin.Context) {
	userID := c.GetString(ContextKeyUserID)
	sessionID := c.GetHeader("Mcp-Session-Id")
	if sessionID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing Mcp-Session-Id"})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusBadRequest))
		return
	}

	serverSessionsMu.Lock()
	session, existed := serverSessions[sessionID]
	if existed && session.userID == userID {
		delete(serverSessions, sessionID)
	} else {
		existed = false
	}
	serverSessionsMu.Unlock()

	if !existed {
		c.Status(http.StatusNotFound)
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusNotFound))
		return
	}

	c.Status(http.StatusNoContent)
	completeRequestLogAsync(getRequestLogEntry(c, http.StatusNoContent))
	log.Printf("[MCP_SERVER] Session deleted: %s", sessionID)
}

// truncateUTF8 按字节上限截断字符串，并回退到最近的合法 UTF-8 边界，
// 避免把一个多字节字符从中间切开产生非法序列。
func truncateUTF8(s string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len(s) <= maxBytes {
		return s
	}
	cut := maxBytes
	for cut > 0 && cut > maxBytes-utf8.UTFMax && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

// extractSSEData 从 SSE 响应里取 JSON-RPC 载荷。流里可能有多个事件（如
// 服务器先推 notification 再推 response），取最后一个 data: 行；同时剥掉
// CRLF 换行残留的 \r，否则 JSON 解析会失败。
func extractSSEData(raw []byte) []byte {
	var last []byte
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimRight(line, "\r")
		if strings.HasPrefix(line, "data: ") {
			last = []byte(strings.TrimPrefix(line, "data: "))
		}
	}
	if last != nil {
		return last
	}
	return raw
}

// ── Response compression ────────────────────────────────────────────────────

const compressMinBytes = 1024
const brotliLevel = 4

var encodingPriority = map[string]int{
	"br":       4,
	"gzip":     3,
	"deflate":  2,
	"identity": 1,
}

func negotiateEncoding(acceptEncoding string) string {
	if acceptEncoding == "" {
		return "identity"
	}

	type candidate struct {
		encoding string
		quality  float64
		priority int
	}

	explicit := make(map[string]bool)
	var wildcardQuality float64 = -1

	var candidates []candidate
	for _, part := range strings.Split(acceptEncoding, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		encoding := part
		quality := 1.0

		if idx := strings.Index(part, ";"); idx != -1 {
			encoding = strings.TrimSpace(part[:idx])
			qPart := strings.TrimSpace(part[idx+1:])
			if strings.HasPrefix(qPart, "q=") {
				if q, err := strconv.ParseFloat(qPart[2:], 64); err == nil {
					quality = q
				}
			}
		}

		encoding = strings.ToLower(encoding)

		if encoding == "*" {
			wildcardQuality = quality
			continue
		}

		explicit[encoding] = true
		if quality == 0 {
			continue
		}

		if prio, ok := encodingPriority[encoding]; ok {
			candidates = append(candidates, candidate{encoding, quality, prio})
		}
	}

	if wildcardQuality > 0 {
		for enc, prio := range encodingPriority {
			if !explicit[enc] {
				candidates = append(candidates, candidate{enc, wildcardQuality, prio})
			}
		}
	}

	if len(candidates) == 0 {
		return "identity"
	}

	best := candidates[0]
	for _, c := range candidates[1:] {
		if c.quality > best.quality || (c.quality == best.quality && c.priority > best.priority) {
			best = c
		}
	}
	return best.encoding
}

func compressResponse(data []byte, encoding string) ([]byte, string) {
	if len(data) < compressMinBytes || encoding == "identity" {
		return data, "identity"
	}

	var buf bytes.Buffer
	var err error

	switch encoding {
	case "br":
		w := brotli.NewWriterLevel(&buf, brotliLevel)
		_, err = w.Write(data)
		if closeErr := w.Close(); err == nil {
			err = closeErr
		}
	case "gzip":
		w := gzip.NewWriter(&buf)
		_, err = w.Write(data)
		if closeErr := w.Close(); err == nil {
			err = closeErr
		}
	case "deflate":
		w, _ := flate.NewWriter(&buf, flate.DefaultCompression)
		_, err = w.Write(data)
		if closeErr := w.Close(); err == nil {
			err = closeErr
		}
	default:
		return data, "identity"
	}

	if err != nil {
		log.Printf("[COMPRESS] %s failed: %v, falling back to identity", encoding, err)
		return data, "identity"
	}
	return buf.Bytes(), encoding
}

// ── 排行榜 ────────────────────────────────────────────────────────────────

func updateLeaderboard() error {
	loc, err := time.LoadLocation(LeaderboardTimezone)
	if err != nil {
		return fmt.Errorf("failed to load timezone: %w", err)
	}

	now := time.Now().In(loc)
	dateStr := now.Format("2006-01-02")
	dayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc)
	dayEnd := dayStart.Add(24 * time.Hour)

	log.Printf("[LEADERBOARD] Updating leaderboard for %s", dateStr)

	rows, err := db.Query(`
		SELECT user_id, COUNT(*) as cnt
		FROM request_logs
		WHERE request_path IN ($1, $2)
		  AND request_timestamp >= $3
		  AND request_timestamp < $4
		  AND status_code = 200
		GROUP BY user_id
		ORDER BY cnt DESC
		LIMIT $5
	`, LeaderboardPath, LeaderboardRESTPath, dayStart, dayEnd, LeaderboardTopN)
	if err != nil {
		return fmt.Errorf("failed to query leaderboard data: %w", err)
	}
	defer rows.Close()

	type userCount struct {
		userID string
		count  int64
	}
	var results []userCount
	for rows.Next() {
		var uc userCount
		if err := rows.Scan(&uc.userID, &uc.count); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		results = append(results, uc)
	}

	if len(results) == 0 {
		log.Printf("[LEADERBOARD] No data for %s", dateStr)
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	datePrefix := now.Format("20060102")
	for rank, uc := range results {
		id := fmt.Sprintf("%s_%02d", datePrefix, rank+1)
		_, err := tx.Exec(`
			INSERT INTO leaderboard (id, date_str, rank, user_id, request_count, updated_at)
			VALUES ($1, $2, $3, $4, $5, NOW())
			ON CONFLICT (id) DO UPDATE SET
				user_id = EXCLUDED.user_id,
				request_count = EXCLUDED.request_count,
				updated_at = NOW()
		`, id, dateStr, rank+1, uc.userID, uc.count)
		if err != nil {
			return fmt.Errorf("failed to upsert leaderboard: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("[LEADERBOARD] Updated %d entries for %s", len(results), dateStr)
	return nil
}

func startLeaderboardScheduler(ctx context.Context) {
	ticker := time.NewTicker(LeaderboardUpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[LEADERBOARD] Scheduler stopped")
			return
		case <-ticker.C:
			if err := updateLeaderboard(); err != nil {
				log.Printf("[LEADERBOARD] Update failed: %v", err)
			}
		}
	}
}

// ── 健康检查 ──────────────────────────────────────────────────────────────

// probeLceLiveness 打 LCE 的存活端点并记录耗时。
// 该端点不建立 session、不占用 LCE 的请求并发额度，所以可以按探测周期反复调用；
// 用 initialize 当探针则会每次占掉一个 session 名额，最终把自己挡在 503 外面。
func probeLceLiveness(ctx context.Context, out *sql.NullInt64) error {
	t0 := time.Now()
	req, err := http.NewRequestWithContext(ctx, "GET", lceHealthURL, nil)
	if err != nil {
		return err
	}
	resp, err := lce.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	*out = sql.NullInt64{Int64: time.Since(t0).Milliseconds(), Valid: true}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("LCE liveness returned %d", resp.StatusCode)
	}
	return nil
}

func formatNullMs(value sql.NullInt64) string {
	if !value.Valid {
		return "n/a"
	}
	return fmt.Sprintf("%dms", value.Int64)
}

func runHealthProbe() {
	ctx, cancel := context.WithTimeout(context.Background(), HealthCheckTimeout)
	defer cancel()

	var livenessMs sql.NullInt64
	var lceLatencyMs sql.NullInt64
	var errMsg sql.NullString
	status := "success"

	defer func() {
		nextCheckAt := time.Now().Add(HealthCheckInterval)
		// 列名是历史遗留：tcp_ping_ms 现在写的是 HTTP 存活探针耗时，
		// codebase_retrieval_ms 写的是 tools/list 耗时。改列名需要迁移且
		// 前端面板按旧列名取数，这里只以注释说明语义。
		_, dbErr := db.Exec(
			`INSERT INTO health_checks (status, tcp_ping_ms, codebase_retrieval_ms, error_message, next_check_at)
			 VALUES ($1, $2, $3, $4, $5)`,
			status, livenessMs, lceLatencyMs, errMsg, nextCheckAt,
		)
		if dbErr != nil {
			log.Printf("[HEALTH] Failed to save result: %v", dbErr)
		}
	}()

	// 两段分开测：存活探针只说明进程和 HTTP 层活着，tools/list 才覆盖 MCP 分发、
	// session 管理和工具注册表。分开记录后，面板才能区分"LCE 挂了"与"LCE 活着但
	// MCP 层坏了"——过去两种情况都只显示一个笼统的 error，而 tcp_ping_ms 这一列
	// 一直写死为 NULL，前端那一栏永远是空的。
	livenessErr := probeLceLiveness(ctx, &livenessMs)

	t0 := time.Now()
	err := lce.toolsList(ctx)
	lceLatencyMs = sql.NullInt64{Int64: time.Since(t0).Milliseconds(), Valid: true}

	if err != nil {
		status = "error"
		if livenessErr == nil {
			errMsg = sql.NullString{String: "LCE process is up but MCP dispatch failed: " + err.Error(), Valid: true}
		} else {
			errMsg = sql.NullString{String: err.Error(), Valid: true}
		}
		return
	}

	log.Printf("[HEALTH] Probe OK: liveness=%s lce_latency=%dms", formatNullMs(livenessMs), lceLatencyMs.Int64)
}

func startHealthScheduler(ctx context.Context) {
	for {
		runHealthProbe()
		select {
		case <-ctx.Done():
			log.Println("[HEALTH] Scheduler stopped")
			return
		case <-time.After(HealthCheckInterval):
		}
	}
}

// ── main ──────────────────────────────────────────────────────────────────

func main() {
	loadConfig()

	logFile, err := os.OpenFile("gin.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		panic("无法创建日志文件: " + err.Error())
	}
	gin.DefaultWriter = io.MultiWriter(os.Stdout, logFile)
	gin.DefaultErrorWriter = io.MultiWriter(os.Stderr, logFile)
	log.SetOutput(io.MultiWriter(os.Stdout, logFile))

	if err := initDB(); err != nil {
		log.Fatalf("无法连接数据库: %v", err)
	}
	defer db.Close()

	if err := initRedis(); err != nil {
		log.Fatalf("无法连接 Redis: %v", err)
	}
	defer redisClient.Close()

	log.Printf("[MCP] LCE endpoint: %s", lceMCPURL)

	log.Println("[LEADERBOARD] Running initial statistics...")
	if err := updateLeaderboard(); err != nil {
		log.Printf("[LEADERBOARD] Initial update failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go startLeaderboardScheduler(ctx)

	log.Println("[HEALTH] Starting health scheduler...")
	go startHealthScheduler(ctx)

	go startMCPSessionSweeper(ctx)
	go startIndexJobSweeper(ctx)

	go func() {
		pprofMux := http.NewServeMux()
		pprofMux.HandleFunc("/debug/pprof/", pprof.Index)
		pprofMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		pprofMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		pprofMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		pprofMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		log.Println("[PPROF] Listening on 127.0.0.1:6060")
		if err := http.ListenAndServe("127.0.0.1:6060", pprofMux); err != nil {
			log.Printf("[PPROF] Server error: %v", err)
		}
	}()

	r := gin.Default()
	r.Use(authMiddleware())

	r.POST("/mcp", handleMCPPost)
	r.DELETE("/mcp", handleMCPDelete)
	// Streamable HTTP 规范：不提供服务端 SSE 推流时对 GET 回 405，
	// MCP SDK 客户端会按"无推流"优雅降级；回 404 会被当成连接错误。
	r.GET("/mcp", func(c *gin.Context) {
		c.Header("Allow", "POST, DELETE")
		c.JSON(http.StatusMethodNotAllowed, gin.H{"error": "SSE stream not supported; use POST"})
	})
	r.POST("/mcp/clear-index", handleClearIndex)
	r.GET("/mcp/tenant-stats", handleTenantStats)

	r.POST("/relay/index-jobs", handleCreateIndexJob)
	r.GET("/relay/index-jobs/:id", handleGetIndexJob)
	r.POST("/relay/index-jobs/:id/complete", handleCompleteIndexJob)
	r.POST("/relay/index-jobs/:id/fail", handleFailIndexJob)
	r.POST("/relay/remote-index", handleRemoteIndex)
	r.POST("/relay/agents/codebase-retrieval", handleCodebaseRetrieval)

	r.NoRoute(func(c *gin.Context) {
		if shouldDebugCapture(c.Request.URL.Path) {
			body, _ := io.ReadAll(c.Request.Body)
			logID, _ := c.Get(ContextKeyLogID)
			logIDStr, _ := logID.(string)
			log.Printf("[DEBUG_CAPTURE] unmatched_request id=%s path=%s method=%s client_ip=%s bytes=%d body=%s",
				logIDStr, c.Request.URL.Path, c.Request.Method, c.ClientIP(), len(body), previewBytesForLog(body, debugCaptureMaxBytes))
		}

		completeRequestLogAsync(getRequestLogEntry(c, http.StatusNotFound))
		c.JSON(http.StatusNotFound, gin.H{"error": "route not found"})
	})

	r.Run(serverAddr)
}
