package main

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"sort"
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
	trustedProxies           []string
	bannedCacheTTL           time.Duration
	defaultDailyRequestLimit int
	dailyIndexBytesLimit     int64
	proDailyRequestLimit     int
	proDailyIndexBytesLimit  int64
	debugCapturePaths        map[string]bool
	debugCaptureMaxBytes     int
	minClientVersion         string
	latestClientVersion      string
)

const (
	ContextKeyUserID    = "user_id"
	ContextKeyUserTier  = "user_tier"
	ContextKeyStartTime = "start_time"
	// 租户归并：tenant_id := org_id ?? user_id。个人密钥（org_id 空）的租户就是
	// user_id，行为与组织功能上线前完全一致。
	ContextKeyTenantID = "tenant_id"
	ContextKeyOrgID    = "org_id"
	ContextKeyOrgRole  = "org_role"

	// clientVersionHeader 是跨仓库契约值（docs/contracts/cloud-protocol.json
	// 的 clientRequestHeaders），由 contract_pin_test.go 钉住。
	clientVersionHeader  = "X-LCE-Client-Version"
	ContextKeyLogID      = "log_id"
	ContextKeyInsertDone = "insert_done"

	StatusPending   = "pending"
	StatusCompleted = "completed"

	LeaderboardUpdateInterval = 30 * time.Minute
	LeaderboardPath           = "/mcp/tools/call/codebase-retrieval"
	LeaderboardTopN           = 10
	LeaderboardTimezone       = "Asia/Shanghai"

	HealthCheckInterval = 2 * time.Minute
	HealthCheckTimeout  = 30 * time.Second
	maxClientMCPBody    = 32 << 20
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
		if !isLoopbackServerAddr(serverAddr) {
			log.Fatalf("[CONFIG] LCE_TENANT_ASSERTION_SECRET is required when binding to a non-loopback address (%s)", serverAddr)
		}
		log.Println("[CONFIG] LCE_TENANT_ASSERTION_SECRET 未配置：不签发租户断言。若 LCE 已开启校验，租户调用会被拒绝")
	}
	dbHost = getEnv("DB_HOST", "localhost")
	dbPort = getEnvInt("DB_PORT", 5432)
	dbUser = getEnv("DB_USER", "postgres")
	dbPassword = getEnv("DB_PASSWORD", "")
	dbName = getEnv("DB_NAME", "postgres")
	redisHost = getEnv("REDIS_HOST", "localhost")
	redisPort = getEnvInt("REDIS_PORT", 6379)
	trustedProxies = parseTrustedProxies(getEnv("TRUSTED_PROXIES", ""))
	bannedCacheTTL = getEnvDuration("BANNED_CACHE_TTL", 5*time.Minute)
	configureTrustedConsole(getEnv("CONSOLE_API_SECRET", ""))
	defaultDailyRequestLimit = getEnvInt("DEFAULT_DAILY_REQUEST_LIMIT", 0)
	// 索引通道除常规请求数配额外，还按上传字节计费：同样一次 upload 可以只带
	// 几字节，也可以携带一整批源码，请求数无法反映实际 embedding 成本。
	dailyIndexBytesLimit = getEnvInt64("DAILY_INDEX_BYTES_LIMIT", defaultDailyIndexBytes)
	// pro 档限额：与前端并行开发的分层配额契约。缺省 0 = 不限。
	proDailyRequestLimit = getEnvInt("PRO_DAILY_REQUEST_LIMIT", 0)
	proDailyIndexBytesLimit = getEnvInt64("PRO_DAILY_INDEX_BYTES_LIMIT", 0)
	initModelConfigKey()
	debugCapturePaths = parsePathSet(getEnv("DEBUG_CAPTURE_PATHS", ""))
	debugCaptureMaxBytes = getEnvInt("DEBUG_CAPTURE_MAX_BYTES", 4096)
	minClientVersion = strings.TrimSpace(getEnv("MIN_CLIENT_VERSION", ""))
	latestClientVersion = strings.TrimSpace(getEnv("LATEST_CLIENT_VERSION", ""))
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

func parseTrustedProxies(value string) []string {
	proxies := make([]string, 0)
	for _, raw := range strings.Split(value, ",") {
		if proxy := strings.TrimSpace(raw); proxy != "" {
			proxies = append(proxies, proxy)
		}
	}
	return proxies
}

func shouldDebugCapture(path string) bool {
	return debugCapturePaths != nil && (debugCapturePaths["*"] || debugCapturePaths[path])
}

// compareVersions 比较 "major.minor.patch[-prerelease]" 形式的版本号。
// 数字段逐段比较；数字段相同时按 semver 语义，带预发布后缀的一方更低
// （1.2.3-beta < 1.2.3）。两侧都带预发布后缀时不再细分先后，视为相等：
// 门禁场景只需要"预发布 < 正式"这一档语义。
func compareVersions(a, b string) int {
	aCore, aPre, _ := strings.Cut(a, "-")
	bCore, bPre, _ := strings.Cut(b, "-")
	aParts := strings.SplitN(aCore, ".", 4)
	bParts := strings.SplitN(bCore, ".", 4)
	for i := 0; i < 3; i++ {
		var av, bv int
		if i < len(aParts) {
			av, _ = strconv.Atoi(aParts[i])
		}
		if i < len(bParts) {
			bv, _ = strconv.Atoi(bParts[i])
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
	}
	if aPre == "" && bPre != "" {
		return 1
	}
	if aPre != "" && bPre == "" {
		return -1
	}
	return 0
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

func isLoopbackServerAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
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
	nReq.Header.Set("Accept", "application/json, text/event-stream")
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
	if nResp.StatusCode < http.StatusOK || nResp.StatusCode >= http.StatusMultipleChoices {
		go m.deleteRemoteSession(sid)
		return "", fmt.Errorf("MCP initialized notification returned %d", nResp.StatusCode)
	}

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

func marshalMCPToolCallBody(id int64, name string, args map[string]interface{}) ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      id,
		"method":  "tools/call",
		"params": map[string]interface{}{
			"name":      name,
			"arguments": args,
		},
	})
}

// validateMCPToolCallBody measures the actual downstream wire representation.
// Raw source-byte limits alone cannot protect LCE because JSON escaping and the
// request envelope add bytes after Relay has accepted the client request.
func validateMCPToolCallBody(name string, args map[string]interface{}) (int, error) {
	body, err := marshalMCPToolCallBody(math.MaxInt64, name, args)
	if err != nil {
		return 0, fmt.Errorf("encode MCP tools/call: %w", err)
	}
	if len(body) > maxLCEMCPRequestBodyBytes {
		return len(body), fmt.Errorf(
			"encoded LCE request exceeds the %d byte limit (%d bytes)",
			maxLCEMCPRequestBodyBytes,
			len(body),
		)
	}
	return len(body), nil
}

func (m *mcpClient) callTool(ctx context.Context, name string, args map[string]interface{}) (*mcpToolResult, error) {
	return m.callToolWithTimeout(ctx, name, args, defaultMCPCallTimeout)
}

func (m *mcpClient) callToolWithTimeout(ctx context.Context, name string, args map[string]interface{}, timeout time.Duration) (result *mcpToolResult, err error) {
	start := time.Now()
	defer func() {
		observeLCECall(name, start, err)
		if err != nil {
			logEvent("lce_call_failed",
				"request_id", requestIDFromContext(ctx),
				"tool", name,
				"error", err.Error(),
			)
		}
	}()
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
	body, err := marshalMCPToolCallBody(m.nextID.Add(1), name, args)
	if err != nil {
		return nil, false, fmt.Errorf("encode MCP tools/call: %w", err)
	}
	if len(body) > maxLCEMCPRequestBodyBytes {
		return nil, false, fmt.Errorf(
			"encoded LCE request exceeds the %d byte limit (%d bytes)",
			maxLCEMCPRequestBodyBytes,
			len(body),
		)
	}

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

		-- 检索只通过 MCP 暴露；索引谓词与 updateLeaderboard 的口径保持一致。
		DROP INDEX IF EXISTS idx_request_logs_codebase_retrieval;
		DROP INDEX IF EXISTS idx_request_logs_codebase_retrieval_v2;
		CREATE INDEX IF NOT EXISTS idx_request_logs_codebase_retrieval_v3
			ON request_logs(user_id, request_timestamp)
			WHERE request_path = '/mcp/tools/call/codebase-retrieval';
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

	// api_keys 表由前端创建与管理，relay 只读其中的 tier。IF EXISTS 兜底
	// 前端尚未建表的全新库；两边都用幂等 ADD COLUMN IF NOT EXISTS 建列不冲突。
	_, err = db.Exec(`ALTER TABLE IF EXISTS api_keys ADD COLUMN IF NOT EXISTS tier TEXT NOT NULL DEFAULT 'free'`)
	if err != nil {
		return fmt.Errorf("failed to migrate api_keys tier column: %w", err)
	}

	// 组织归属列同样由前端写入（NULL = 个人密钥）。org_role 取值 'owner'|'member'。
	// 两侧幂等 ADD COLUMN IF NOT EXISTS，谁先跑都不冲突。
	_, err = db.Exec(`
		ALTER TABLE IF EXISTS api_keys ADD COLUMN IF NOT EXISTS org_id TEXT;
		ALTER TABLE IF EXISTS api_keys ADD COLUMN IF NOT EXISTS org_role TEXT;
	`)
	if err != nil {
		return fmt.Errorf("failed to migrate api_keys org columns: %w", err)
	}

	// 审计补充：request_logs.user_id 永远是真实用户；tenant_id 记录该请求归属的
	// 租户（org_id ?? user_id），旧行为 NULL。追加 ALTER，不改已部署的 CREATE 文本。
	_, err = db.Exec(`ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(255)`)
	if err != nil {
		return fmt.Errorf("failed to migrate request_logs tenant_id column: %w", err)
	}

	if err := migrateAccessControlTables(); err != nil {
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
//
// api key 认证走一层进程内缓存（单实例设计，横向扩展前需外置到 Redis）：
//   - 正缓存 TTL 30s：封禁 / 删除 / 重置 key 的撤销延迟最多 30 秒，产品上可接受，
//     不做跨进程失效机制。
//   - 负缓存 TTL 5s：查无此 key 也短暂缓存，防止爆破流量打穿 PostgreSQL。
//   - 容量上限 authCacheMaxEntries，写入时若已满先惰性清理过期条目；仍满则
//     跳过缓存（请求本身仍走 DB 认证，fail-open 只影响缓存不影响正确性）。
//
// 哈希兼容：api_keys.id 由前端写入。存量为 MD5 hex，前端切换到 SHA-256 后
// 写入 SHA-256 hex；这里同时用两种哈希查询（id 为主键，IN 两值仍走索引），
// 因此前端切换写入算法时 relay 无需改动。

const (
	authCachePositiveTTL = 30 * time.Second
	authCacheNegativeTTL = 5 * time.Second
	authCacheMaxEntries  = 10000

	// tier 取值契约（与前端并行开发，已定死）：'free' | 'pro'，
	// 未知值按 free 处理（fail-safe：不给未知档位放开限额）。
	tierFree = "free"
	tierPro  = "pro"

	// org_role 契约值：'owner' | 'member'。删除类操作只认显式 'owner'，
	// 其他一切（member、空、脏数据）在组织语境下一律按非 owner 处理（fail-closed）。
	orgRoleOwner = "owner"
)

// normalizeTier 把 api_keys.tier 归一到已知档位：只有显式 'pro' 生效，
// 其他一切（空、未知值、脏数据）都按 free 处理。
func normalizeTier(tier string) string {
	if strings.TrimSpace(tier) == tierPro {
		return tierPro
	}
	return tierFree
}

// authIdentity 是一次认证解析出的完整身份：真实用户、tier、组织归属。
// TenantID 是数据归属与 LCE 租户的唯一口径：org_id ?? user_id。
type authIdentity struct {
	UserID  string
	Tier    string
	OrgID   string // 空 = 个人密钥
	OrgRole string // 仅 OrgID 非空时有意义；'owner' | 'member'
}

func (a authIdentity) TenantID() string {
	if a.OrgID != "" {
		return a.OrgID
	}
	return a.UserID
}

// userTierCtxKey 把 tier 从认证中间件带进 request context，供索引链路
// （handleCodebaseIndex → uploadIndexBatch → chargeIndexBytes）读取。
type userTierCtxKey struct{}

func withUserTier(ctx context.Context, tier string) context.Context {
	return context.WithValue(ctx, userTierCtxKey{}, normalizeTier(tier))
}

func userTierFromContext(ctx context.Context) string {
	if ctx == nil {
		return tierFree
	}
	tier, _ := ctx.Value(userTierCtxKey{}).(string)
	return normalizeTier(tier)
}

// authIdentityCtxKey 把完整身份带进 request context：索引链路的配额计费需要
// org_id 才能选择组织池限额。缺失时按零值（个人、free）处理，等价旧行为。
type authIdentityCtxKey struct{}

func withAuthIdentity(ctx context.Context, id authIdentity) context.Context {
	return context.WithValue(ctx, authIdentityCtxKey{}, id)
}

func authIdentityFromContext(ctx context.Context) authIdentity {
	if ctx == nil {
		return authIdentity{}
	}
	id, _ := ctx.Value(authIdentityCtxKey{}).(authIdentity)
	return id
}

type authCacheEntry struct {
	identity  authIdentity
	ok        bool
	expiresAt time.Time
}

var (
	authCacheMu sync.Mutex
	authCache   = make(map[string]authCacheEntry)
)

func authCacheGet(key string, now time.Time) (authCacheEntry, bool) {
	authCacheMu.Lock()
	defer authCacheMu.Unlock()
	entry, found := authCache[key]
	if !found || now.After(entry.expiresAt) {
		return authCacheEntry{}, false
	}
	return entry, true
}

func authCachePut(key string, entry authCacheEntry) {
	authCacheMu.Lock()
	defer authCacheMu.Unlock()
	if _, exists := authCache[key]; !exists && len(authCache) >= authCacheMaxEntries {
		now := time.Now()
		for k, v := range authCache {
			if now.After(v.expiresAt) {
				delete(authCache, k)
			}
		}
		if len(authCache) >= authCacheMaxEntries {
			return // 仍满：跳过缓存，不驱逐未过期条目
		}
	}
	authCache[key] = entry
}

func normalizeOrgRole(role, orgID string) string {
	if strings.TrimSpace(orgID) == "" {
		return ""
	}
	for _, candidate := range strings.Split(role, ",") {
		if strings.TrimSpace(candidate) == orgRoleOwner {
			return orgRoleOwner
		}
	}
	return "member"
}

// lookupAPIKey 同时按 MD5 与 SHA-256 哈希匹配 api_keys.id（双读过渡）。
// 个人密钥只依赖 Relay 自有表，避免 Better Auth 组织表尚未迁移时全站鉴权
// 中断。组织密钥再以 Better Auth 表为权威来源；api_keys.org_role 只是展示
// 冗余，不能让已移除成员或已降权 owner 继续获得组织权限。
func lookupAPIKey(token string) (authIdentity, bool, error) {
	md5Hash := md5.Sum([]byte(token))
	sha256Hash := sha256.Sum256([]byte(token))
	var id authIdentity
	err := db.QueryRow(`
		SELECT keys.user_id,
		       COALESCE(keys.tier, 'free'),
		       COALESCE(keys.org_id, '')
		FROM api_keys AS keys
		WHERE keys.id IN ($1, $2)
	`,
		hex.EncodeToString(md5Hash[:]), hex.EncodeToString(sha256Hash[:]),
	).Scan(&id.UserID, &id.Tier, &id.OrgID)
	if err == sql.ErrNoRows {
		return authIdentity{}, false, nil
	}
	if err != nil {
		return authIdentity{}, false, err
	}
	id.Tier = normalizeTier(id.Tier)
	id.OrgID = strings.TrimSpace(id.OrgID)
	if id.OrgID != "" {
		var currentRole string
		err = db.QueryRow(`
			SELECT COALESCE(members."role", '')
			FROM "member" AS members
			JOIN "organization" AS organizations
			  ON organizations."id" = members."organizationId"
			WHERE members."organizationId" = $1
			  AND members."userId" = $2
		`, id.OrgID, id.UserID).Scan(&currentRole)
		if err == sql.ErrNoRows {
			return authIdentity{}, false, nil
		}
		if err != nil {
			return authIdentity{}, false, err
		}
		id.OrgRole = normalizeOrgRole(currentRole, id.OrgID)
	}
	return id, true, nil
}

func authenticateRequest(c *gin.Context) (authIdentity, bool) {
	authHeader := c.GetHeader("Authorization")
	if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
		return authIdentity{}, false
	}

	token := strings.TrimPrefix(authHeader, "Bearer ")
	cacheKey := func() string {
		sum := sha256.Sum256([]byte(token))
		return hex.EncodeToString(sum[:])
	}()

	now := time.Now()
	if entry, hit := authCacheGet(cacheKey, now); hit {
		return entry.identity, entry.ok
	}

	identity, ok, err := lookupAPIKey(token)
	if err != nil {
		// DB 故障：fail-closed 拒绝本次请求，且不缓存，故障恢复后立即回归正常
		return authIdentity{}, false
	}
	ttl := authCachePositiveTTL
	if !ok {
		ttl = authCacheNegativeTTL
	}
	authCachePut(cacheKey, authCacheEntry{identity: identity, ok: ok, expiresAt: now.Add(ttl)})
	return identity, ok
}

func authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		startTime := time.Now()
		c.Set(ContextKeyStartTime, startTime)

		identity, ok := authenticateRequest(c)
		if !ok {
			authHeader := c.GetHeader("Authorization")
			if authHeader == "" {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing authorization header"})
			} else {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
			}
			return
		}
		userID := identity.UserID
		tier := identity.Tier
		tenantID := identity.TenantID()

		c.Set(ContextKeyUserID, userID)
		c.Set(ContextKeyUserTier, tier)
		c.Set(ContextKeyTenantID, tenantID)
		c.Set(ContextKeyOrgID, identity.OrgID)
		c.Set(ContextKeyOrgRole, identity.OrgRole)
		// tier 与完整身份同时进 request context：索引链路（uploadIndexBatch）只拿
		// 得到 context.Context，不经过 gin.Context；配额计费还需要 org 归属。
		c.Request = c.Request.WithContext(
			withAuthIdentity(withUserTier(c.Request.Context(), tier), identity),
		)

		{
			clientVersion := strings.TrimSpace(c.GetHeader(clientVersionHeader))
			gate := evaluateVersionGate(clientVersion)
			recordVersionGate(gate)
			if gate == "reject_426" {
				logEvent("client_version_rejected",
					"user_id", userID,
					"path", c.Request.URL.Path,
					"client_version", clientVersion,
					"min_version", minClientVersion,
				)
				c.AbortWithStatusJSON(http.StatusUpgradeRequired, gin.H{
					"error":       fmt.Sprintf("client version %s is below minimum %s; please update lce-cloud", clientVersion, minClientVersion),
					"min_version": minClientVersion,
				})
				return
			}
		}

		if isUserBanned(userID) {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "account banned; contact the administrator"})
			return
		}

		trustedConsole := isTrustedConsoleRequest(c)
		if !trustedConsole {
			if ok, limit := checkRequestQuota(userID, identity.OrgID, tier); !ok {
				logEvent("quota_rejected",
					"user_id", userID,
					"tenant", tenantID,
					"tier", tier,
					"path", c.Request.URL.Path,
					"limit", strconv.Itoa(limit),
				)
				c.Header("Retry-After", quotaRetryAfterHeader(time.Now()))
				c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": fmt.Sprintf("daily request quota exceeded (%d/day)", limit)})
				return
			}
		}

		if trustedConsole {
			c.Next()
			return
		}

		logID := uuid.New().String()
		c.Set(ContextKeyLogID, logID)
		// request_id 复用请求日志表的 id：响应头回传方便用户报障，
		// 塞进 request context 让 LCE 调用的错误日志能带上同一个 id。
		c.Header("X-Request-Id", logID)
		c.Request = c.Request.WithContext(withRequestID(c.Request.Context(), logID))
		logEvent("request_start",
			"request_id", logID,
			"user_id", userID,
			"path", c.Request.URL.Path,
			"method", c.Request.Method,
		)

		insertDone := make(chan struct{})
		c.Set(ContextKeyInsertDone, insertDone)

		path := c.Request.URL.Path
		method := c.Request.Method
		clientIP := c.ClientIP()
		go func() {
			defer close(insertDone)
			_, err := db.Exec(`
				INSERT INTO request_logs (id, user_id, tenant_id, status, request_path, request_method, request_timestamp, client_ip)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			`, logID, userID, tenantID, StatusPending, path, method, startTime, clientIP)
			if err != nil {
				log.Printf("[ERROR] Failed to insert request log: %v", err)
			}
		}()

		c.Next()
	}
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
	mcpServerName           = "lce"
	mcpServerVersion        = "2.0.0"
	mcpSessionTTL           = 30 * time.Minute
	mcpSessionSweepInterval = 60 * time.Second
	mcpMaxSessions          = 1000
	mcpMaxSessionsPerUser   = 16
	toolsCacheTTL           = 5 * time.Minute
)

type chatMCPToolPolicy struct {
	description string
	arguments   map[string]struct{}
	required    map[string]struct{}
}

// Chat MCP is a remote tenant surface, not a transparent proxy to every tool
// installed in LCE. Keep both tools and caller-controlled arguments explicit so
// future local/admin capabilities cannot become remotely callable by accident.
var chatMCPToolPolicies = map[string]chatMCPToolPolicy{
	"codebase-retrieval": {
		description: "Search the authenticated user's server-side LCE index. Use information_request for the semantic goal and technical_terms for exact identifier hints. Pass the optional root_id to scope retrieval to one indexed root; omit it to search the tenant default scope. Returns a tenant context pack; local worktree, Git, connectors, workflows, and context_bundle are not available through this remote service.",
		arguments: stringSet(
			"information_request",
			"technical_terms",
			"response_format",
			"root_id",
		),
		required: stringSet("information_request"),
	},
	"codebase_symbol_graph": {
		description: "Query the authenticated user's server-side symbol graph for definitions, references, callers, callees, importers, related tests, and bounded impact. root_id is required because indexing protocol v1 never writes to the shared default root; the service supplies the tenant identity.",
		arguments: stringSet(
			"root_id",
			"symbol",
			"query_type",
			"depth",
			"limit",
			"under",
			"include_snippets",
			"response_format",
		),
		required: stringSet("root_id", "symbol"),
	},
}

func stringSet(values ...string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}

func sortedStringSetKeys(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func isChatMCPToolAllowed(name string) bool {
	if strings.TrimSpace(name) == codebaseIndexToolName {
		return true
	}
	_, allowed := chatMCPToolPolicies[strings.TrimSpace(name)]
	return allowed
}

func chatMCPToolUsesRerankConfig(name string) bool {
	return name == "codebase-retrieval"
}

func rewriteToolSchema(toolJSON json.RawMessage) (json.RawMessage, error) {
	var tool map[string]interface{}
	if err := json.Unmarshal(toolJSON, &tool); err != nil {
		return toolJSON, nil
	}

	name, _ := tool["name"].(string)
	name = strings.TrimSpace(name)
	policy, needsRewrite := chatMCPToolPolicies[name]
	if !needsRewrite {
		return toolJSON, nil
	}
	tool["name"] = name
	tool["description"] = policy.description

	schemaRaw, ok := tool["inputSchema"]
	if !ok {
		return nil, fmt.Errorf("allowed tool %q has no inputSchema", name)
	}
	schema, ok := schemaRaw.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("allowed tool %q has a non-object inputSchema", name)
	}

	props, ok := schema["properties"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("allowed tool %q has no object properties schema", name)
	}
	for field := range props {
		if _, allowed := policy.arguments[field]; !allowed {
			delete(props, field)
		}
	}
	for _, field := range sortedStringSetKeys(policy.required) {
		if _, present := props[field]; !present {
			return nil, fmt.Errorf("allowed tool %q is missing required property %q", name, field)
		}
	}

	schema["required"] = sortedStringSetKeys(policy.required)
	schema["additionalProperties"] = false

	delete(schema, "oneOf")

	return json.Marshal(tool)
}

func validateChatMCPToolArgs(toolName string, args map[string]interface{}) error {
	policy, ok := chatMCPToolPolicies[toolName]
	if !ok {
		return fmt.Errorf("tool is not available")
	}
	unsupported := make([]string, 0)
	for field := range args {
		if _, allowed := policy.arguments[field]; !allowed {
			unsupported = append(unsupported, field)
		}
	}
	if len(unsupported) > 0 {
		sort.Strings(unsupported)
		return fmt.Errorf("unsupported arguments for %s: %s", toolName, strings.Join(unsupported, ", "))
	}
	for _, field := range sortedStringSetKeys(policy.required) {
		value, exists := args[field]
		if !exists || value == nil {
			return fmt.Errorf("missing required argument: %s", field)
		}
		if text, ok := value.(string); ok && strings.TrimSpace(text) == "" {
			return fmt.Errorf("required argument must not be blank: %s", field)
		}
	}
	// root_id 透传给 LCE 前施加与 codebase_index 相同的长度上限（fail-closed：
	// 非字符串一律拒绝）。只有白名单里含 root_id 的工具才会走到这里。
	if value, exists := args["root_id"]; exists && value != nil {
		text, ok := value.(string)
		if !ok {
			return fmt.Errorf("root_id must be a string")
		}
		if len(text) > maxIndexRootIDLen {
			return fmt.Errorf("root_id exceeds %d bytes", maxIndexRootIDLen)
		}
	}
	return nil
}

func filterChatMCPTools(raw json.RawMessage) (json.RawMessage, error) {
	var tools []json.RawMessage
	if err := json.Unmarshal(raw, &tools); err != nil {
		return nil, fmt.Errorf("parse MCP tools: %w", err)
	}
	filtered := make([]json.RawMessage, 0, len(chatMCPToolPolicies))
	seen := make(map[string]struct{}, len(chatMCPToolPolicies))
	for _, tool := range tools {
		var metadata struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(tool, &metadata); err != nil {
			return nil, fmt.Errorf("parse MCP tool metadata: %w", err)
		}
		metadata.Name = strings.TrimSpace(metadata.Name)
		if _, allowed := chatMCPToolPolicies[metadata.Name]; !allowed {
			continue
		}
		if _, duplicate := seen[metadata.Name]; duplicate {
			return nil, fmt.Errorf("duplicate allowed MCP tool %q", metadata.Name)
		}
		rewritten, err := rewriteToolSchema(tool)
		if err != nil {
			return nil, fmt.Errorf("rewrite MCP tool schema %q: %w", metadata.Name, err)
		}
		filtered = append(filtered, rewritten)
		seen[metadata.Name] = struct{}{}
	}
	missing := make([]string, 0)
	for name := range chatMCPToolPolicies {
		if _, present := seen[name]; !present {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return nil, fmt.Errorf("upstream LCE is missing required public MCP tools: %s", strings.Join(missing, ", "))
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
	tools, err = appendCodebaseIndexTool(tools)
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

	body, err := io.ReadAll(io.LimitReader(c.Request.Body, maxClientMCPBody+1))
	if err != nil {
		c.JSON(http.StatusBadRequest, rpcError(nil, -32700, "failed to read request body"))
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusBadRequest))
		return
	}
	if len(body) > maxClientMCPBody {
		c.JSON(http.StatusRequestEntityTooLarge, rpcError(nil, -32600, "MCP request body exceeds 32 MiB"))
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusRequestEntityTooLarge))
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
			"serverInfo":      map[string]interface{}{"name": mcpServerName, "version": mcpServerVersion},
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

func clientUpdateAvailable(c *gin.Context) bool {
	if latestClientVersion == "" {
		return false
	}
	v := strings.TrimSpace(c.GetHeader(clientVersionHeader))
	if v != "" && compareVersions(v, latestClientVersion) < 0 {
		recordVersionGate("update_hint")
		return true
	}
	return false
}

// requestTenantID 取本请求的租户（org_id ?? user_id）。中间件未设置时（如
// 单测直接构造 gin.Context）退回 user_id，与组织功能上线前行为一致。
func requestTenantID(c *gin.Context) string {
	if tenantID := c.GetString(ContextKeyTenantID); tenantID != "" {
		return tenantID
	}
	return c.GetString(ContextKeyUserID)
}

func handleMCPToolsCall(c *gin.Context, id json.RawMessage, params json.RawMessage, userID string) {
	tenantID := requestTenantID(c)
	if tenantID == "" {
		tenantID = userID
	}
	var p struct {
		Name      string                 `json:"name"`
		Arguments map[string]interface{} `json:"arguments"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		c.JSON(http.StatusOK, rpcError(id, -32602, "Invalid params"))
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))
		return
	}
	p.Name = strings.TrimSpace(p.Name)
	if !isChatMCPToolAllowed(p.Name) {
		c.JSON(http.StatusOK, rpcError(id, -32601, "Tool not available through chat MCP: "+p.Name))
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))
		return
	}

	logIDStr, _ := c.Get(ContextKeyLogID)
	logIDVal, _ := logIDStr.(string)
	toolPath := "/mcp/tools/call/" + p.Name
	c.Set(ContextKeyMetricsPath, toolPath)
	updateRequestPathAsync(logIDVal, toolPath, getInsertDone(c))

	if p.Arguments == nil {
		p.Arguments = make(map[string]interface{})
	}
	if p.Name == codebaseIndexToolName {
		// 索引编排与数据归属全部按租户：index_jobs 等表的 user_id 列存 tenantID。
		payload, err := handleCodebaseIndex(c.Request.Context(), tenantID, p.Arguments)
		if err != nil {
			if errors.Is(c.Request.Context().Err(), context.Canceled) {
				completeRequestLogAsync(getRequestLogEntry(c, 499))
				return
			}
			errorSource := "relay"
			var upstreamErr *indexUpstreamError
			if errors.As(err, &upstreamErr) {
				errorSource = "lce"
			}
			saveErrorDetailsAsync(logIDVal, errorSource, err.Error(), getInsertDone(c))
			encoded, _ := json.Marshal(codebaseIndexEnvelope(false, nil, err.Error()))
			c.JSON(http.StatusOK, rpcResult(id, map[string]interface{}{
				"content": []map[string]interface{}{{"type": "text", "text": string(encoded)}},
				"isError": true,
			}))
			completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))
			return
		}
		indexResult := codebaseIndexEnvelope(true, payload, "")
		if clientUpdateAvailable(c) {
			indexResult["_client_update_available"] = true
		}
		encoded, err := json.Marshal(indexResult)
		if err != nil {
			c.JSON(http.StatusOK, rpcError(id, -32000, "encode index response: "+err.Error()))
			completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))
			return
		}
		c.JSON(http.StatusOK, rpcResult(id, map[string]interface{}{
			"content": []map[string]interface{}{{"type": "text", "text": string(encoded)}},
			"isError": false,
		}))
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))
		return
	}
	if err := validateChatMCPToolArgs(p.Name, p.Arguments); err != nil {
		c.JSON(http.StatusOK, rpcError(id, -32602, err.Error()))
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))
		return
	}
	// 传给 LCE 的租户永远是 tenant_id := org_id ?? user_id（强制覆写，客户端不可指定）。
	p.Arguments["tenant_id"] = tenantID
	var operationLease *indexOperationLease
	var cfg map[string]interface{}
	var err error
	if chatMCPToolUsesRerankConfig(p.Name) {
		// 操作租约按租户（与索引 publish/clear 串行的是租户数据）；
		// BYO rerank 配置仍按真实用户（用的是调用者自己的 key）。
		operationLease, cfg, err = acquireModelConfigOperation(c.Request.Context(), tenantID, userID, "chat-retrieval")
	} else {
		operationLease, err = acquireSharedIndexOperation(
			c.Request.Context(),
			tenantID,
			"chat-tool:"+uuid.NewString(),
			"chat-"+p.Name,
		)
	}
	if err != nil {
		c.JSON(http.StatusOK, rpcError(id, -32000, err.Error()))
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))
		return
	}
	defer operationLease.Release()
	if cfg != nil {
		p.Arguments["model_config"] = cfg
	}

	result, err := lce.callTool(operationLease.Context(), p.Name, p.Arguments)
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

	// _client_update_available 与 _index_status 合并注入：同一次 unmarshal/marshal。
	// _index_status 只注入 codebase-retrieval 的成功 JSON 响应；查询失败只降级为
	// 不注入（进度可见是 best-effort，不能拖垮检索本身）。
	var activeJob *activeIndexJob
	if p.Name == "codebase-retrieval" && !result.IsError {
		if activeJob, err = loadActiveIndexJob(c.Request.Context(), tenantID); err != nil {
			log.Printf("[INDEX_STATUS] active job lookup failed (tenant=%s): %v", tenantID, err)
			activeJob = nil
		}
	}
	contentText := injectRetrievalExtras(result.Content, clientUpdateAvailable(c), activeJob)

	c.JSON(http.StatusOK, rpcResult(id, map[string]interface{}{
		"content": []map[string]interface{}{
			{"type": "text", "text": contentText},
		},
		"isError": result.IsError,
	}))
	completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))
}

// ── 清除索引（冷却 + 日志清理）──────────────────────────────────────────

const clearIndexCooldownSeconds = 72 * 60 * 60 // 72 hours

// clearIndexCooldownKey 按租户计冷却：组织索引是共享数据，冷却也共享。
func clearIndexCooldownKey(tenantID string) string {
	return "clear_cooldown:" + tenantID
}

func checkClearIndexCooldown(ctx context.Context, tenantID string) error {
	if redisClient == nil {
		return nil
	}
	ttl, err := redisClient.TTL(ctx, clearIndexCooldownKey(tenantID)).Result()
	if err != nil || ttl <= 0 {
		return nil
	}
	hours := int(ttl.Hours())
	minutes := int(ttl.Minutes()) % 60
	return fmt.Errorf("清除索引冷却中，剩余 %d 小时 %d 分钟后可再次操作", hours, minutes)
}

func setClearIndexCooldown(ctx context.Context, tenantID string) {
	if redisClient == nil {
		return
	}
	redisClient.Set(ctx, clearIndexCooldownKey(tenantID), "1", time.Duration(clearIndexCooldownSeconds)*time.Second)
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

func handleClearIndex(c *gin.Context) {
	userID, _ := c.Get(ContextKeyUserID)
	userIDStr, _ := userID.(string)
	if userIDStr == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusUnauthorized))
		return
	}
	// 数据归属按租户：组织密钥清的是整个组织的索引，个人密钥行为不变。
	tenantID := requestTenantID(c)

	if rejectNonOwnerIndexDeletion(c) {
		return
	}

	if err := checkClearIndexCooldown(c.Request.Context(), tenantID); err != nil {
		c.JSON(http.StatusTooManyRequests, gin.H{"error": err.Error()})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusTooManyRequests))
		return
	}
	lease, err := acquireExclusiveIndexOperation(c.Request.Context(), tenantID, "clear-index")
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
	if err := clearUserIndexState(opCtx, tenantID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "清除 Relay 索引状态失败: " + err.Error()})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusInternalServerError))
		return
	}

	args := map[string]interface{}{"tenant_id": tenantID}
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

	setClearIndexCooldown(c.Request.Context(), tenantID)
	// 审计日志按真实用户删除：request_logs 归属个人，与租户归并无关。
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

	// 统计口径按租户：组织成员看到的是组织共享索引的统计。
	tenantID := requestTenantID(c)
	args := map[string]interface{}{"tenant_id": tenantID}
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
		tenantID,
		indexJobStatusCompleted,
	)
	if err := row.Scan(&completedJobCount); err != nil {
		log.Printf("[STATS] completed index job count failed (tenant=%s): %v", tenantID, err)
	}
	stats["indexingCount"] = completedJobCount

	// active_job 是对外契约字段：有 running 任务才带，字段名不可改。
	if activeJob, err := loadActiveIndexJob(c.Request.Context(), tenantID); err != nil {
		log.Printf("[STATS] active job lookup failed (tenant=%s): %v", tenantID, err)
	} else if activeJob != nil {
		stats["active_job"] = activeJob.activeJobPayload()
	}

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
		WHERE request_path = $1
		  AND request_timestamp >= $2
		  AND request_timestamp < $3
		  AND status_code = 200
		GROUP BY user_id
		ORDER BY cnt DESC
		LIMIT $4
	`, LeaderboardPath, dayStart, dayEnd, LeaderboardTopN)
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
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate leaderboard rows: %w", err)
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
		// /metrics 默认只在这个 loopback 内部端口暴露：nginx 只向 relay 转发
		// /mcp 前缀，主端口不注册 /metrics（除非配置 METRICS_TOKEN）。
		pprofMux.Handle("/metrics", metricsHandler())
		log.Println("[PPROF] Listening on 127.0.0.1:6060 (pprof + /metrics)")
		if err := http.ListenAndServe("127.0.0.1:6060", pprofMux); err != nil {
			log.Printf("[PPROF] Server error: %v", err)
		}
	}()

	// 等价于 gin.Default()，但 Recovery 换成结构化日志版本：panic 会带
	// request_id/path 落一条 logfmt 事件再回 500。
	r := gin.New()
	if err := r.SetTrustedProxies(trustedProxies); err != nil {
		log.Fatalf("TRUSTED_PROXIES 配置无效: %v", err)
	}
	r.Use(gin.Logger())
	r.Use(gin.CustomRecovery(func(c *gin.Context, recovered interface{}) {
		logID, _ := c.Get(ContextKeyLogID)
		logIDStr, _ := logID.(string)
		logEvent("panic_recovered",
			"request_id", logIDStr,
			"path", c.Request.URL.Path,
			"method", c.Request.Method,
			"panic", fmt.Sprintf("%v", recovered),
		)
		c.AbortWithStatus(http.StatusInternalServerError)
	}))
	r.Use(metricsMiddleware())

	// 可选的公网 /metrics：仅当 METRICS_TOKEN 非空时注册（注册在 authMiddleware
	// 之前，不走 api key 认证）；token 不匹配一律 404，不泄露端点存在性。
	// 未配置时该路由不存在，自然 404。
	if metricsToken := strings.TrimSpace(os.Getenv("METRICS_TOKEN")); metricsToken != "" {
		metricsGinHandler := gin.WrapH(metricsHandler())
		r.GET("/metrics", func(c *gin.Context) {
			if c.GetHeader("Authorization") != "Bearer "+metricsToken {
				c.JSON(http.StatusNotFound, gin.H{"error": "route not found"})
				return
			}
			metricsGinHandler(c)
		})
	}

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
	r.GET("/mcp/roots", handleListRoots)
	r.POST("/mcp/delete-root", handleDeleteRoot)

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
