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

	"github.com/andybalholm/brotli"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

// ── 配置 ──────────────────────────────────────────────────────────────────

var (
	serverAddr           string
	lceMCPURL            string
	dbHost               string
	dbPort               int
	dbUser               string
	dbPassword           string
	dbName               string
	redisHost            string
	redisPort            int
	apiKeyCacheTTL       time.Duration
	debugCapturePaths    map[string]bool
	debugCaptureMaxBytes int
)

const (
	ContextKeyUserID     = "user_id"
	ContextKeyStartTime  = "start_time"
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
)

func loadConfig() {
	_ = godotenv.Load()

	serverAddr = getEnv("SERVER_ADDR", "127.0.0.1:8080")
	lceMCPURL = getEnv("LCE_MCP_URL", "http://127.0.0.1:3000/mcp")
	dbHost = getEnv("DB_HOST", "localhost")
	dbPort = getEnvInt("DB_PORT", 5432)
	dbUser = getEnv("DB_USER", "postgres")
	dbPassword = getEnv("DB_PASSWORD", "")
	dbName = getEnv("DB_NAME", "postgres")
	redisHost = getEnv("REDIS_HOST", "localhost")
	redisPort = getEnvInt("REDIS_PORT", 6379)
	apiKeyCacheTTL = getEnvDuration("API_KEY_CACHE_TTL", 30*time.Minute)
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
		Timeout: 120 * time.Second,
	},
}

func (m *mcpClient) ensureSession(ctx context.Context) (string, error) {
	m.mu.RLock()
	sid := m.sessionID
	m.mu.RUnlock()
	if sid != "" {
		return sid, nil
	}
	return m.initSession(ctx)
}

func (m *mcpClient) initSession(ctx context.Context) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sessionID != "" {
		return m.sessionID, nil
	}

	initBody, _ := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      m.nextID.Add(1),
		"method":  "initialize",
		"params": map[string]interface{}{
			"protocolVersion": "2025-03-26",
			"capabilities":   map[string]interface{}{},
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
	m.sessionID = ""
	m.mu.Unlock()
}

type mcpToolResult struct {
	Content []byte
	IsError bool
}

func (m *mcpClient) callTool(ctx context.Context, name string, args map[string]interface{}) (*mcpToolResult, error) {
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

		CREATE INDEX IF NOT EXISTS idx_request_logs_codebase_retrieval
			ON request_logs(user_id, request_timestamp)
			WHERE request_path = '/agents/codebase-retrieval';
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

	return nil
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
	cacheKey := "apikey:" + tokenMD5

	ctx := context.Background()
	if userID, err := redisClient.Get(ctx, cacheKey).Result(); err == nil {
		return userID, true
	}

	var userID string
	err := db.QueryRow("SELECT user_id FROM api_keys WHERE id = $1", tokenMD5).Scan(&userID)
	if err != nil {
		return "", false
	}

	redisClient.Set(ctx, cacheKey, userID, apiKeyCacheTTL)
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
	toolsCacheTTL           = 5 * time.Minute
)

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

func sweepExpiredMCPSessions() {
	serverSessionsMu.Lock()
	defer serverSessionsMu.Unlock()
	now := time.Now()
	for id, session := range serverSessions {
		if now.Sub(session.lastActivity) > mcpSessionTTL {
			delete(serverSessions, id)
			log.Printf("[MCP_SERVER] Session expired: %s", id)
		}
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

func getCachedToolsList(ctx context.Context) (json.RawMessage, error) {
	toolsCacheMu.RLock()
	if toolsCache != nil && time.Since(toolsCacheTime) < toolsCacheTTL {
		cached := toolsCache
		toolsCacheMu.RUnlock()
		return cached, nil
	}
	toolsCacheMu.RUnlock()

	tools, err := lce.fetchToolsList(ctx)
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

	if rpc.Method == "notifications/initialized" {
		c.Status(http.StatusAccepted)
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusAccepted))
		return
	}

	if rpc.Method == "initialize" {
		if sessionID != "" {
			c.JSON(http.StatusBadRequest, rpcError(rpc.ID, -32600, "initialize must not include Mcp-Session-Id"))
			completeRequestLogAsync(getRequestLogEntry(c, http.StatusBadRequest))
			return
		}

		serverSessionsMu.Lock()
		if len(serverSessions) >= mcpMaxSessions {
			serverSessionsMu.Unlock()
			c.JSON(http.StatusServiceUnavailable, rpcError(rpc.ID, -32000, "session limit exceeded"))
			completeRequestLogAsync(getRequestLogEntry(c, http.StatusServiceUnavailable))
			return
		}
		newSID := uuid.New().String()
		serverSessions[newSID] = &mcpServerSession{
			userID:       userID,
			lastActivity: time.Now(),
		}
		serverSessionsMu.Unlock()

		c.Header("Mcp-Session-Id", newSID)
		c.JSON(http.StatusOK, rpcResult(rpc.ID, map[string]interface{}{
			"protocolVersion": mcpProtocolVersion,
			"capabilities":   map[string]interface{}{"tools": map[string]interface{}{}},
			"serverInfo":     map[string]interface{}{"name": mcpRelayName, "version": mcpRelayVersion},
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

	serverSessionsMu.RLock()
	session, ok := serverSessions[sessionID]
	serverSessionsMu.RUnlock()
	if !ok {
		c.JSON(http.StatusNotFound, rpcError(rpc.ID, -32000, "Invalid or expired session"))
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusNotFound))
		return
	}
	session.lastActivity = time.Now()

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

	logIDStr, _ := c.Get(ContextKeyLogID)
	logIDVal, _ := logIDStr.(string)
	toolPath := "/mcp/tools/call/" + p.Name
	updateRequestPathAsync(logIDVal, toolPath, getInsertDone(c))

	if p.Arguments == nil {
		p.Arguments = make(map[string]interface{})
	}
	p.Arguments["tenant_id"] = userID

	if p.Name == "codebase_clear_index" {
		if err := checkClearIndexCooldown(c.Request.Context(), userID); err != nil {
			c.JSON(http.StatusOK, rpcError(id, -32000, err.Error()))
			completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))
			return
		}
	}

	result, err := lce.callTool(c.Request.Context(), p.Name, p.Arguments)
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

	if p.Name == "codebase_clear_index" {
		setClearIndexCooldown(c.Request.Context(), userID)
		deleteUserLogsAsync(userID)
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

func handleFindMissing(c *gin.Context) {
	userID, _ := c.Get(ContextKeyUserID)
	userIDStr, _ := userID.(string)
	if userIDStr == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusUnauthorized))
		return
	}

	var req struct {
		Files []struct {
			Path string `json:"path"`
			Hash string `json:"hash"`
			Size int64  `json:"size"`
		} `json:"files"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusBadRequest))
		return
	}

	filesArg := make([]map[string]interface{}, len(req.Files))
	for i, f := range req.Files {
		filesArg[i] = map[string]interface{}{"path": f.Path, "hash": f.Hash, "size": f.Size}
	}
	args := map[string]interface{}{"tenant_id": userIDStr, "files": filesArg}
	result, err := lce.callTool(c.Request.Context(), "codebase_find_missing", args)
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

	c.Data(http.StatusOK, "application/json", result.Content)
	completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))
}

func handleRemoteIndex(c *gin.Context) {
	userID, _ := c.Get(ContextKeyUserID)
	userIDStr, _ := userID.(string)
	if userIDStr == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusUnauthorized))
		return
	}

	var req struct {
		Files []struct {
			Path    string `json:"path"`
			Content string `json:"content"`
			Hash    string `json:"hash"`
		} `json:"files"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusBadRequest))
		return
	}

	filesArg := make([]map[string]interface{}, len(req.Files))
	for i, f := range req.Files {
		filesArg[i] = map[string]interface{}{"path": f.Path, "content": f.Content, "hash": f.Hash}
	}
	args := map[string]interface{}{"tenant_id": userIDStr, "files": filesArg}
	result, err := lce.callTool(c.Request.Context(), "codebase_remote_index", args)
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

	c.Data(http.StatusOK, "application/json", result.Content)
	completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))
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

	args := map[string]interface{}{
		"tenant_id": userIDStr,
		"query":     req.InformationRequest,
	}
	result, err := lce.callTool(c.Request.Context(), "codebase-retrieval", args)
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
		content = content[:req.MaxOutputLength]
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

	args := map[string]interface{}{"tenant_id": userIDStr}
	result, err := lce.callTool(c.Request.Context(), "codebase_clear_index", args)
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

	var indexingCount int
	row := db.QueryRow(
		`SELECT COUNT(*) FROM request_logs WHERE user_id = $1 AND request_path = '/mcp' AND status_code = 200`,
		userIDStr,
	)
	_ = row.Scan(&indexingCount)
	stats["indexingCount"] = indexingCount

	c.JSON(http.StatusOK, stats)
	completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))
}

func handleMCPDelete(c *gin.Context) {
	sessionID := c.GetHeader("Mcp-Session-Id")
	if sessionID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing Mcp-Session-Id"})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusBadRequest))
		return
	}

	serverSessionsMu.Lock()
	_, existed := serverSessions[sessionID]
	delete(serverSessions, sessionID)
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

func extractSSEData(raw []byte) []byte {
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "data: ") {
			return []byte(strings.TrimPrefix(line, "data: "))
		}
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

func runHealthProbe() {
	ctx, cancel := context.WithTimeout(context.Background(), HealthCheckTimeout)
	defer cancel()

	var lceLatencyMs sql.NullInt64
	var errMsg sql.NullString
	status := "success"

	defer func() {
		nextCheckAt := time.Now().Add(HealthCheckInterval)
		_, dbErr := db.Exec(
			`INSERT INTO health_checks (status, tcp_ping_ms, codebase_retrieval_ms, error_message, next_check_at)
			 VALUES ($1, $2, $3, $4, $5)`,
			status, sql.NullInt64{}, lceLatencyMs, errMsg, nextCheckAt,
		)
		if dbErr != nil {
			log.Printf("[HEALTH] Failed to save result: %v", dbErr)
		}
	}()

	t0 := time.Now()
	err := lce.toolsList(ctx)
	lceLatencyMs = sql.NullInt64{Int64: time.Since(t0).Milliseconds(), Valid: true}

	if err != nil {
		status = "error"
		errMsg = sql.NullString{String: err.Error(), Valid: true}
		return
	}

	log.Printf("[HEALTH] Probe OK: lce_latency=%dms", lceLatencyMs.Int64)
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
	r.POST("/mcp/clear-index", handleClearIndex)
	r.GET("/mcp/tenant-stats", handleTenantStats)

	r.POST("/relay/find-missing", handleFindMissing)
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
