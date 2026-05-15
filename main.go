package main

import (
	"bufio"
	"bytes"
	"compress/flate"
	"compress/gzip"
	"context"
	"crypto/md5"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/andybalholm/brotli"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/klauspost/compress/zstd"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"golang.org/x/net/proxy"
)

// 从环境变量加载的配置
var (
	serverAddr      string
	augmentAPIURL   string
	augmentAPIToken string
	dbHost          string
	dbPort          int
	dbUser          string
	dbPassword      string
	dbName          string
	redisPort       int
	apiKeyCacheTTL  time.Duration
	sessionTTL      time.Duration
	upstreamProxy   string
)

const (
	// Context keys
	ContextKeyUserID      = "user_id"
	ContextKeyStartTime   = "start_time"
	ContextKeyLogID       = "log_id"
	ContextKeyInsertDone  = "insert_done"

	// 请求状态
	StatusPending   = "pending"
	StatusCompleted = "completed"

	// Leaderboard 配置
	LeaderboardUpdateInterval = 30 * time.Minute
	LeaderboardPath           = "/agents/codebase-retrieval"
	LeaderboardTopN           = 10
	LeaderboardTimezone       = "Asia/Shanghai"

	// Health check 配置
	HealthCheckInterval = 2 * time.Minute
	HealthCheckTimeout  = 60 * time.Second
)

var allowedPaths = []string{
	"/get-models",
	"/agents/list-remote-tools",
	"/find-missing",
	"/batch-upload",
	"/checkpoint-blobs",
	"/agents/codebase-retrieval",
	"/record-request-events",
	"/report-error",
	"/settings/get-mcp-user-configs",
	"/settings/get-mcp-tenant-configs",
	"/indexed-commits/register-blobset",
	"/indexed-commits/get-latest-blobset",
}

var ssePaths = []string{
	"/chat-stream",
	"/prompt-enhancer",
}

// ChatStreamRequest 用于验证 /chat-stream 请求
type ChatStreamRequest struct {
	Message      string  `json:"message"`
	Mode         string  `json:"mode"`
	SystemPrompt *string `json:"system_prompt"`
}

const PROMPT_ENHANCER_MESSAGE_PREFIX = "⚠️ NO TOOLS ALLOWED ⚠️\n\nHere is an instruction that I'd like to give you, but it needs to be improved. Rewrite and enhance this instruction to make it clearer, more specific, less ambiguous, and correct any mistakes. Do not use any tools: reply immediately with your answer, even if you're not sure. Consider the context of our conversation history when enhancing the prompt. If there is code in triple backticks (```) consider whether it is a code sample and should remain unchanged.Reply with the following format:\n\n### BEGIN RESPONSE ###\nHere is an enhanced version of the original instruction that is more specific and clear:\n<augment-enhanced-prompt>enhanced prompt goes here</augment-enhanced-prompt>\n\n### END RESPONSE ###\n\nHere is my original instruction:\n\n"

// 请求头过滤：Host、Authorization、hop-by-hop 头以及反代相关头
var skipRequestHeaders = map[string]bool{
	"Host":              true,
	"Authorization":     true,
	"Content-Length":    true, // 由 Go http 库根据实际请求体自动计算
	"Connection":        true, // hop-by-hop
	"Keep-Alive":        true, // hop-by-hop
	"Te":                true, // hop-by-hop
	"Upgrade":           true, // hop-by-hop
	"Proxy-Connection":  true, // 非标准代理头
	"X-Forwarded-For":   true, // 反代相关
	"X-Forwarded-Proto": true,
	"X-Forwarded-Host":  true,
	"X-Forwarded-Port":  true,
	"X-Real-Ip":         true,
	"X-Original-Uri":    true,
	"Via":               true,
	// 由 relay 控制的头，忽略客户端传入的值
	"X-Request-Id":         true,
	"X-Request-Session-Id": true,
	"Sentry-Trace":         true,
	"Baggage":              true,
	"User-Agent":           true,
	// 由 relay 控制压缩策略，忽略客户端传入的编码头
	"Accept-Encoding":  true,
	"Content-Encoding": true,
}

// 响应头过滤：hop-by-hop 头，代理不应转发
var skipResponseHeaders = map[string]bool{
	"Connection":          true, // hop-by-hop
	"Keep-Alive":          true, // hop-by-hop
	"Transfer-Encoding":   true, // hop-by-hop
	"Te":                  true, // hop-by-hop
	"Trailer":             true, // hop-by-hop
	"Upgrade":             true, // hop-by-hop
	"Proxy-Authorization": true, // 代理认证相关
	"Proxy-Authenticate":  true,
	"Content-Length":      true, // 由 Gin 根据实际响应体大小自动设置
	"Content-Encoding":    true, // 响应体已被 http.Client 解码，避免不匹配
	"Alt-Svc":             true, // HTTP/3 替代服务声明，对代理无意义
}

// generateRandomHex 生成指定长度的随机十六进制字符串
func generateRandomHex(length int) string {
	randBytes := make([]byte, length/2)
	rand.Read(randBytes)
	return hex.EncodeToString(randBytes)
}

// 常用邮箱后缀
var emailDomains = []string{
	"gmail.com", "outlook.com", "hotmail.com", "yahoo.com",
	"icloud.com", "protonmail.com", "qq.com", "163.com",
}

// generateRandomEmail 生成随机邮箱地址
func generateRandomEmail() string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 7) // 6位用户名 + 1位选择域名
	rand.Read(b)
	username := make([]byte, 6)
	for i := 0; i < 6; i++ {
		username[i] = chars[int(b[i])%len(chars)]
	}
	domain := emailDomains[int(b[6])%len(emailDomains)]
	return fmt.Sprintf("%s@%s", username, domain)
}

// generateRandomTenantName 生成随机租户名称 (格式: dxx-discoveryx)
func generateRandomTenantName() string {
	b := make([]byte, 2)
	rand.Read(b)
	num1 := int(b[0]) % 21 // 0-20
	num2 := int(b[1]) % 10 // 0-9
	return fmt.Sprintf("d%d-discovery%d", num1, num2)
}

// 模拟 CLI/插件 的 User-Agent，每个 session 随机绑定一种
var fakeUserAgents = []string{
	"augment.cli/0.27.1 (commit c7a6130f)/interactive",
	"Augment.vscode-augment/0.859.7 (darwin; arm64; 25.2.0) vscode/1.120.0",
}

// sessionInfo 存储在 Redis 中的会话信息
type sessionInfo struct {
	SessionID string `json:"session_id"`
	UA        string `json:"ua"`
}

// getOrCreateSession 获取或创建用户会话，返回 session_id 和 User-Agent
// 会话在用户不活跃超过 sessionTTL 后自动过期，下次请求会创建新会话
func getOrCreateSession(ctx context.Context, userID string) (sessionID, ua string) {
	cacheKey := fmt.Sprintf("session:%s", userID)

	// 尝试获取已有会话
	val, err := redisClient.Get(ctx, cacheKey).Result()
	if err == nil {
		var info sessionInfo
		if json.Unmarshal([]byte(val), &info) == nil {
			// 刷新 TTL（活跃续期）
			redisClient.Expire(ctx, cacheKey, sessionTTL)
			return info.SessionID, info.UA
		}
	}

	// 创建新会话
	newSessionID := uuid.New().String()
	b := make([]byte, 1)
	rand.Read(b)
	newUA := fakeUserAgents[int(b[0])%len(fakeUserAgents)]

	info := sessionInfo{SessionID: newSessionID, UA: newUA}
	data, _ := json.Marshal(info)
	redisClient.Set(ctx, cacheKey, string(data), sessionTTL)

	return newSessionID, newUA
}

// sanitizeGetModelsResponse 对 /get-models 响应进行隐私处理
func sanitizeGetModelsResponse(respBody []byte) []byte {
	var data map[string]interface{}
	if err := json.Unmarshal(respBody, &data); err != nil {
		return respBody // 解析失败，返回原始响应
	}

	if user, ok := data["user"].(map[string]interface{}); ok {
		user["id"] = uuid.New().String()
		user["email"] = generateRandomEmail()
		user["tenant_id"] = generateRandomHex(32)
		user["tenant_name"] = generateRandomTenantName()
		// created_at 保留原值
	}

	sanitized, err := json.Marshal(data)
	if err != nil {
		return respBody // 编码失败，返回原始响应
	}
	return sanitized
}

var httpClient *http.Client
var sseHttpClient *http.Client

// upstreamProxyFunc 给定一个目标请求，返回应使用的代理 URL（nil 表示直连）。
// 与 httpClient.Transport.Proxy 共享同一份解析逻辑，供 health probe 复用。
var upstreamProxyFunc func(*http.Request) (*url.URL, error)

// initHTTPClients 根据配置初始化向上游请求的 HTTP 客户端。
// 若设置了 UPSTREAM_PROXY 则强制走该代理，否则回退到标准的
// HTTP_PROXY / HTTPS_PROXY / NO_PROXY 环境变量。
func initHTTPClients() {
	proxyFunc := http.ProxyFromEnvironment
	if upstreamProxy != "" {
		u, err := url.Parse(upstreamProxy)
		if err != nil || u.Scheme == "" || u.Host == "" {
			log.Printf("[PROXY] UPSTREAM_PROXY 无效，已忽略: %q (err=%v)", upstreamProxy, err)
		} else {
			proxyFunc = http.ProxyURL(u)
			log.Printf("[PROXY] 上游请求将通过代理: %s://%s", u.Scheme, u.Host)
		}
	}
	upstreamProxyFunc = proxyFunc

	httpClient = &http.Client{
		Transport: &http.Transport{
			Proxy:               proxyFunc,
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 20,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	sseHttpClient = &http.Client{
		Transport: &http.Transport{
			Proxy:               proxyFunc,
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 20,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true, // 禁用压缩以支持流式传输
		},
	}
}

// 全局数据库连接
var db *sql.DB

// 全局 Redis 客户端
var redisClient *redis.Client

// loadConfig 从 .env 文件加载配置
func loadConfig() {
	_ = godotenv.Load() // 忽略错误，允许使用环境变量

	serverAddr = getEnv("SERVER_ADDR", "127.0.0.1:8080")
	augmentAPIURL = getEnv("AUGMENT_API_URL", "")
	augmentAPIToken = getEnv("AUGMENT_API_TOKEN", "")
	dbHost = getEnv("DB_HOST", "localhost")
	dbPort = getEnvInt("DB_PORT", 5432)
	dbUser = getEnv("DB_USER", "postgres")
	dbPassword = getEnv("DB_PASSWORD", "")
	dbName = getEnv("DB_NAME", "postgres")
	redisPort = getEnvInt("REDIS_PORT", 6379)
	apiKeyCacheTTL = getEnvDuration("API_KEY_CACHE_TTL", 30*time.Minute)
	sessionTTL = getEnvDuration("SESSION_TTL", 5*time.Minute)
	upstreamProxy = getEnv("UPSTREAM_PROXY", "")
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

// initDB 初始化数据库连接
func initDB() error {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName)

	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		return err
	}

	// 配置连接池：避免频繁建连导致 SCRAM-SHA-256 认证消耗大量 CPU
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)

	if err = db.Ping(); err != nil {
		return err
	}

	// 自动迁移：创建 request_logs 表
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

	// 自动迁移：创建 leaderboard 表
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

		-- 部分索引：加速 codebase-retrieval 统计查询
		CREATE INDEX IF NOT EXISTS idx_request_logs_codebase_retrieval
			ON request_logs(user_id, request_timestamp)
			WHERE request_path = '/agents/codebase-retrieval';
	`)
	if err != nil {
		return fmt.Errorf("failed to migrate leaderboard table: %w", err)
	}

	// 自动迁移：创建 error_details 表
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

	// 自动迁移：创建 health_checks 表
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

// initRedis 初始化 Redis 连接
func initRedis() error {
	redisClient = redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("localhost:%d", redisPort),
		DB:   0,
	})
	_, err := redisClient.Ping(context.Background()).Result()
	if err != nil {
		return fmt.Errorf("failed to connect to Redis: %w", err)
	}
	return nil
}

// authenticateRequest 验证请求的 Authorization header，返回 user_id
// 如果验证失败返回空字符串和 false
func authenticateRequest(c *gin.Context) (string, bool) {
	authHeader := c.GetHeader("Authorization")
	if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
		return "", false
	}

	token := strings.TrimPrefix(authHeader, "Bearer ")
	hash := md5.Sum([]byte(token))
	tokenMD5 := hex.EncodeToString(hash[:])
	cacheKey := "apikey:" + tokenMD5

	// 1. 先查 Redis 缓存
	ctx := context.Background()
	if userID, err := redisClient.Get(ctx, cacheKey).Result(); err == nil {
		return userID, true
	}

	// 2. 缓存未命中，查数据库
	var userID string
	err := db.QueryRow("SELECT user_id FROM api_keys WHERE id = $1", tokenMD5).Scan(&userID)
	if err != nil {
		return "", false
	}

	// 3. 写入缓存
	redisClient.Set(ctx, cacheKey, userID, apiKeyCacheTTL)

	return userID, true
}

func authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 记录请求开始时间
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

		// 将 user_id 存入 context
		c.Set(ContextKeyUserID, userID)

		// 生成 UUID 并异步插入 pending 日志，用 channel 保证后续 UPDATE/FK 的时序
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

// RequestLogEntry 请求日志记录
type RequestLogEntry struct {
	LogID            string
	StatusCode       int
	ResponseDuration time.Duration
	InsertDone       <-chan struct{}
}

// completeRequestLogAsync 异步更新请求日志状态为已完成
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

// saveErrorDetailsAsync 异步保存错误详情到数据库
// source: "proxy" 表示本地转发服务错误，"upstream" 表示上游服务错误
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

// getInsertDone 从 Gin context 提取 INSERT 完成信号
func getInsertDone(c *gin.Context) <-chan struct{} {
	if v, ok := c.Get(ContextKeyInsertDone); ok {
		if ch, ok := v.(chan struct{}); ok {
			return ch
		}
	}
	return nil
}

// getRequestLogEntry 从 Gin context 提取日志数据
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

func validateChatStreamRequest(body []byte) error {
	var req ChatStreamRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("invalid JSON")
	}

	// 检查 mode 必须为 CHAT
	if req.Mode != "CHAT" {
		return fmt.Errorf("mode must be CHAT")
	}

	// 检查 system_prompt 必须为 null
	if req.SystemPrompt != nil {
		return fmt.Errorf("system_prompt must be null")
	}

	// 检查 message 必须以固定 prompt 开头（prompt-enhancer 功能）
	if !strings.HasPrefix(req.Message, PROMPT_ENHANCER_MESSAGE_PREFIX) {
		return fmt.Errorf("message must start with required prompt")
	}

	return nil
}

// ── 压缩相关 ──────────────────────────────────────────────────────────────

// compressMinBytes 低于此大小的 body 不压缩，避免小数据压缩后膨胀
const compressMinBytes = 1024

// brotliLevel brotli 压缩等级。
// 默认 DefaultCompression(=6) 在 profile 中占用 CPU 过高，
// 降到 4 后速度约 3-5x，压缩比仅低 5-10%。
const brotliLevel = 4

// encodingPriority 定义支持的编码及其优先级，数值越大越优先
var encodingPriority = map[string]int{
	"br":       4,
	"gzip":     3,
	"deflate":  2,
	"identity": 1,
}

// zstdEncoder 进程级全局 zstd encoder。EncodeAll 并发安全，
// klauspost/compress 内部已池化 encoder state，无需我们再加 sync.Pool。
// 用 SpeedFastest（接近 gzip-1 速度但压缩比更好），优先把 CPU 占用打下来。
// WithEncoderConcurrency(1) 避免每次调用 fork 多个 goroutine 并行压一个 body —
// 我们的并发已经在请求层面，再 fork 反而抢 CPU。
var zstdEncoder = func() *zstd.Encoder {
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(1),
	)
	if err != nil {
		log.Fatalf("[ZSTD] init encoder failed: %v", err)
	}
	return enc
}()

// compressBodyZstd 使用 zstd 压缩请求体。小于阈值的不压缩。
func compressBodyZstd(data []byte) []byte {
	if len(data) < compressMinBytes {
		return data
	}
	return zstdEncoder.EncodeAll(data, nil)
}

// negotiateEncoding 解析 Accept-Encoding 头，返回最佳匹配编码
// 优先级：br > gzip > deflate > identity
func negotiateEncoding(acceptEncoding string) string {
	if acceptEncoding == "" {
		return "identity"
	}

	type candidate struct {
		encoding string
		quality  float64
		priority int
	}

	// 记录显式列出的编码名，用于通配符展开
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

	// 通配符展开：将未显式列出的支持编码以通配符 quality 加入候选
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

// compressResponse 按指定编码压缩数据，失败时回退为 identity
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
		log.Printf("[COMPRESS] %s compression failed: %v, falling back to identity", encoding, err)
		return data, "identity"
	}
	return buf.Bytes(), encoding
}

func proxyHandler(c *gin.Context) {
	// 拦截 /record-request-events 和 /report-error，不转发到上游，避免被 trace
	if c.Request.URL.Path == "/record-request-events" || c.Request.URL.Path == "/report-error" {
		c.JSON(http.StatusOK, gin.H{})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))
		return
	}

	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "failed to read request body"})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusBadRequest))
		return
	}

	// 请求体 zstd 压缩（小于阈值不压缩）
	compressedBody := compressBodyZstd(body)
	useCompressedReq := len(body) >= compressMinBytes

	targetURL := augmentAPIURL + c.Request.URL.Path

	var reqBody []byte
	if useCompressedReq {
		reqBody = compressedBody
	} else {
		reqBody = body
	}
	req, err := http.NewRequestWithContext(c.Request.Context(), "POST", targetURL, bytes.NewReader(reqBody))
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "failed to create request"})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusInternalServerError))
		return
	}

	req.Header.Set("Authorization", "Bearer "+augmentAPIToken)

	for key, values := range c.Request.Header {
		if skipRequestHeaders[key] {
			continue
		}
		for _, v := range values {
			req.Header.Add(key, v)
		}
	}

	if useCompressedReq {
		req.Header.Set("Content-Encoding", "zstd")
	}

	// 注入模拟 CLI/插件 请求头
	sessionID, ua := getOrCreateSession(c.Request.Context(), c.GetString(ContextKeyUserID))
	req.Header.Set("X-Request-Id", uuid.New().String())
	req.Header.Set("X-Request-Session-Id", sessionID)
	req.Header.Set("User-Agent", ua)

	resp, err := httpClient.Do(req)
	if err != nil {
		if errors.Is(c.Request.Context().Err(), context.Canceled) {
			completeRequestLogAsync(getRequestLogEntry(c, 499))
			return
		}
		logID, _ := c.Get(ContextKeyLogID)
		logIDStr, _ := logID.(string)
		saveErrorDetailsAsync(logIDStr, "proxy", err.Error(), getInsertDone(c))
		c.AbortWithStatusJSON(http.StatusBadGateway, gin.H{"error": "failed to forward request"})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusBadGateway))
		return
	}
	defer resp.Body.Close()

	for key, values := range resp.Header {
		if skipResponseHeaders[key] {
			continue
		}
		for _, v := range values {
			c.Header(key, v)
		}
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "failed to read response body"})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusInternalServerError))
		return
	}

	if resp.StatusCode >= 400 {
		logID, _ := c.Get(ContextKeyLogID)
		logIDStr, _ := logID.(string)
		saveErrorDetailsAsync(logIDStr, "upstream", string(respBody), getInsertDone(c))
	}

	// 对 /get-models 成功响应进行隐私处理
	if c.Request.URL.Path == "/get-models" && resp.StatusCode == http.StatusOK {
		respBody = sanitizeGetModelsResponse(respBody)
	}

	// 根据客户端 Accept-Encoding 压缩响应体
	chosenEncoding := negotiateEncoding(c.GetHeader("Accept-Encoding"))
	respBody, actualEncoding := compressResponse(respBody, chosenEncoding)
	if actualEncoding != "identity" {
		c.Header("Content-Encoding", actualEncoding)
		c.Header("Vary", "Accept-Encoding")
	}

	c.Data(resp.StatusCode, resp.Header.Get("Content-Type"), respBody)
	completeRequestLogAsync(getRequestLogEntry(c, resp.StatusCode))
}

func sseProxyHandler(c *gin.Context) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "failed to read request body"})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusBadRequest))
		return
	}

	// 验证 /chat-stream 请求内容
	if c.FullPath() == "/chat-stream" {
		if err = validateChatStreamRequest(body); err != nil {
			logID, _ := c.Get(ContextKeyLogID)
			logIDStr, _ := logID.(string)
			saveErrorDetailsAsync(logIDStr, "proxy", err.Error(), getInsertDone(c))
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "request validation failed"})
			completeRequestLogAsync(getRequestLogEntry(c, http.StatusForbidden))
			return
		}
	}

	targetURL := augmentAPIURL + c.Request.URL.Path

	req, err := http.NewRequestWithContext(c.Request.Context(), "POST", targetURL, bytes.NewReader(body))
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "failed to create request"})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusInternalServerError))
		return
	}

	req.Header.Set("Authorization", "Bearer "+augmentAPIToken)

	for key, values := range c.Request.Header {
		if skipRequestHeaders[key] {
			continue
		}
		for _, v := range values {
			req.Header.Add(key, v)
		}
	}

	// 注入模拟 CLI/插件 请求头
	sessionID, ua := getOrCreateSession(c.Request.Context(), c.GetString(ContextKeyUserID))
	req.Header.Set("X-Request-Id", uuid.New().String())
	req.Header.Set("X-Request-Session-Id", sessionID)
	req.Header.Set("User-Agent", ua)

	resp, err := sseHttpClient.Do(req)
	if err != nil {
		if errors.Is(c.Request.Context().Err(), context.Canceled) {
			completeRequestLogAsync(getRequestLogEntry(c, 499))
			return
		}
		logID, _ := c.Get(ContextKeyLogID)
		logIDStr, _ := logID.(string)
		saveErrorDetailsAsync(logIDStr, "proxy", err.Error(), getInsertDone(c))
		c.AbortWithStatusJSON(http.StatusBadGateway, gin.H{"error": "failed to forward request: " + err.Error()})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusBadGateway))
		return
	}
	defer resp.Body.Close()

	// 上游返回错误状态码时，直接转发错误响应（非流式）
	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		logID, _ := c.Get(ContextKeyLogID)
		logIDStr, _ := logID.(string)
		saveErrorDetailsAsync(logIDStr, "upstream", string(respBody), getInsertDone(c))
		c.Data(resp.StatusCode, resp.Header.Get("Content-Type"), respBody)
		completeRequestLogAsync(getRequestLogEntry(c, resp.StatusCode))
		return
	}

	// 透传上游响应头（过滤 hop-by-hop 头）
	for key, values := range resp.Header {
		if skipResponseHeaders[key] {
			continue
		}
		for _, v := range values {
			c.Header(key, v)
		}
	}

	// 写入响应头
	c.Writer.WriteHeaderNow()

	// 获取 Flusher 接口以支持实时刷新
	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))
		return
	}

	// 流式读取并转发
	reader := bufio.NewReader(resp.Body)
	for {
		select {
		case <-c.Request.Context().Done():
			completeRequestLogAsync(getRequestLogEntry(c, 499))
			return
		default:
			line, err := reader.ReadBytes('\n')
			if err != nil {
				completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))
				return
			}

			_, writeErr := c.Writer.Write(line)
			if writeErr != nil {
				completeRequestLogAsync(getRequestLogEntry(c, 499))
				return
			}

			flusher.Flush()
		}
	}
}

// updateLeaderboard 统计当天 codebase-retrieval 请求并更新排行榜
func updateLeaderboard() error {
	// 加载 Asia/Shanghai 时区
	loc, err := time.LoadLocation(LeaderboardTimezone)
	if err != nil {
		return fmt.Errorf("failed to load timezone: %w", err)
	}

	// 获取当天日期范围（Asia/Shanghai）
	now := time.Now().In(loc)
	dateStr := now.Format("2006-01-02")
	dayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc)
	dayEnd := dayStart.Add(24 * time.Hour)

	log.Printf("[LEADERBOARD] Updating leaderboard for %s", dateStr)

	// 查询当天 top N 用户（只统计 status_code=200 的成功请求）
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

	// 收集结果
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

	// 开始事务
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// UPSERT 每条记录
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

// startLeaderboardScheduler 启动排行榜定时更新任务
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

// ==================== Health Check ====================

const (
	healthTestBlobName    = "676dfa60a4fccdf3b949a93f4cc09d0cd6aba4fb0f97f03617ae31a48c3c5ce0"
	healthTestBlobContent = "#include <stdio.h>\nint main() {\n   printf(\"Hello, World!\");\n   return 0;\n}\n"
)

func healthProbeHeaders() http.Header {
	b := make([]byte, 1)
	rand.Read(b)
	ua := fakeUserAgents[int(b[0])%len(fakeUserAgents)]

	h := http.Header{}
	h.Set("Content-Type", "application/json")
	h.Set("User-Agent", ua)
	h.Set("X-Request-Id", uuid.New().String())
	h.Set("X-Request-Session-Id", uuid.New().String())
	h.Set("Authorization", "Bearer "+augmentAPIToken)
	return h
}

// dialUpstreamForPing 建立到上游 hostPort 的 TCP 连接，必要时穿过 proxyURL 指定的代理。
// 用于健康检查测量"本地→（代理→）上游"的完整握手 RTT。调用方负责 Close 返回的 conn。
// 仅支持 socks5 / socks5h / http 代理；https-proxy（到代理本身走 TLS）暂不支持。
func dialUpstreamForPing(ctx context.Context, upstreamHostPort string, proxyURL *url.URL) (net.Conn, error) {
	dialer := &net.Dialer{}
	if proxyURL == nil {
		return dialer.DialContext(ctx, "tcp", upstreamHostPort)
	}
	switch strings.ToLower(proxyURL.Scheme) {
	case "socks5", "socks5h":
		var auth *proxy.Auth
		if proxyURL.User != nil {
			pass, _ := proxyURL.User.Password()
			auth = &proxy.Auth{User: proxyURL.User.Username(), Password: pass}
		}
		d, err := proxy.SOCKS5("tcp", proxyURL.Host, auth, dialer)
		if err != nil {
			return nil, err
		}
		if cd, ok := d.(proxy.ContextDialer); ok {
			return cd.DialContext(ctx, "tcp", upstreamHostPort)
		}
		return d.Dial("tcp", upstreamHostPort)
	case "http":
		return dialViaHTTPConnect(ctx, dialer, upstreamHostPort, proxyURL)
	default:
		return nil, fmt.Errorf("unsupported proxy scheme for tcp-ping: %q", proxyURL.Scheme)
	}
}

// dialViaHTTPConnect 通过 HTTP CONNECT 代理建立到 target 的隧道连接。
func dialViaHTTPConnect(ctx context.Context, dialer *net.Dialer, target string, proxyURL *url.URL) (net.Conn, error) {
	proxyHost := proxyURL.Host
	if !strings.Contains(proxyHost, ":") {
		proxyHost += ":80"
	}
	conn, err := dialer.DialContext(ctx, "tcp", proxyHost)
	if err != nil {
		return nil, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n", target, target)
	if proxyURL.User != nil {
		pass, _ := proxyURL.User.Password()
		token := base64.StdEncoding.EncodeToString([]byte(proxyURL.User.Username() + ":" + pass))
		fmt.Fprintf(&sb, "Proxy-Authorization: Basic %s\r\n", token)
	}
	sb.WriteString("\r\n")
	if _, err := conn.Write([]byte(sb.String())); err != nil {
		conn.Close()
		return nil, err
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err != nil {
		conn.Close()
		return nil, err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		conn.Close()
		return nil, fmt.Errorf("proxy CONNECT %s: %s", target, resp.Status)
	}
	_ = conn.SetDeadline(time.Time{})
	return conn, nil
}

func runHealthProbe() {
	ctx, cancel := context.WithTimeout(context.Background(), HealthCheckTimeout)
	defer cancel()

	var tcpPingMs, codebaseRetrievalMs sql.NullInt64
	var errMsg sql.NullString
	status := "success"

	defer func() {
		nextCheckAt := time.Now().Add(HealthCheckInterval)
		_, dbErr := db.Exec(
			`INSERT INTO health_checks (status, tcp_ping_ms, codebase_retrieval_ms, error_message, next_check_at)
			 VALUES ($1, $2, $3, $4, $5)`,
			status, tcpPingMs, codebaseRetrievalMs, errMsg, nextCheckAt,
		)
		if dbErr != nil {
			log.Printf("[HEALTH] Failed to save result: %v", dbErr)
		}
	}()

	base := strings.TrimRight(augmentAPIURL, "/")

	// Step 0: TCP ping
	// 测的是建立到"上游 IP"的 TCP 连接所需时间。
	// 若配置了代理，会穿过代理（SOCKS5/HTTP CONNECT）建立隧道，时延 ≈
	// 本地→代理 RTT + 代理→上游 RTT。本地代理的第一段 <1ms 是噪声，
	// 主体就是代理到上游的真实握手 RTT。
	u, _ := url.Parse(augmentAPIURL)
	tcpHost := u.Host
	if !strings.Contains(tcpHost, ":") {
		if u.Scheme == "https" {
			tcpHost += ":443"
		} else {
			tcpHost += ":80"
		}
	}
	var proxyURL *url.URL
	if upstreamProxyFunc != nil {
		fakeReq, _ := http.NewRequest("GET", augmentAPIURL, nil)
		if pu, perr := upstreamProxyFunc(fakeReq); perr == nil && pu != nil {
			proxyURL = pu
		}
	}
	t0 := time.Now()
	conn, err := dialUpstreamForPing(ctx, tcpHost, proxyURL)
	tcpPingMs = sql.NullInt64{Int64: time.Since(t0).Milliseconds(), Valid: true}
	if err != nil {
		status = "error"
		errMsg = sql.NullString{String: "tcp-ping: " + err.Error(), Valid: true}
		return
	}
	conn.Close()

	// Step 1: find-missing
	findBody, _ := json.Marshal(map[string]interface{}{
		"model":            "",
		"mem_object_names": []string{healthTestBlobName},
	})

	req1, _ := http.NewRequestWithContext(ctx, "POST", base+"/find-missing", bytes.NewReader(findBody))
	req1.Header = healthProbeHeaders()
	resp1, err := httpClient.Do(req1)

	if err != nil {
		status = "error"
		errMsg = sql.NullString{String: "find-missing: " + err.Error(), Valid: true}
		return
	}
	defer resp1.Body.Close()

	if resp1.StatusCode != http.StatusOK {
		status = "error"
		errMsg = sql.NullString{String: fmt.Sprintf("find-missing returned %d", resp1.StatusCode), Valid: true}
		return
	}

	var findResult struct {
		UnknownMemoryNames []string `json:"unknown_memory_names"`
	}
	json.NewDecoder(resp1.Body).Decode(&findResult)

	// Step 2: batch-upload if missing
	if len(findResult.UnknownMemoryNames) > 0 {
		uploadBody, _ := json.Marshal(map[string]interface{}{
			"blobs": []map[string]string{{
				"blob_name": healthTestBlobName,
				"path":      "main.c",
				"content":   healthTestBlobContent,
			}},
		})
		req2, _ := http.NewRequestWithContext(ctx, "POST", base+"/batch-upload", bytes.NewReader(uploadBody))
		req2.Header = healthProbeHeaders()
		resp2, err := httpClient.Do(req2)
		if err == nil {
			resp2.Body.Close()
		}
	}

	// Step 3: codebase-retrieval
	retBody, _ := json.Marshal(map[string]interface{}{
		"information_request":          "Find the main function or main entry point",
		"blobs":                        map[string]interface{}{"checkpoint_id": nil, "added_blobs": []string{healthTestBlobName}, "deleted_blobs": []string{}},
		"dialog":                       []interface{}{},
		"max_output_length":            0,
		"disable_codebase_retrieval":   false,
		"enable_commit_retrieval":      false,
		"enable_conversation_retrieval": false,
	})

	t2 := time.Now()
	req3, _ := http.NewRequestWithContext(ctx, "POST", base+"/agents/codebase-retrieval", bytes.NewReader(retBody))
	req3.Header = healthProbeHeaders()
	resp3, err := httpClient.Do(req3)
	codebaseRetrievalMs = sql.NullInt64{Int64: time.Since(t2).Milliseconds(), Valid: true}

	if err != nil {
		status = "error"
		errMsg = sql.NullString{String: "codebase-retrieval: " + err.Error(), Valid: true}
		return
	}
	defer resp3.Body.Close()

	if resp3.StatusCode != http.StatusOK {
		status = "error"
		errMsg = sql.NullString{String: fmt.Sprintf("codebase-retrieval returned %d", resp3.StatusCode), Valid: true}
		return
	}

	log.Printf("[HEALTH] Probe OK: tcp-ping=%dms, codebase-retrieval=%dms",
		tcpPingMs.Int64, codebaseRetrievalMs.Int64)
}

// startHealthScheduler 启动健康检查定时任务
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

func main() {
	// 加载配置
	loadConfig()

	// 初始化 HTTP 客户端（含上游代理配置）
	initHTTPClients()

	// 设置日志同时输出到控制台和文件
	logFile, err := os.OpenFile("gin.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		panic("无法创建日志文件: " + err.Error())
	}
	gin.DefaultWriter = io.MultiWriter(os.Stdout, logFile)
	gin.DefaultErrorWriter = io.MultiWriter(os.Stderr, logFile)
	log.SetOutput(io.MultiWriter(os.Stdout, logFile))

	// 初始化数据库连接
	if err := initDB(); err != nil {
		log.Fatalf("无法连接数据库: %v", err)
	}
	defer db.Close()

	// 初始化 Redis 连接
	if err := initRedis(); err != nil {
		log.Fatalf("无法连接 Redis: %v", err)
	}
	defer redisClient.Close()

	// 启动时执行一次 leaderboard 统计
	log.Println("[LEADERBOARD] Running initial statistics...")
	if err := updateLeaderboard(); err != nil {
		log.Printf("[LEADERBOARD] Initial update failed: %v", err)
	}

	// 启动 leaderboard 定时任务
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go startLeaderboardScheduler(ctx)

	// 启动 health check 定时任务
	log.Println("[HEALTH] Starting health scheduler...")
	go startHealthScheduler(ctx)

	// 启动 pprof server（独立 mux，仅本地访问）
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

	for _, path := range allowedPaths {
		r.POST(path, proxyHandler)
	}

	// 注册 SSE 流式路由（暂时禁用，保留代码）
	// for _, path := range ssePaths {
	// 	r.POST(path, sseProxyHandler)
	// }

	// 处理 404 路由不匹配
	// 注意：authMiddleware 已经为认证成功的请求创建了 pending 日志，这里只需更新状态
	r.NoRoute(func(c *gin.Context) {
		// 如果 authMiddleware 已经创建了日志记录，更新其状态为 404
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusNotFound))
		c.JSON(http.StatusNotFound, gin.H{"error": "route not found"})
	})

	r.Run(serverAddr)
}
