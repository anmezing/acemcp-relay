package main

// ── Prometheus 指标 ─────────────────────────────────────────────────────────
//
// 基数纪律：
//   - path 标签只用归一化路由模板（gin FullPath 或 /mcp/tools/call/<tool>），
//     工具名来自 chatMCPToolPolicies 白名单 + codebase-index，是有界集合。
//   - relay_index_bytes_total 不带 user_id 标签：api key 用户方向上无上界，
//     每用户一条时间序列会随注册量线性膨胀，因此只保留全局累计值；
//     按用户的字节明细已有 Redis 配额计数（quota:indexbytes:*）可查。

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ContextKeyMetricsPath 允许 handler 把归一化路径细化后回传给 metrics 中间件
// （如 /mcp → /mcp/tools/call/<tool>）；请求日志完成更新复用同一归一化路径。
const ContextKeyMetricsPath = "metrics_path"

var metricsRegistry = prometheus.NewRegistry()

var (
	metricHTTPRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "relay_http_requests_total",
		Help: "Total HTTP requests handled by the relay, by normalized route template.",
	}, []string{"path", "method", "status"})

	metricHTTPDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "relay_http_request_duration_seconds",
		Help:    "HTTP request duration in seconds, by normalized route template.",
		Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300},
	}, []string{"path"})

	metricVersionGate = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "relay_client_version_gate_total",
		Help: "Client version gate outcomes (pass or reject_426).",
	}, []string{"result"})

	metricLCEDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "relay_lce_upstream_duration_seconds",
		Help:    "Duration of upstream LCE tools/call invocations in seconds.",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300},
	}, []string{"tool"})

	metricLCEErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "relay_lce_upstream_errors_total",
		Help: "Failed upstream LCE tools/call invocations (transport/RPC errors).",
	}, []string{"tool"})

	// 全局 counter，不带 user_id：见文件头部的基数说明。
	metricIndexBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "relay_index_bytes_total",
		Help: "Total index upload bytes accepted by the daily quota gate (global; per-user detail lives in Redis quota keys).",
	})
)

func init() {
	metricsRegistry.MustRegister(
		metricHTTPRequests,
		metricHTTPDuration,
		metricVersionGate,
		metricLCEDuration,
		metricLCEErrors,
		metricIndexBytes,
	)
}

// metricsPathLabel 返回归一化路径标签，绝不返回原始 URL：
// handler 精化值 > gin 路由模板 > "unmatched"（NoRoute 落到这里，防基数爆炸）。
func metricsPathLabel(c *gin.Context) string {
	if v, ok := c.Get(ContextKeyMetricsPath); ok {
		if s, ok := v.(string); ok && s != "" {
			return s
		}
	}
	if fp := c.FullPath(); fp != "" {
		return fp
	}
	return "unmatched"
}

func metricsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		path := metricsPathLabel(c)
		metricHTTPRequests.WithLabelValues(path, c.Request.Method, strconv.Itoa(c.Writer.Status())).Inc()
		metricHTTPDuration.WithLabelValues(path).Observe(time.Since(start).Seconds())
	}
}

func metricsHandler() http.Handler {
	return promhttp.HandlerFor(metricsRegistry, promhttp.HandlerOpts{})
}

// evaluateVersionGate 把版本门禁判定收敛成纯函数：返回 ""（门禁未启用或
// 客户端未上报版本，不计数）、"pass" 或 "reject_426"。
func evaluateVersionGate(clientVersion string) string {
	return evaluateVersionGateAgainst(clientVersion, currentMinClientVersion())
}

func evaluateVersionGateAgainst(clientVersion, minimumVersion string) string {
	if minimumVersion == "" || clientVersion == "" {
		return ""
	}
	if compareVersions(clientVersion, minimumVersion) < 0 {
		return "reject_426"
	}
	return "pass"
}

func recordVersionGate(result string) {
	if result != "" {
		metricVersionGate.WithLabelValues(result).Inc()
	}
}

func observeLCECall(tool string, start time.Time, err error) {
	metricLCEDuration.WithLabelValues(tool).Observe(time.Since(start).Seconds())
	if err != nil {
		metricLCEErrors.WithLabelValues(tool).Inc()
	}
}
