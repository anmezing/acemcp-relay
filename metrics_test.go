package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestMetricsMiddlewareCountsAndLabels 断言中间件按归一化路由模板计数，
// 并且 handler 精化的 metrics_path（工具调用）优先于路由模板。
func TestMetricsMiddlewareCountsAndLabels(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(metricsMiddleware())
	r.POST("/mm-test/:id", func(c *gin.Context) {
		c.Status(http.StatusTeapot)
	})
	r.POST("/mm-mcp", func(c *gin.Context) {
		c.Set(ContextKeyMetricsPath, "/mm-mcp/tools/call/codebase-retrieval")
		c.Status(http.StatusOK)
	})
	r.NoRoute(func(c *gin.Context) {
		c.Status(http.StatusNotFound)
	})

	do := func(path string) {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("POST", path, nil))
	}
	do("/mm-test/abc")
	do("/mm-test/def")
	do("/mm-mcp")
	do("/mm-nowhere")

	cases := []struct {
		path, method, status string
		want                 float64
	}{
		// path 是路由模板而非原始 URL：两个不同 id 落在同一条时间序列上。
		{"/mm-test/:id", "POST", "418", 2},
		{"/mm-mcp/tools/call/codebase-retrieval", "POST", "200", 1},
		{"unmatched", "POST", "404", 1},
	}
	for _, tc := range cases {
		got := testutil.ToFloat64(metricHTTPRequests.WithLabelValues(tc.path, tc.method, tc.status))
		if got != tc.want {
			t.Errorf("relay_http_requests_total{path=%q,method=%q,status=%q} = %v, want %v",
				tc.path, tc.method, tc.status, got, tc.want)
		}
	}
	// 原始 URL 不应成为标签。
	if got := testutil.ToFloat64(metricHTTPRequests.WithLabelValues("/mm-test/abc", "POST", "418")); got != 0 {
		t.Errorf("raw URL leaked into path label: %v", got)
	}

	// duration histogram 与 counter 同口径。
	if got := testutil.CollectAndCount(metricHTTPDuration, "relay_http_request_duration_seconds"); got < 3 {
		t.Errorf("expected at least 3 duration series, got %d", got)
	}
}

func TestMetricsIndexBytesCounter(t *testing.T) {
	before := testutil.ToFloat64(metricIndexBytes)
	metricIndexBytes.Add(1234)
	after := testutil.ToFloat64(metricIndexBytes)
	if after-before != 1234 {
		t.Errorf("relay_index_bytes_total delta = %v, want 1234", after-before)
	}
}

func TestMetricsEndpointServesRegistry(t *testing.T) {
	metricHTTPRequests.WithLabelValues("/mm-endpoint", "GET", "200").Inc()
	w := httptest.NewRecorder()
	metricsHandler().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("/metrics returned %d", w.Code)
	}
	body := w.Body.String()
	for _, name := range []string{
		"relay_http_requests_total",
		"relay_http_request_duration_seconds",
		"relay_index_bytes_total",
	} {
		if !strings.Contains(body, name) {
			t.Errorf("/metrics output missing %s", name)
		}
	}
}
