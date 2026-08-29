package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestHandleMCPGetReturnsMethodNotAllowed(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/mcp", func(c *gin.Context) {
		// No database is needed here: an absent log id makes the completion helper
		// a no-op while the route contract remains identical to production.
		c.Set(ContextKeyStartTime, time.Now())
		handleMCPGet(c)
	})

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/mcp", nil)
	router.ServeHTTP(response, request)

	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusMethodNotAllowed)
	}
	if got := response.Header().Get("Allow"); got != "POST, DELETE" {
		t.Fatalf("Allow = %q, want %q", got, "POST, DELETE")
	}
}

func TestNormalizedMCPRequestPath(t *testing.T) {
	tests := []struct {
		name string
		rpc  jsonRPCRequest
		want string
	}{
		{name: "initialize", rpc: jsonRPCRequest{Method: "initialize"}, want: "/mcp/initialize"},
		{name: "initialized notification", rpc: jsonRPCRequest{Method: "notifications/initialized"}, want: "/mcp/notifications/initialized"},
		{name: "tools list", rpc: jsonRPCRequest{Method: "tools/list"}, want: "/mcp/tools/list"},
		{
			name: "allowed tool call",
			rpc: jsonRPCRequest{
				Method: "tools/call",
				Params: json.RawMessage(`{"name":" codebase-retrieval "}`),
			},
			want: "/mcp/tools/call/codebase-retrieval",
		},
		{
			name: "unknown tool remains bounded",
			rpc: jsonRPCRequest{
				Method: "tools/call",
				Params: json.RawMessage(`{"name":"arbitrary-user-value"}`),
			},
			want: "/mcp/tools/call",
		},
		{name: "unknown rpc method remains bounded", rpc: jsonRPCRequest{Method: "custom/method"}, want: "/mcp"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizedMCPRequestPath(tt.rpc); got != tt.want {
				t.Fatalf("normalizedMCPRequestPath() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestGetRequestLogEntryCapturesNormalizedPath(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Set(ContextKeyStartTime, time.Now())
	c.Set(ContextKeyLogID, "request-1")
	c.Set(ContextKeyMetricsPath, "/mcp/tools/call/codebase-retrieval")

	entry := getRequestLogEntry(c, http.StatusOK)
	if entry.LogID != "request-1" {
		t.Fatalf("LogID = %q, want %q", entry.LogID, "request-1")
	}
	if entry.RequestPath != "/mcp/tools/call/codebase-retrieval" {
		t.Fatalf("RequestPath = %q, want %q", entry.RequestPath, "/mcp/tools/call/codebase-retrieval")
	}
}

func TestHandleMCPGetSetsNormalizedLogPath(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Set(ContextKeyStartTime, time.Now())

	handleMCPGet(c)

	entry := getRequestLogEntry(c, http.StatusMethodNotAllowed)
	if entry.RequestPath != "/mcp/session/listen" {
		t.Fatalf("RequestPath = %q, want %q", entry.RequestPath, "/mcp/session/listen")
	}
}

func TestHandleMCPPostExpiredSessionRecordsActualToolPath(t *testing.T) {
	const expiredSessionID = "expired-session-for-request-log-test"
	serverSessionsMu.Lock()
	delete(serverSessions, expiredSessionID)
	serverSessionsMu.Unlock()

	response := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(response)
	c.Request = httptest.NewRequest(
		http.MethodPost,
		"/mcp",
		strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"codebase-retrieval","arguments":{"information_request":"test"}}}`),
	)
	c.Request.Header.Set("Mcp-Session-Id", expiredSessionID)
	c.Set(ContextKeyStartTime, time.Now())
	c.Set(ContextKeyUserID, "user-1")

	handleMCPPost(c)

	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusNotFound)
	}
	entry := getRequestLogEntry(c, response.Code)
	if entry.RequestPath != "/mcp/tools/call/codebase-retrieval" {
		t.Fatalf("RequestPath = %q, want %q", entry.RequestPath, "/mcp/tools/call/codebase-retrieval")
	}
}

func TestHandleMCPDeleteSetsNormalizedLogPath(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodDelete, "/mcp", nil)
	c.Set(ContextKeyStartTime, time.Now())

	handleMCPDelete(c)

	entry := getRequestLogEntry(c, http.StatusBadRequest)
	if entry.RequestPath != "/mcp/session/terminate" {
		t.Fatalf("RequestPath = %q, want %q", entry.RequestPath, "/mcp/session/terminate")
	}
}
