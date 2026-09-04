package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

type signalFlushWriter struct {
	*httptest.ResponseRecorder
	flushed chan struct{}
	once    sync.Once
}

func (w *signalFlushWriter) Flush() {
	w.ResponseRecorder.Flush()
	select {
	case w.flushed <- struct{}{}:
	default:
	}
}

func TestMigrateErrorDetailsTableEnforcesCascadeDelete(t *testing.T) {
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()
	previousDB := db
	db = mockDB
	defer func() { db = previousDB }()

	mock.ExpectExec(`(?s)CREATE TABLE IF NOT EXISTS error_details .*REFERENCES request_logs\(id\) ON DELETE CASCADE.*DO \$\$.*confdeltype.*ON DELETE CASCADE`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	if err := migrateErrorDetailsTable(); err != nil {
		t.Fatalf("migrateErrorDetailsTable() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestPersistRequestLogCompletionWritesErrorBeforeCompletingParent(t *testing.T) {
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()
	previousDB := db
	db = mockDB
	defer func() { db = previousDB }()

	entry := RequestLogEntry{
		LogID:            "request-with-error",
		StatusCode:       http.StatusInternalServerError,
		ResponseDuration: 1250 * time.Millisecond,
		RequestPath:      "/mcp/tenant-stats",
	}
	mock.ExpectBegin()
	mock.ExpectExec(`(?s)INSERT INTO error_details .*VALUES \(\$1, \$2, \$3\)`).
		WithArgs(entry.LogID, "lce", "connection reset by peer").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`(?s)UPDATE request_logs.*status = \$1.*status_code = \$2.*response_duration_ms = \$3.*request_path = CASE.*WHERE id = \$5`).
		WithArgs(StatusCompleted, http.StatusInternalServerError, int64(1250), entry.RequestPath, entry.LogID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	updated, err := persistRequestLogCompletion(entry, &requestLogErrorDetail{
		Source:  "lce",
		Message: "connection reset by peer",
	})
	if err != nil {
		t.Fatalf("persistRequestLogCompletion() error = %v", err)
	}
	if !updated {
		t.Fatal("persistRequestLogCompletion() updated = false, want true")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestPersistRequestLogCompletionRollsBackWhenErrorDetailFails(t *testing.T) {
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()
	previousDB := db
	db = mockDB
	defer func() { db = previousDB }()

	entry := RequestLogEntry{LogID: "request-with-error", StatusCode: http.StatusOK}
	mock.ExpectBegin()
	mock.ExpectExec(`(?s)INSERT INTO error_details`).
		WithArgs(entry.LogID, "relay", "write failed").
		WillReturnError(context.DeadlineExceeded)
	mock.ExpectRollback()

	updated, err := persistRequestLogCompletion(entry, &requestLogErrorDetail{Source: "relay", Message: "write failed"})
	if err == nil {
		t.Fatal("persistRequestLogCompletion() error = nil, want insert failure")
	}
	if updated {
		t.Fatal("persistRequestLogCompletion() updated = true, want false")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestDeleteCompletedUserLogsBeforeUsesStableBoundary(t *testing.T) {
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()
	previousDB := db
	db = mockDB
	defer func() { db = previousDB }()

	before := time.Date(2026, time.September, 4, 10, 30, 0, 0, time.UTC)
	mock.ExpectExec(`(?s)DELETE FROM request_logs.*user_id = \$1.*request_timestamp < \$2.*status = \$3`).
		WithArgs("user-1", before, StatusCompleted).
		WillReturnResult(sqlmock.NewResult(0, 7))

	rows, err := deleteCompletedUserLogsBefore("user-1", before)
	if err != nil {
		t.Fatalf("deleteCompletedUserLogsBefore() error = %v", err)
	}
	if rows != 7 {
		t.Fatalf("deleted rows = %d, want 7", rows)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestHandleMCPGetStreamsSSE(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const sessionID = "sse-request-log-test"
	serverSessionsMu.Lock()
	serverSessions[sessionID] = &mcpServerSession{userID: "user-1", lastActivity: time.Now()}
	serverSessionsMu.Unlock()
	defer func() {
		serverSessionsMu.Lock()
		delete(serverSessions, sessionID)
		serverSessionsMu.Unlock()
		closeMCPSSEStream(sessionID)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	response := &signalFlushWriter{
		ResponseRecorder: httptest.NewRecorder(),
		flushed:          make(chan struct{}, 4),
	}
	c, _ := gin.CreateTestContext(response)
	c.Request = httptest.NewRequest(http.MethodGet, "/mcp", nil).WithContext(ctx)
	c.Request.Header.Set("Accept", "text/event-stream")
	c.Request.Header.Set("Mcp-Session-Id", sessionID)
	c.Set(ContextKeyStartTime, time.Now())
	c.Set(ContextKeyUserID, "user-1")

	done := make(chan struct{})
	go func() {
		handleMCPGet(c)
		close(done)
	}()

	select {
	case <-response.flushed:
	case <-time.After(time.Second):
		t.Fatal("SSE handler did not flush the connected event")
	}
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	if got := response.Header().Get("Content-Type"); got != "text/event-stream; charset=utf-8" {
		t.Fatalf("Content-Type = %q, want text/event-stream; charset=utf-8", got)
	}
	if got := response.Body.String(); !strings.Contains(got, ": connected\n\n") {
		t.Fatalf("initial SSE comment missing from body: %q", got)
	}

	if !publishMCPSSEEvent(sessionID, []byte(`{"jsonrpc":"2.0","method":"notifications/test"}`)) {
		t.Fatal("publishMCPSSEEvent returned false for an active stream")
	}
	select {
	case <-response.flushed:
	case <-time.After(time.Second):
		t.Fatal("SSE handler did not flush the published event")
	}
	if got := response.Body.String(); !strings.Contains(got, "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/test\"}") {
		t.Fatalf("published SSE event missing from body: %q", got)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("SSE handler did not stop after request cancellation")
	}
}

func TestHandleMCPGetCompletesRequestLogOnDisconnect(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()
	previousDB := db
	db = mockDB
	defer func() { db = previousDB }()

	const (
		sessionID = "sse-request-log-completion-test"
		logID     = "sse-request-log-entry"
	)
	serverSessionsMu.Lock()
	serverSessions[sessionID] = &mcpServerSession{userID: "user-1", lastActivity: time.Now()}
	serverSessionsMu.Unlock()
	defer func() {
		serverSessionsMu.Lock()
		delete(serverSessions, sessionID)
		serverSessionsMu.Unlock()
		closeMCPSSEStream(sessionID)
	}()

	// A long-lived SSE request must not remain pending forever after the client
	// disconnects. The handler should complete it with the existing 499 convention.
	mock.ExpectExec("UPDATE request_logs").
		WithArgs(StatusCompleted, 499, sqlmock.AnyArg(), "/mcp/session/listen", logID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	ctx, cancel := context.WithCancel(context.Background())
	response := &signalFlushWriter{
		ResponseRecorder: httptest.NewRecorder(),
		flushed:          make(chan struct{}, 2),
	}
	c, _ := gin.CreateTestContext(response)
	c.Request = httptest.NewRequest(http.MethodGet, "/mcp", nil).WithContext(ctx)
	c.Request.Header.Set("Accept", "text/event-stream")
	c.Request.Header.Set("Mcp-Session-Id", sessionID)
	c.Set(ContextKeyStartTime, time.Now())
	c.Set(ContextKeyLogID, logID)
	c.Set(ContextKeyInsertDone, make(chan struct{}))
	close(c.MustGet(ContextKeyInsertDone).(chan struct{}))
	c.Set(ContextKeyUserID, "user-1")

	done := make(chan struct{})
	go func() {
		handleMCPGet(c)
		close(done)
	}()
	select {
	case <-response.flushed:
	case <-time.After(time.Second):
		t.Fatal("SSE handler did not flush before disconnect test")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("SSE handler did not stop after cancellation")
	}

	deadline := time.Now().Add(time.Second)
	for {
		err = mock.ExpectationsWereMet()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("request log completion expectation not met: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
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
	c.Request = httptest.NewRequest(http.MethodGet, "/mcp", nil)
	c.Request.Header.Set("Accept", "text/event-stream")
	c.Set(ContextKeyStartTime, time.Now())

	handleMCPGet(c)

	entry := getRequestLogEntry(c, http.StatusBadRequest)
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
