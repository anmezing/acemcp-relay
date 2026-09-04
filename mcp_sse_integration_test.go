package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func newMCPSSERelayTestRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set(ContextKeyUserID, "sse-integration-user")
		c.Next()
	})
	router.POST("/mcp", handleMCPPost)
	router.DELETE("/mcp", handleMCPDelete)
	router.GET("/mcp", handleMCPGet)
	return router
}

func seedMCPSSERelayTestSession(t *testing.T, sessionID string) {
	t.Helper()
	serverSessionsMu.Lock()
	serverSessions[sessionID] = &mcpServerSession{
		userID:       "sse-integration-user",
		lastActivity: time.Now(),
	}
	serverSessionsMu.Unlock()
	t.Cleanup(func() {
		serverSessionsMu.Lock()
		delete(serverSessions, sessionID)
		serverSessionsMu.Unlock()
		closeMCPSSEStream(sessionID)
	})
}

func waitForMCPSSERelayStreamGone(t *testing.T, sessionID string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		mcpSSEStreamsMu.Lock()
		_, exists := mcpSSEStreams[sessionID]
		mcpSSEStreamsMu.Unlock()
		if !exists {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("SSE stream %q was not unregistered", sessionID)
}

func readSSEUntil(t *testing.T, reader *bufio.Reader, marker string) string {
	t.Helper()
	result := make(chan string, 1)
	go func() {
		var received strings.Builder
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				result <- fmt.Sprintf("read error: %v; received=%q", err, received.String())
				return
			}
			received.WriteString(line)
			if strings.Contains(received.String(), marker) {
				result <- received.String()
				return
			}
		}
	}()
	select {
	case received := <-result:
		if strings.HasPrefix(received, "read error:") {
			t.Fatal(received)
		}
		return received
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for SSE marker %q", marker)
		return ""
	}
}

func TestMCPGetSSEHTTPIntegrationLifecycle(t *testing.T) {
	const sessionID = "sse-http-integration-lifecycle"
	seedMCPSSERelayTestSession(t, sessionID)

	httpServer := httptest.NewServer(newMCPSSERelayTestRouter())
	defer httpServer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, httpServer.URL+"/mcp", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Mcp-Session-Id", sessionID)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /mcp status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if got := resp.Header.Get("Content-Type"); got != "text/event-stream; charset=utf-8" {
		t.Fatalf("Content-Type = %q, want text/event-stream; charset=utf-8", got)
	}
	reader := bufio.NewReader(resp.Body)
	readSSEUntil(t, reader, ": connected\n\n")

	if !publishMCPSSEEvent(sessionID, []byte(`{"jsonrpc":"2.0","method":"notifications/progress"}`)) {
		t.Fatal("publishMCPSSEEvent returned false for the active network stream")
	}
	readSSEUntil(t, reader, "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\"}\n\n")

	// DELETE is an end-to-end session lifecycle action: it must terminate the
	// server-side SSE handler instead of leaving a hidden goroutine/connection.
	deleteReq, err := http.NewRequest(http.MethodDelete, httpServer.URL+"/mcp", nil)
	if err != nil {
		t.Fatal(err)
	}
	deleteReq.Header.Set("Mcp-Session-Id", sessionID)
	deleteResp, err := http.DefaultClient.Do(deleteReq)
	if err != nil {
		t.Fatal(err)
	}
	deleteResp.Body.Close()
	if deleteResp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE /mcp status = %d, want %d", deleteResp.StatusCode, http.StatusNoContent)
	}

	readDone := make(chan error, 1)
	go func() {
		_, readErr := reader.ReadString('\n')
		readDone <- readErr
	}()
	select {
	case readErr := <-readDone:
		if readErr != io.EOF {
			t.Fatalf("SSE body after DELETE returned %v, want EOF", readErr)
		}
	case <-time.After(time.Second):
		t.Fatal("SSE body remained open after DELETE /mcp")
	}
	waitForMCPSSERelayStreamGone(t, sessionID)
}

func TestMCPGetSSERejectsInvalidNegotiationAndSession(t *testing.T) {
	httpServer := httptest.NewServer(newMCPSSERelayTestRouter())
	defer httpServer.Close()

	tests := []struct {
		name       string
		accept     string
		sessionID  string
		wantStatus int
	}{
		{name: "missing accept", accept: "application/json", sessionID: "unused", wantStatus: http.StatusNotAcceptable},
		{name: "explicit q zero overrides wildcard", accept: "text/event-stream;q=0, */*;q=1", sessionID: "unused", wantStatus: http.StatusNotAcceptable},
		{name: "invalid session", accept: "text/event-stream", sessionID: "missing-sse-session", wantStatus: http.StatusNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, httpServer.URL+"/mcp", nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Accept", tt.accept)
			req.Header.Set("Mcp-Session-Id", tt.sessionID)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != tt.wantStatus {
				t.Fatalf("GET /mcp status = %d, want %d", resp.StatusCode, tt.wantStatus)
			}
		})
	}
}

func TestMCPInitializeThenGetSSEIntegration(t *testing.T) {
	httpServer := httptest.NewServer(newMCPSSERelayTestRouter())
	defer httpServer.Close()

	initReq, err := http.NewRequest(
		http.MethodPost,
		httpServer.URL+"/mcp",
		strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"sse-integration-test","version":"1.0.0"}}}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	initReq.Header.Set("Content-Type", "application/json")
	initReq.Header.Set("Accept", "application/json, text/event-stream")
	initResp, err := http.DefaultClient.Do(initReq)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, initResp.Body)
	initResp.Body.Close()
	if initResp.StatusCode != http.StatusOK {
		t.Fatalf("initialize status = %d, want %d", initResp.StatusCode, http.StatusOK)
	}
	sessionID := initResp.Header.Get("Mcp-Session-Id")
	if sessionID == "" {
		t.Fatal("initialize did not return Mcp-Session-Id")
	}
	t.Cleanup(func() {
		serverSessionsMu.Lock()
		delete(serverSessions, sessionID)
		serverSessionsMu.Unlock()
		closeMCPSSEStream(sessionID)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	getReq, err := http.NewRequestWithContext(ctx, http.MethodGet, httpServer.URL+"/mcp", nil)
	if err != nil {
		t.Fatal(err)
	}
	getReq.Header.Set("Accept", "text/event-stream")
	getReq.Header.Set("Mcp-Session-Id", sessionID)
	getResp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatal(err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET after initialize status = %d, want %d", getResp.StatusCode, http.StatusOK)
	}
	readSSEUntil(t, bufio.NewReader(getResp.Body), ": connected\n\n")
	cancel()
	waitForMCPSSERelayStreamGone(t, sessionID)
}
