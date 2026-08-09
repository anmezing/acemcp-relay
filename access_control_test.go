package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func trustedConsoleTestContext(method, path, token string) *gin.Context {
	request := httptest.NewRequest(method, path, nil)
	if token != "" {
		request.Header.Set(consoleTokenHeader, token)
	}
	context, _ := gin.CreateTestContext(httptest.NewRecorder())
	context.Request = request
	return context
}

func TestTrustedConsoleRequest(t *testing.T) {
	previousToken := trustedConsoleToken
	t.Cleanup(func() {
		trustedConsoleToken = previousToken
	})

	configureTrustedConsole("test-secret")
	validToken := trustedConsoleToken
	if validToken != "4807ce18f1faaa0d2e4ee689165b315f5ec3a49d2da10a2178ca00c4e6e73b48" {
		t.Fatalf("unexpected console token derivation: %s", validToken)
	}

	tests := []struct {
		name   string
		method string
		path   string
		token  string
		want   bool
	}{
		{name: "tenant stats", method: http.MethodGet, path: "/mcp/tenant-stats", token: validToken, want: true},
		{name: "clear index", method: http.MethodPost, path: "/mcp/clear-index", token: validToken, want: true},
		{name: "missing token", method: http.MethodGet, path: "/mcp/tenant-stats", want: false},
		{name: "wrong token", method: http.MethodGet, path: "/mcp/tenant-stats", token: "wrong", want: false},
		{name: "wrong method", method: http.MethodPost, path: "/mcp/tenant-stats", token: validToken, want: false},
		{name: "public MCP path", method: http.MethodPost, path: "/mcp", token: validToken, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := isTrustedConsoleRequest(trustedConsoleTestContext(test.method, test.path, test.token)); got != test.want {
				t.Fatalf("isTrustedConsoleRequest() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestTrustedConsoleRequestDisabledWithoutSecret(t *testing.T) {
	previousToken := trustedConsoleToken
	t.Cleanup(func() {
		trustedConsoleToken = previousToken
	})

	configureTrustedConsole("")
	if isTrustedConsoleRequest(trustedConsoleTestContext(http.MethodGet, "/mcp/tenant-stats", "")) {
		t.Fatal("trusted console access must stay disabled without a configured secret")
	}
}
