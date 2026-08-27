package main

import (
	"net/http"
	"net/http/httptest"
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
