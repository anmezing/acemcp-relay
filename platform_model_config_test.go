package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

func platformConfigSaveTestRouter(t *testing.T, embeddingChanged bool) (*gin.Engine, *atomic.Int32) {
	t.Helper()
	previousToken, previousURL, previousConfigToken, previousClient := trustedConsoleToken, lcePlatformConfigURL, lcePlatformConfigToken, lce
	t.Cleanup(func() {
		trustedConsoleToken, lcePlatformConfigURL, lcePlatformConfigToken, lce = previousToken, previousURL, previousConfigToken, previousClient
	})
	configureTrustedConsole("platform-save-test")
	var saves atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var body struct {
			Action string `json:"action"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Error(err)
			writer.WriteHeader(400)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		if body.Action == "validate" {
			_ = json.NewEncoder(writer).Encode(map[string]interface{}{"embeddingChanged": embeddingChanged, "validationTicket": "test-ticket"})
		} else {
			saves.Add(1)
			_, _ = writer.Write([]byte(`{"config":{"promptEnhancer":{"enabled":true}}}`))
		}
	}))
	t.Cleanup(upstream.Close)
	lcePlatformConfigURL, lcePlatformConfigToken = upstream.URL, "config-test-token"
	lce = &mcpClient{http: upstream.Client()}
	router := gin.New()
	router.POST("/internal/platform-model-config", handleSavePlatformModelConfig)
	return router, &saves
}

func platformSaveTestRequest() *http.Request {
	request := httptest.NewRequest(http.MethodPost, "/internal/platform-model-config", strings.NewReader(`{"section":"promptEnhancer","config":{"promptEnhancer":{"enabled":true}}}`))
	request.Header.Set(consoleTokenHeader, trustedConsoleToken)
	request.Header.Set("Content-Type", "application/json")
	return request
}

func TestPromptConfigSaveDoesNotWaitForIndexReaders(t *testing.T) {
	router, saves := platformConfigSaveTestRouter(t, false)
	platformModelConfigBarrier.RLock()
	released := false
	defer func() {
		if !released {
			platformModelConfigBarrier.RUnlock()
		}
	}()
	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { defer close(done); router.ServeHTTP(recorder, platformSaveTestRequest()) }()
	select {
	case <-done:
		if recorder.Code != http.StatusOK || saves.Load() != 1 {
			t.Fatalf("save status=%d calls=%d body=%s", recorder.Code, saves.Load(), recorder.Body.String())
		}
	case <-time.After(time.Second):
		platformModelConfigBarrier.RUnlock()
		released = true
		<-done
		t.Fatal("prompt-only save waited for the global index barrier")
	}
}

func TestPlatformConfigBarrierDoesNotHoldSSEReadLock(t *testing.T) {
	router := gin.New()
	router.Use(platformModelConfigReadBarrier())
	var acquired bool
	router.GET("/mcp", func(c *gin.Context) {
		acquired = platformModelConfigBarrier.TryLock()
		if acquired {
			platformModelConfigBarrier.Unlock()
		}
		c.Status(http.StatusOK)
	})
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/mcp", nil))
	if !acquired {
		t.Fatal("SSE notification handler holds an index read lock for the lifetime of the connection")
	}
}

func TestPlatformConfigBarrierStillProtectsDataRequests(t *testing.T) {
	router := gin.New()
	router.Use(platformModelConfigReadBarrier())
	var acquired bool
	router.POST("/mcp", func(c *gin.Context) {
		acquired = platformModelConfigBarrier.TryLock()
		if acquired {
			platformModelConfigBarrier.Unlock()
		}
		c.Status(http.StatusOK)
	})
	router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/mcp", nil))
	if acquired {
		t.Fatal("data requests must retain the index read barrier")
	}
}

func TestConcurrentPlatformSaveReturnsConflictWithoutQueueing(t *testing.T) {
	router, saves := platformConfigSaveTestRouter(t, false)
	platformModelConfigAdminMu.Lock()
	defer platformModelConfigAdminMu.Unlock()
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, platformSaveTestRequest())
	if recorder.Code != http.StatusConflict || saves.Load() != 0 {
		t.Fatalf("status=%d saves=%d", recorder.Code, saves.Load())
	}
}

func TestCancelledPlatformValidationNeverSaves(t *testing.T) {
	router, saves := platformConfigSaveTestRouter(t, false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, platformSaveTestRequest().WithContext(ctx))
	if recorder.Code != http.StatusGatewayTimeout || saves.Load() != 0 {
		t.Fatalf("status=%d saves=%d body=%s", recorder.Code, saves.Load(), recorder.Body.String())
	}
}

func TestEmbeddingConfigSaveFailsBeforeClearingAnActiveIndex(t *testing.T) {
	router, saves := platformConfigSaveTestRouter(t, true)
	platformModelConfigBarrier.RLock()
	defer platformModelConfigBarrier.RUnlock()
	request := httptest.NewRequest(http.MethodPost, "/internal/platform-model-config", strings.NewReader(`{"section":"embeddings","config":{"embeddings":{}},"confirmEmbeddingReset":true}`))
	request.Header.Set(consoleTokenHeader, trustedConsoleToken)
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusConflict || saves.Load() != 0 {
		t.Fatalf("status=%d saves=%d", recorder.Code, saves.Load())
	}
}

func TestParsePlatformModelConfigPatch(t *testing.T) {
	tests := []struct {
		name        string
		section     string
		body        string
		wantErr     string
		wantSection string
	}{
		{name: "embeddings", section: "embeddings", body: `{"embeddings":{"provider":"voyage"}}`, wantSection: "embeddings"},
		{name: "rerank", section: "rerank", body: `{"rerank":{"provider":"voyage"}}`, wantSection: "rerank"},
		{name: "prompt enhancer", section: "promptEnhancer", body: `{"promptEnhancer":{"enabled":false}}`, wantSection: "promptEnhancer"},
		{name: "missing section", body: `{"embeddings":{}}`, wantErr: "section must be"},
		{name: "unknown section", section: "chat", body: `{"chat":{}}`, wantErr: "section must be"},
		{name: "multiple sections", section: "embeddings", body: `{"embeddings":{},"rerank":{}}`, wantErr: "exactly one"},
		{name: "mismatched section", section: "rerank", body: `{"embeddings":{}}`, wantErr: "must match"},
		{name: "null section", section: "rerank", body: `{"rerank":null}`, wantErr: "must be a JSON object"},
		{name: "array envelope", section: "rerank", body: `[]`, wantErr: "config must be a JSON object"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := parsePlatformModelConfigPatch(test.section, json.RawMessage(test.body))
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("parsePlatformModelConfigPatch() error = %v, want substring %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parsePlatformModelConfigPatch() error = %v", err)
			}
			if len(got) != 1 || got[test.wantSection] == nil {
				t.Fatalf("parsePlatformModelConfigPatch() = %#v, want only %q", got, test.wantSection)
			}
		})
	}
}

func TestClearAllRelayIndexStateCommitsAllTables(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	previousDB := db
	db = mockDB
	defer func() { db = previousDB }()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM index_operation_leases").
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec("DELETE FROM index_jobs").
		WillReturnResult(sqlmock.NewResult(0, 3))
	mock.ExpectExec("DELETE FROM indexed_files").
		WillReturnResult(sqlmock.NewResult(0, 5))
	mock.ExpectExec("DELETE FROM index_workspaces").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	got, err := clearAllRelayIndexState(context.Background())
	if err != nil {
		t.Fatalf("clearAllRelayIndexState() error = %v", err)
	}
	want := clearedRelayIndexes{Jobs: 3, Files: 5, Workspaces: 1, Leases: 2}
	if got != want {
		t.Fatalf("clearAllRelayIndexState() = %+v, want %+v", got, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestClearAllRelayIndexStateRollsBackOnDeleteFailure(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	previousDB := db
	db = mockDB
	defer func() { db = previousDB }()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM index_operation_leases").
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec("DELETE FROM index_jobs").
		WillReturnError(errors.New("jobs table unavailable"))
	mock.ExpectRollback()

	_, err = clearAllRelayIndexState(context.Background())
	if err == nil || err.Error() != "jobs table unavailable" {
		t.Fatalf("clearAllRelayIndexState() error = %v, want jobs table unavailable", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}
