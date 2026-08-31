package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

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

func TestAcquirePlatformModelConfigWriteBarrierIsBounded(t *testing.T) {
	platformModelConfigBarrier.RLock()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	err := acquirePlatformModelConfigWriteBarrier(ctx)
	cancel()
	platformModelConfigBarrier.RUnlock()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("acquirePlatformModelConfigWriteBarrier() error = %v, want deadline exceeded", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := acquirePlatformModelConfigWriteBarrier(ctx); err != nil {
		t.Fatalf("acquirePlatformModelConfigWriteBarrier() after release error = %v", err)
	}
	platformModelConfigBarrier.Unlock()
}

func TestHandleSavePlatformModelConfigRejectsConcurrentSave(t *testing.T) {
	gin.SetMode(gin.TestMode)
	previousToken := trustedConsoleToken
	trustedConsoleToken = "console-token"
	t.Cleanup(func() { trustedConsoleToken = previousToken })

	platformModelConfigAdminMu.Lock()
	defer platformModelConfigAdminMu.Unlock()

	request := httptest.NewRequest(
		http.MethodPost,
		"/internal/platform-model-config",
		strings.NewReader(`{"section":"rerank","config":{"rerank":{"model":"rerank-v2"}}}`),
	)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(consoleTokenHeader, trustedConsoleToken)
	response := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(response)
	context.Request = request

	handleSavePlatformModelConfig(context)

	if response.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusConflict, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), "MODEL_CONFIG_SAVE_IN_PROGRESS") {
		t.Fatalf("body = %s, want save-in-progress code", response.Body.String())
	}
}

func TestHandleSavePlatformModelConfigSurfacesProviderQuotaFailureWithoutRelayRetry(t *testing.T) {
	gin.SetMode(gin.TestMode)
	requestCount := 0
	lceServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requestCount++
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusPaymentRequired)
		_, _ = response.Write([]byte(`{"error":"Embedding API 余额不足，请充值后重试"}`))
	}))
	defer lceServer.Close()

	previousURL := lcePlatformConfigURL
	previousLceToken := lcePlatformConfigToken
	previousConsoleToken := trustedConsoleToken
	previousHTTPClient := lce.http
	lcePlatformConfigURL = lceServer.URL
	lcePlatformConfigToken = "lce-token"
	trustedConsoleToken = "console-token"
	lce.http = lceServer.Client()
	t.Cleanup(func() {
		lcePlatformConfigURL = previousURL
		lcePlatformConfigToken = previousLceToken
		trustedConsoleToken = previousConsoleToken
		lce.http = previousHTTPClient
	})

	request := httptest.NewRequest(
		http.MethodPost,
		"/internal/platform-model-config",
		strings.NewReader(`{"section":"embeddings","config":{"embeddings":{"model":"voyage-code-3"}}}`),
	)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(consoleTokenHeader, trustedConsoleToken)
	response := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(response)
	context.Request = request

	handleSavePlatformModelConfig(context)

	if response.Code != http.StatusPaymentRequired {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusPaymentRequired, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), "余额不足") {
		t.Fatalf("body = %s, want provider quota failure", response.Body.String())
	}
	if requestCount != 1 {
		t.Fatalf("LCE validation requests = %d, want exactly 1 Relay attempt", requestCount)
	}
}

func TestRerankSaveDoesNotWaitForIndexBarrier(t *testing.T) {
	gin.SetMode(gin.TestMode)
	lceServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get("X-LCE-Platform-Config-Token") != "lce-token" {
			http.Error(response, "missing token", http.StatusUnauthorized)
			return
		}
		var body struct {
			Action string `json:"action"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			http.Error(response, err.Error(), http.StatusBadRequest)
			return
		}
		response.Header().Set("Content-Type", "application/json")
		switch body.Action {
		case "validate":
			_, _ = response.Write([]byte(`{"embeddingChanged":false,"validationTicket":"ticket"}`))
		case "save":
			_, _ = response.Write([]byte(`{"ok":true,"embeddingChanged":false,"config":{"rerank":{"model":"rerank-v2"}}}`))
		default:
			http.Error(response, "unexpected action", http.StatusBadRequest)
		}
	}))
	defer lceServer.Close()

	previousURL := lcePlatformConfigURL
	previousLceToken := lcePlatformConfigToken
	previousConsoleToken := trustedConsoleToken
	previousHTTPClient := lce.http
	lcePlatformConfigURL = lceServer.URL
	lcePlatformConfigToken = "lce-token"
	trustedConsoleToken = "console-token"
	lce.http = lceServer.Client()
	t.Cleanup(func() {
		lcePlatformConfigURL = previousURL
		lcePlatformConfigToken = previousLceToken
		trustedConsoleToken = previousConsoleToken
		lce.http = previousHTTPClient
	})

	request := httptest.NewRequest(
		http.MethodPost,
		"/internal/platform-model-config",
		strings.NewReader(`{"section":"rerank","config":{"rerank":{"model":"rerank-v2"}}}`),
	)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(consoleTokenHeader, trustedConsoleToken)
	response := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(response)
	context.Request = request

	platformModelConfigBarrier.RLock()
	barrierHeld := true
	defer func() {
		if barrierHeld {
			platformModelConfigBarrier.RUnlock()
		}
	}()

	done := make(chan struct{})
	go func() {
		handleSavePlatformModelConfig(context)
		close(done)
	}()

	select {
	case <-done:
		platformModelConfigBarrier.RUnlock()
		barrierHeld = false
	case <-time.After(time.Second):
		platformModelConfigBarrier.RUnlock()
		barrierHeld = false
		<-done
		t.Fatal("rerank save waited for the indexing read barrier")
	}

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusOK, response.Body.String())
	}
}
