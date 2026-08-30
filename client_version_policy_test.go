package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

func TestGetClientVersionPolicyRequiresTrustedConsole(t *testing.T) {
	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	context.Request = httptest.NewRequest(http.MethodGet, "/internal/client-version-policy", nil)
	handleGetClientVersionPolicy(context)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", recorder.Code)
	}
}

func TestClientVersionPolicyResponseUsesRuntimeValues(t *testing.T) {
	previousMin := currentMinClientVersion()
	t.Cleanup(func() {
		setMinClientVersion(previousMin)
	})
	setMinClientVersion("1.3.4")

	response := clientVersionPolicyResponse()
	if response["package"] != cloudClientPackageName || response["minimum_version"] != "1.3.4" {
		t.Fatalf("unexpected policy response: %#v", response)
	}
	if _, exists := response["latest_version"]; exists {
		t.Fatalf("latest version must come from npm, not relay policy: %#v", response)
	}
	if response["index_client_version_required"] != true {
		t.Fatalf("minimum version must require index client version: %#v", response)
	}
}

func TestSaveClientVersionPolicyValidatesBeforeDatabaseAccess(t *testing.T) {
	previousToken := trustedConsoleToken
	t.Cleanup(func() { trustedConsoleToken = previousToken })
	configureTrustedConsole("test-secret")

	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	context.Request = httptest.NewRequest(http.MethodPost, "/internal/client-version-policy", strings.NewReader(`{"minimum_version":"latest"}`))
	context.Request.Header.Set("Content-Type", "application/json")
	context.Request.Header.Set(consoleTokenHeader, trustedConsoleToken)
	handleSaveClientVersionPolicy(context)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s, want 400", recorder.Code, recorder.Body.String())
	}
}

func TestSaveMinimumClientVersionPersistsThenUpdatesRuntime(t *testing.T) {
	previousMin := currentMinClientVersion()
	t.Cleanup(func() { setMinClientVersion(previousMin) })

	withMockDB(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO relay_runtime_settings").
			WithArgs(minimumClientVersionSetting, "1.3.4").
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()

		if err := saveMinimumClientVersion(" 1.3.4 "); err != nil {
			t.Fatalf("save minimum version: %v", err)
		}
		if got := currentMinClientVersion(); got != "1.3.4" {
			t.Fatalf("runtime minimum = %q, want 1.3.4", got)
		}
	})
}

func TestSaveMinimumClientVersionPersistsDisabledState(t *testing.T) {
	previousMin := currentMinClientVersion()
	t.Cleanup(func() { setMinClientVersion(previousMin) })
	setMinClientVersion("1.3.4")

	withMockDB(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO relay_runtime_settings").
			WithArgs(minimumClientVersionSetting, "").
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()

		if err := saveMinimumClientVersion(""); err != nil {
			t.Fatalf("disable minimum version: %v", err)
		}
		if got := currentMinClientVersion(); got != "" {
			t.Fatalf("runtime minimum = %q, want disabled", got)
		}
	})
}

func TestSaveClientVersionPolicyAcceptsNullAsExplicitDisable(t *testing.T) {
	previousMin := currentMinClientVersion()
	previousToken := trustedConsoleToken
	t.Cleanup(func() {
		setMinClientVersion(previousMin)
		trustedConsoleToken = previousToken
	})
	setMinClientVersion("1.3.4")
	configureTrustedConsole("test-secret")

	withMockDB(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO relay_runtime_settings").
			WithArgs(minimumClientVersionSetting, "").
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()

		recorder := httptest.NewRecorder()
		context, _ := gin.CreateTestContext(recorder)
		context.Request = httptest.NewRequest(http.MethodPost, "/internal/client-version-policy", strings.NewReader(`{"minimum_version":null}`))
		context.Request.Header.Set("Content-Type", "application/json")
		context.Request.Header.Set(consoleTokenHeader, trustedConsoleToken)
		handleSaveClientVersionPolicy(context)

		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d, body = %s, want 200", recorder.Code, recorder.Body.String())
		}
		if got := currentMinClientVersion(); got != "" {
			t.Fatalf("runtime minimum = %q, want disabled", got)
		}
	})
}
