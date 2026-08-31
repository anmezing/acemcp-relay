package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

// ── 测试基建 ───────────────────────────────────────────────────────────────

func resetDeleteRootRateLimit() {
	deleteRootSeenMu.Lock()
	deleteRootSeen = make(map[string]time.Time)
	deleteRootSeenMu.Unlock()
}

// withMockDB 把全局 db 换成 sqlmock，并在收尾断言所有预期都被消费。
func withMockDB(t *testing.T, fn func(mock sqlmock.Sqlmock)) {
	t.Helper()
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	previousDB := db
	previousAcquireDelete := acquireDeleteRootOperation
	previousAcquireDismiss := acquireDismissRootFailureOperation
	db = mockDB
	acquireDeleteRootOperation = func(ctx context.Context, tenantID string) (deleteRootOperationLease, error) {
		return noopDeleteRootLease{ctx: ctx}, nil
	}
	acquireDismissRootFailureOperation = func(ctx context.Context, tenantID string) (deleteRootOperationLease, error) {
		return noopDeleteRootLease{ctx: ctx}, nil
	}
	defer func() {
		db = previousDB
		acquireDeleteRootOperation = previousAcquireDelete
		acquireDismissRootFailureOperation = previousAcquireDismiss
		mockDB.Close()
	}()
	fn(mock)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

type noopDeleteRootLease struct {
	ctx     context.Context
	release func()
}

func (l noopDeleteRootLease) Context() context.Context { return l.ctx }
func (l noopDeleteRootLease) Release() {
	if l.release != nil {
		l.release()
	}
}

func newRootAdminContext(t *testing.T, userID, method, body string) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(method, "/mcp/test", bytes.NewReader([]byte(body)))
	request.Header.Set("Content-Type", "application/json")
	c.Request = request
	if userID != "" {
		c.Set(ContextKeyUserID, userID)
	}
	return c, recorder
}

func stubLCEClearIndexRoot(t *testing.T, fn func(ctx context.Context, userID, rootID string) (*mcpToolResult, error)) {
	t.Helper()
	previous := lceClearIndexRoot
	lceClearIndexRoot = fn
	t.Cleanup(func() { lceClearIndexRoot = previous })
}

func activeJobRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"root_id", "indexed_files", "total_files", "phase"})
}

// ── _index_status / active_job ─────────────────────────────────────────────

func TestLoadActiveIndexJobReturnsLatestRunningJob(t *testing.T) {
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("FROM index_jobs").
			WithArgs("user-1").
			WillReturnRows(activeJobRows().AddRow("repo-a", 5, 12, "indexing"))
		job, err := loadActiveIndexJob(context.Background(), "user-1")
		if err != nil {
			t.Fatal(err)
		}
		if job == nil || job.RootID != "repo-a" || job.IndexedFiles != 5 || job.TotalFiles != 12 || job.Phase != "indexing" {
			t.Fatalf("unexpected active job: %+v", job)
		}
	})
}

func TestLoadActiveIndexJobReturnsNilWithoutRunningJob(t *testing.T) {
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("FROM index_jobs").
			WithArgs("user-1").
			WillReturnRows(activeJobRows())
		job, err := loadActiveIndexJob(context.Background(), "user-1")
		if err != nil {
			t.Fatal(err)
		}
		if job != nil {
			t.Fatalf("expected nil without running job, got %+v", job)
		}
	})
}

func TestInjectRetrievalIndexStatusAddsStatus(t *testing.T) {
	active := &activeIndexJob{RootID: "repo-a", IndexedFiles: 3, TotalFiles: 9, Phase: "indexing"}
	out := injectRetrievalIndexStatus([]byte(`{"results":[1]}`), active)
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatal(err)
	}
	status, ok := parsed["_index_status"].(map[string]interface{})
	if !ok {
		t.Fatalf("_index_status missing: %s", out)
	}
	if status["state"] != "building" || status["root_id"] != "repo-a" ||
		status["indexed_files"] != float64(3) || status["total_files"] != float64(9) {
		t.Fatalf("unexpected _index_status contract payload: %#v", status)
	}
	if _, exists := status["phase"]; exists {
		t.Fatal("_index_status must not carry phase (contract has four fields)")
	}
	if _, exists := parsed["results"]; !exists {
		t.Fatal("original payload was lost")
	}
}

func TestInjectRetrievalIndexStatusSkipsNonJSONAndNoOpCases(t *testing.T) {
	active := &activeIndexJob{RootID: "repo-a"}
	for _, raw := range []string{"plain text result", `[1,2,3]`, `"scalar"`} {
		if got := injectRetrievalIndexStatus([]byte(raw), active); got != raw {
			t.Fatalf("non-object response must pass through unchanged: %q -> %q", raw, got)
		}
	}
	original := `{"results":[]}`
	if got := injectRetrievalIndexStatus([]byte(original), nil); got != original {
		t.Fatalf("nothing to inject must return original bytes: %q", got)
	}
	out := injectRetrievalIndexStatus([]byte(original), active)
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatal(err)
	}
	if _, exists := parsed["_index_status"]; !exists {
		t.Fatal("_index_status missing when a job is running")
	}
}

// ── GET /mcp/roots ─────────────────────────────────────────────────────────

func TestHandleListRootsAggregatesFilesAndKeepsEmptyWorkspaceZero(t *testing.T) {
	indexedAt := time.Date(2026, 8, 1, 10, 30, 0, 0, time.UTC)
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("FROM index_workspaces").
			WithArgs("user-1").
			WillReturnRows(sqlmock.NewRows([]string{
				"workspace_id", "root_id", "branch", "revision", "cloud_revision", "indexed_at",
				"file_count", "total_size",
			}).
				AddRow("repo-a", "repo-a", "main", "abc", int64(7), indexedAt, int64(42), int64(123456)).
				AddRow("repo-a@feature-x", "repo-a@feature-x", "feature/x", "def", int64(3), indexedAt.Add(-30*time.Minute), int64(10), int64(999)).
				AddRow("repo-b", "repo-b", "", "", int64(0), indexedAt.Add(-time.Hour), int64(0), int64(0)))
		mock.ExpectQuery("FROM index_jobs").
			WithArgs("user-1").
			WillReturnRows(sqlmock.NewRows([]string{
				"workspace_id", "root_id", "branch", "revision", "phase", "status",
				"total_files", "indexed_files", "failed_files", "error", "cloud_revision", "started_at",
			}))

		c, recorder := newRootAdminContext(t, "user-1", "GET", "")
		handleListRoots(c)

		if recorder.Code != 200 {
			t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
		}
		var response struct {
			Roots []indexRootView `json:"roots"`
		}
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		if len(response.Roots) != 3 {
			t.Fatalf("expected 3 roots, got %#v", response.Roots)
		}
		first := response.Roots[0]
		if first.RootID != "repo-a" || first.FileCount != 42 || first.TotalSizeBytes != 123456 ||
			first.CloudRevision != 7 || first.IndexedAt != "2026-08-01T10:30:00Z" {
			t.Fatalf("unexpected first root: %+v", first)
		}
		// 无 '@' 的 root：base 就是 root_id，视图分支归入 "default"；
		// 既有 branch 字段继续携带 start 上报的 Git 元数据，不受派生字段影响。
		if first.BaseRootID != "repo-a" || first.ViewBranch != "default" || first.Branch != "main" {
			t.Fatalf("unexpected derived view fields for plain root: %+v", first)
		}
		// 带 '@' 的 root：按最后一个 '@' 拆分。
		versioned := response.Roots[1]
		if versioned.BaseRootID != "repo-a" || versioned.ViewBranch != "feature-x" ||
			versioned.RootID != "repo-a@feature-x" || versioned.Branch != "feature/x" {
			t.Fatalf("unexpected derived view fields for versioned root: %+v", versioned)
		}
		// 空 workspace 的聚合列必须是 0，而不是 null 或缺字段
		second := response.Roots[2]
		if second.FileCount != 0 || second.TotalSizeBytes != 0 {
			t.Fatalf("empty workspace must aggregate to zero: %+v", second)
		}
	})
}

func TestLoadIndexRootsMergesLatestProgressAndKeepsUnpublishedFailuresVisible(t *testing.T) {
	indexedAt := time.Date(2026, 8, 1, 10, 30, 0, 0, time.UTC)
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("FROM index_workspaces").
			WithArgs("user-1").
			WillReturnRows(sqlmock.NewRows([]string{
				"workspace_id", "root_id", "branch", "revision", "cloud_revision", "indexed_at",
				"file_count", "total_size",
			}).AddRow("repo-a", "repo-a", "main", "old", int64(7), indexedAt, int64(42), int64(123456)))
		mock.ExpectQuery("FROM index_jobs").
			WithArgs("user-1").
			WillReturnRows(sqlmock.NewRows([]string{
				"workspace_id", "root_id", "branch", "revision", "phase", "status",
				"total_files", "indexed_files", "failed_files", "error", "cloud_revision", "started_at",
			}).
				AddRow("repo-a", "repo-a", "main", "next", "uploading", "running", 100, 37, 0, "", int64(8), indexedAt.Add(time.Hour)).
				AddRow("repo-b", "repo-b@feature", "feature/x", "broken", "scanning", "failed", 12, 3, 1, "manifest rejected", int64(0), indexedAt))

		roots, err := loadIndexRoots(context.Background(), "user-1")
		if err != nil {
			t.Fatal(err)
		}
		if len(roots) != 2 {
			t.Fatalf("expected published and unpublished roots, got %#v", roots)
		}
		if roots[0].IndexState != "building" || roots[0].IndexPhase != "uploading" ||
			roots[0].ProgressPercent != 37 || roots[0].IndexedFiles != 37 || roots[0].TotalFiles != 100 ||
			!roots[0].IndexAvailable || roots[0].Revision != "old" || roots[0].CloudRevision != 7 ||
			roots[0].SyncRevision != "next" || roots[0].SyncCloudRevision != 8 {
			t.Fatalf("running progress not merged into published root: %+v", roots[0])
		}
		if roots[1].IndexedAt != "" || roots[1].IndexAvailable || roots[1].IndexState != "failed" ||
			roots[1].IndexError != "manifest rejected" || roots[1].IndexErrorCode != "index_failed" ||
			roots[1].IndexErrorOrigin != "unknown" || roots[1].IndexRecovery != "inspect_logs" ||
			roots[1].BaseRootID != "repo-b" || roots[1].ViewBranch != "feature" || roots[1].SyncRevision != "broken" {
			t.Fatalf("unpublished failed root must remain visible: %+v", roots[1])
		}
	})
}

func TestLoadIndexRootsDoesNotResurrectDeletedCompletedOrSupersededJobs(t *testing.T) {
	startedAt := time.Date(2026, 8, 1, 10, 30, 0, 0, time.UTC)
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("FROM index_workspaces").
			WithArgs("user-1").
			WillReturnRows(sqlmock.NewRows([]string{
				"workspace_id", "root_id", "branch", "revision", "cloud_revision", "indexed_at",
				"file_count", "total_size",
			}))
		mock.ExpectQuery("FROM index_jobs").
			WithArgs("user-1").
			WillReturnRows(sqlmock.NewRows([]string{
				"workspace_id", "root_id", "branch", "revision", "phase", "status",
				"total_files", "indexed_files", "failed_files", "error", "cloud_revision", "started_at",
			}).
				AddRow("deleted-a", "deleted-a", "main", "done", "completed", "completed", 10, 10, 0, "", int64(3), startedAt).
				AddRow("deleted-b", "deleted-b", "main", "old", "superseded", "superseded", 10, 4, 0, "", int64(2), startedAt.Add(-time.Minute)))

		roots, err := loadIndexRoots(context.Background(), "user-1")
		if err != nil {
			t.Fatal(err)
		}
		if len(roots) != 0 {
			t.Fatalf("historical terminal jobs must not resurrect deleted roots: %+v", roots)
		}
	})
}

func TestClassifyIndexFailure(t *testing.T) {
	cases := []struct {
		name     string
		status   string
		detail   string
		expected indexFailureDiagnostic
	}{
		{"heartbeat", indexJobStatusTimedOut, "index job heartbeat timed out", indexFailureDiagnostic{"heartbeat_timeout", "relay", "restart_client"}},
		{"embedding space changed", indexJobStatusFailed, "LCE cloud index begin failed: cloud embedding space changed; clear the tenant root before starting a new index job", indexFailureDiagnostic{"embedding_space_changed", "remote_index", "reset_root"}},
		{"cloudflare 502", indexJobStatusFailed, `remote-index 502: {"title":"Error 502: Bad gateway","detail":"origin web server returned an invalid response"}`, indexFailureDiagnostic{"upstream_bad_gateway", "remote_index", "retry_after_service_recovers"}},
		{"provider billing", indexJobStatusFailed, "embedding provider: insufficient balance", indexFailureDiagnostic{"provider_billing", "provider", "fix_provider_billing"}},
		{"provider rate limit", indexJobStatusFailed, "remote-index 429: too many requests", indexFailureDiagnostic{"provider_rate_limited", "provider", "retry_later"}},
		{"file count", indexJobStatusFailed, "manifest exceeds 100000 files", indexFailureDiagnostic{"repository_file_limit", "client", "reduce_repository"}},
		{"file size", indexJobStatusFailed, "file exceeds the 524288 byte limit: generated/data.json", indexFailureDiagnostic{"repository_file_size_limit", "client", "reduce_repository"}},
		{"manifest file size", indexJobStatusFailed, "manifest file size is invalid: generated/data.json", indexFailureDiagnostic{"repository_file_size_limit", "client", "reduce_repository"}},
		{"quota", indexJobStatusFailed, "index quota exceeded", indexFailureDiagnostic{"index_quota_exceeded", "relay", "wait_for_quota_reset"}},
		{"credentials", indexJobStatusFailed, "remote-index 401: invalid api key", indexFailureDiagnostic{"provider_authentication", "provider", "fix_credentials"}},
		{"network", indexJobStatusFailed, "dial tcp: connection refused", indexFailureDiagnostic{"network_unavailable", "network", "restart_client"}},
		{"unknown", indexJobStatusFailed, "manifest rejected", indexFailureDiagnostic{"index_failed", "unknown", "inspect_logs"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyIndexFailure(tc.status, tc.detail); got != tc.expected {
				t.Fatalf("classifyIndexFailure(%q, %q) = %+v, want %+v", tc.status, tc.detail, got, tc.expected)
			}
		})
	}
}

func TestSplitRootIDViewDerivation(t *testing.T) {
	cases := []struct {
		rootID string
		base   string
		branch string
	}{
		{"repo-a", "repo-a", "default"},
		{"repo-a@feature-x", "repo-a", "feature-x"},
		// 多个 '@'：按最后一个拆分
		{"repo@a@b", "repo@a", "b"},
		// 畸形/历史数据不猜测：任一侧为空则整体作为 base
		{"repo-a@", "repo-a@", "default"},
		{"@feature-x", "@feature-x", "default"},
		{"", "", "default"},
	}
	for _, tc := range cases {
		base, branch := splitRootIDView(tc.rootID)
		if base != tc.base || branch != tc.branch {
			t.Fatalf("splitRootIDView(%q) = (%q, %q), want (%q, %q)",
				tc.rootID, base, branch, tc.base, tc.branch)
		}
	}
}

func TestHandleListRootsReturnsEmptyArrayNotNull(t *testing.T) {
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("FROM index_workspaces").
			WithArgs("user-1").
			WillReturnRows(sqlmock.NewRows([]string{
				"workspace_id", "root_id", "branch", "revision", "cloud_revision", "indexed_at",
				"file_count", "total_size",
			}))
		mock.ExpectQuery("FROM index_jobs").
			WithArgs("user-1").
			WillReturnRows(sqlmock.NewRows([]string{
				"workspace_id", "root_id", "branch", "revision", "phase", "status",
				"total_files", "indexed_files", "failed_files", "error", "cloud_revision", "started_at",
			}))
		c, recorder := newRootAdminContext(t, "user-1", "GET", "")
		handleListRoots(c)
		if recorder.Code != 200 || !strings.Contains(recorder.Body.String(), `"roots":[]`) {
			t.Fatalf("expected empty roots array, got %d %s", recorder.Code, recorder.Body.String())
		}
	})
}

// ── POST /mcp/dismiss-root-failure ─────────────────────────────────────────

func expectDismissRootFailureTx(mock sqlmock.Sqlmock, userID, rootID string, dismissed int64) {
	mock.ExpectBegin()
	mock.ExpectExec("pg_advisory_xact_lock").
		WithArgs(userID).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("DELETE FROM index_jobs").
		WithArgs(userID, rootID, indexJobStatusFailed, indexJobStatusTimedOut).
		WillReturnResult(sqlmock.NewResult(0, dismissed))
	mock.ExpectCommit()
}

func TestHandleDismissRootFailurePreservesPublishedIndexState(t *testing.T) {
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("SELECT EXISTS").
			WithArgs("user-1", "repo-a").
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
		expectDismissRootFailureTx(mock, "user-1", "repo-a", 2)

		c, recorder := newRootAdminContext(t, "user-1", "POST", `{"root_id":"repo-a"}`)
		handleDismissRootFailure(c)
		if recorder.Code != 200 {
			t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
		}
		if !strings.Contains(recorder.Body.String(), `"dismissed_jobs":2`) {
			t.Fatalf("unexpected response: %s", recorder.Body.String())
		}
		// sqlmock 未配置 indexed_files/index_workspaces DELETE；若实现误删已发布快照，
		// ExpectationsWereMet 会失败。
	})
}

func TestHandleDismissRootFailureIsIdempotent(t *testing.T) {
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("SELECT EXISTS").
			WithArgs("user-1", "repo-a").
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
		expectDismissRootFailureTx(mock, "user-1", "repo-a", 0)

		c, recorder := newRootAdminContext(t, "user-1", "POST", `{"root_id":"repo-a"}`)
		handleDismissRootFailure(c)
		if recorder.Code != 200 || !strings.Contains(recorder.Body.String(), `"dismissed":true`) {
			t.Fatalf("idempotent dismiss should succeed, got %d %s", recorder.Code, recorder.Body.String())
		}
	})
}

func TestHandleDismissRootFailureConflictsWithRunningJob(t *testing.T) {
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("SELECT EXISTS").
			WithArgs("user-1", "repo-a").
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))

		c, recorder := newRootAdminContext(t, "user-1", "POST", `{"root_id":"repo-a"}`)
		handleDismissRootFailure(c)
		if recorder.Code != 409 {
			t.Fatalf("status = %d, want 409, body = %s", recorder.Code, recorder.Body.String())
		}
	})
}

// ── POST /mcp/delete-root ──────────────────────────────────────────────────

func TestHandleDeleteRootRejectsEmptyRootID(t *testing.T) {
	resetDeleteRootRateLimit()
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		for _, body := range []string{`{}`, `{"root_id":"  "}`, `not json`} {
			c, recorder := newRootAdminContext(t, "user-1", "POST", body)
			handleDeleteRoot(c)
			if recorder.Code != 400 {
				t.Fatalf("body %q: status = %d, want 400", body, recorder.Code)
			}
		}
	})
}

func TestHandleDeleteRootConflictsWithRunningJob(t *testing.T) {
	resetDeleteRootRateLimit()
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("SELECT EXISTS").
			WithArgs("user-1", "repo-a").
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
		c, recorder := newRootAdminContext(t, "user-1", "POST", `{"root_id":"repo-a"}`)
		handleDeleteRoot(c)
		if recorder.Code != 409 {
			t.Fatalf("status = %d, want 409, body = %s", recorder.Code, recorder.Body.String())
		}
		if !strings.Contains(recorder.Body.String(), "error") {
			t.Fatalf("409 body must carry an error field: %s", recorder.Body.String())
		}
	})
	// 409 不消耗频率窗口：任务结束后应能立即重试
	if wait := checkDeleteRootRateLimit("user-1", "repo-a", time.Now()); wait != 0 {
		t.Fatalf("409 must not charge the rate window, wait = %d", wait)
	}
}

func TestHandleDeleteRootAcquiresExclusiveLeaseBeforeRunningCheck(t *testing.T) {
	resetDeleteRootRateLimit()
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		acquired := false
		released := false
		acquireDeleteRootOperation = func(ctx context.Context, tenantID string) (deleteRootOperationLease, error) {
			if tenantID != "user-1" {
				t.Fatalf("lease tenant = %q, want user-1", tenantID)
			}
			acquired = true
			return noopDeleteRootLease{ctx: ctx, release: func() { released = true }}, nil
		}
		mock.ExpectQuery("SELECT EXISTS").
			WithArgs("user-1", "repo-a").
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))

		c, recorder := newRootAdminContext(t, "user-1", "POST", `{"root_id":"repo-a"}`)
		handleDeleteRoot(c)

		if recorder.Code != 409 || !acquired || !released {
			t.Fatalf("status=%d acquired=%v released=%v body=%s", recorder.Code, acquired, released, recorder.Body.String())
		}
	})
}

func TestHandleDeleteRootLeaseFailureDoesNotInspectJobs(t *testing.T) {
	resetDeleteRootRateLimit()
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		acquireDeleteRootOperation = func(ctx context.Context, tenantID string) (deleteRootOperationLease, error) {
			return nil, errors.New("lease unavailable")
		}
		c, recorder := newRootAdminContext(t, "user-1", "POST", `{"root_id":"repo-a"}`)
		handleDeleteRoot(c)
		if recorder.Code != 503 {
			t.Fatalf("status = %d, want 503, body = %s", recorder.Code, recorder.Body.String())
		}
	})
}

func TestCheckDeleteRootRateLimitEnforcesOnePerMinute(t *testing.T) {
	resetDeleteRootRateLimit()
	now := time.Now()
	if wait := checkDeleteRootRateLimit("user-1", "repo-a", now); wait != 0 {
		t.Fatalf("first call must pass, wait = %d", wait)
	}
	if wait := checkDeleteRootRateLimit("user-1", "repo-a", now.Add(10*time.Second)); wait <= 0 || wait > 60 {
		t.Fatalf("second call within a minute must wait, wait = %d", wait)
	}
	if wait := checkDeleteRootRateLimit("user-2", "repo-a", now); wait != 0 {
		t.Fatal("rate limit must be per tenant")
	}
	// key 是 (tenant, root)：同租户的另一个 root 不共享窗口
	if wait := checkDeleteRootRateLimit("user-1", "repo-b", now.Add(10*time.Second)); wait != 0 {
		t.Fatal("rate limit must be per (tenant, root)")
	}
	if wait := checkDeleteRootRateLimit("user-1", "repo-a", now.Add(deleteRootMinInterval)); wait != 0 {
		t.Fatalf("after the interval the call must pass, wait = %d", wait)
	}
}

func TestHandleDeleteRootRateLimitedReturns429(t *testing.T) {
	resetDeleteRootRateLimit()
	checkDeleteRootRateLimit("user-1", "repo-a", time.Now())
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("SELECT EXISTS").
			WithArgs("user-1", "repo-a").
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
		c, recorder := newRootAdminContext(t, "user-1", "POST", `{"root_id":"repo-a"}`)
		handleDeleteRoot(c)
		if recorder.Code != 429 {
			t.Fatalf("status = %d, want 429, body = %s", recorder.Code, recorder.Body.String())
		}
	})
}

func TestHandleDeleteRootLCEFailureKeepsRelayRows(t *testing.T) {
	resetDeleteRootRateLimit()
	stubLCEClearIndexRoot(t, func(ctx context.Context, userID, rootID string) (*mcpToolResult, error) {
		return nil, errors.New("lce unreachable")
	})
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		// 只允许 409 检查这一条查询；出现任何 DELETE 都会因未预期而失败，
		// 这正是"LCE 失败时 relay 行不删"的断言。
		mock.ExpectQuery("SELECT EXISTS").
			WithArgs("user-1", "repo-a").
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
		c, recorder := newRootAdminContext(t, "user-1", "POST", `{"root_id":"repo-a"}`)
		handleDeleteRoot(c)
		if recorder.Code != 502 {
			t.Fatalf("status = %d, want 502, body = %s", recorder.Code, recorder.Body.String())
		}
	})
}

func TestHandleDeleteRootLCEIsErrorAlsoReturns502(t *testing.T) {
	resetDeleteRootRateLimit()
	stubLCEClearIndexRoot(t, func(ctx context.Context, userID, rootID string) (*mcpToolResult, error) {
		return &mcpToolResult{Content: []byte(`{"error":"tenant busy"}`), IsError: true}, nil
	})
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("SELECT EXISTS").
			WithArgs("user-1", "repo-a").
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
		c, recorder := newRootAdminContext(t, "user-1", "POST", `{"root_id":"repo-a"}`)
		handleDeleteRoot(c)
		if recorder.Code != 502 {
			t.Fatalf("status = %d, want 502, body = %s", recorder.Code, recorder.Body.String())
		}
	})
}

func expectDeleteRootTx(mock sqlmock.Sqlmock, userID, rootID string, deletedFiles int64) {
	mock.ExpectBegin()
	mock.ExpectExec("pg_advisory_xact_lock").
		WithArgs(userID).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("DELETE FROM index_jobs").
		WithArgs(userID, rootID).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec("DELETE FROM indexed_files AS files").
		WithArgs(userID, rootID).
		WillReturnResult(sqlmock.NewResult(0, deletedFiles))
	mock.ExpectExec("DELETE FROM index_workspaces").
		WithArgs(userID, rootID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
}

func TestHandleDeleteRootSuccessUsesRelayDeletedFilesFallback(t *testing.T) {
	resetDeleteRootRateLimit()
	var lceRoot string
	stubLCEClearIndexRoot(t, func(ctx context.Context, userID, rootID string) (*mcpToolResult, error) {
		lceRoot = rootID
		return &mcpToolResult{Content: []byte(`{"ok":true}`)}, nil
	})
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("SELECT EXISTS").
			WithArgs("user-1", "repo-a").
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
		expectDeleteRootTx(mock, "user-1", "repo-a", 17)

		c, recorder := newRootAdminContext(t, "user-1", "POST", `{"root_id":"repo-a"}`)
		handleDeleteRoot(c)
		if recorder.Code != 200 {
			t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
		}
		var response struct {
			Deleted      bool  `json:"deleted"`
			DeletedFiles int64 `json:"deleted_files"`
		}
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		if !response.Deleted || response.DeletedFiles != 17 {
			t.Fatalf("unexpected response: %+v", response)
		}
	})
	if lceRoot != "repo-a" {
		t.Fatalf("LCE clear must be called with the requested root, got %q", lceRoot)
	}
}

func TestHandleDeleteRootPrefersLCEDeletedFileCount(t *testing.T) {
	resetDeleteRootRateLimit()
	stubLCEClearIndexRoot(t, func(ctx context.Context, userID, rootID string) (*mcpToolResult, error) {
		return &mcpToolResult{Content: []byte(`{"payload":{"deleted_files":99}}`)}, nil
	})
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("SELECT EXISTS").
			WithArgs("user-1", "repo-a").
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
		expectDeleteRootTx(mock, "user-1", "repo-a", 17)

		c, recorder := newRootAdminContext(t, "user-1", "POST", `{"root_id":"repo-a"}`)
		handleDeleteRoot(c)
		if recorder.Code != 200 || !strings.Contains(recorder.Body.String(), `"deleted_files":99`) {
			t.Fatalf("expected LCE-reported count, got %d %s", recorder.Code, recorder.Body.String())
		}
	})
}

func TestExtractLCEDeletedFiles(t *testing.T) {
	cases := []struct {
		content string
		count   int64
		ok      bool
	}{
		{`{"deleted_files":12}`, 12, true},
		{`{"payload":{"deletedCount":3}}`, 3, true},
		{`{"ok":true}`, 0, false},
		{`not json`, 0, false},
	}
	for _, tc := range cases {
		count, ok := extractLCEDeletedFiles([]byte(tc.content))
		if count != tc.count || ok != tc.ok {
			t.Fatalf("%q: got (%d, %v), want (%d, %v)", tc.content, count, ok, tc.count, tc.ok)
		}
	}
}
