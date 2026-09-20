package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func publicationUpstream(t *testing.T, replies map[string]string, calls *[]string) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Params struct {
				Arguments struct {
					Operation string `json:"operation"`
				} `json:"arguments"`
			} `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		op := request.Params.Arguments.Operation
		*calls = append(*calls, op)
		reply, ok := replies[op]
		if !ok {
			t.Errorf("unexpected upstream operation %q", op)
			w.WriteHeader(500)
			return
		}
		if reply == "unavailable" {
			w.WriteHeader(503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": 1, "result": map[string]interface{}{"content": []interface{}{map[string]string{"type": "text", "text": reply}}}})
	}))
	previous, previousURL := lce, lceMCPURL
	lce, lceMCPURL = &mcpClient{http: server.Client(), sessionID: "recovery-test"}, server.URL
	t.Cleanup(func() { lce, lceMCPURL = previous, previousURL; server.Close() })
}

func expectPublicationLease(mock sqlmock.Sqlmock, acquired bool) {
	expectIndexOperationLock(mock, "tenant")
	mock.ExpectQuery("WITH expired AS").WithArgs(sqlmock.AnyArg(), "tenant", "job:job", indexOperationShared, "reconcile-publication", indexOperationLeaseDuration.Milliseconds(), indexOperationExclusive).
		WillReturnRows(sqlmock.NewRows([]string{"acquired"}).AddRow(acquired))
	mock.ExpectCommit()
	if acquired {
		mock.ExpectQuery("SELECT EXISTS.*index_jobs").WithArgs("job", "tenant").WillReturnRows(sqlmock.NewRows([]string{"active"}).AddRow(true))
	}
}

func expectPublicationRelease(mock sqlmock.Sqlmock) {
	mock.ExpectExec("DELETE FROM index_operation_leases").WithArgs("tenant", sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
}

func TestPublicationReconcileDoesNotRaceActiveSubmission(t *testing.T) {
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		expectPublicationLease(mock, false)
		if err := reconcileIndexPublication(context.Background(), publicationCandidate{id: "job", user: "tenant", root: "root"}); !errors.Is(err, errIndexOperationBusy) {
			t.Fatalf("wanted busy, got %v", err)
		}
	})
}

func TestPublicationReconcileKeepsFenceWhenCancellationIsUnknown(t *testing.T) {
	var calls []string
	publicationUpstream(t, map[string]string{"status": "unavailable", "abort": "unavailable"}, &calls)
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		expectPublicationLease(mock, true)
		expectPublicationRelease(mock)
		err := reconcileIndexPublication(context.Background(), publicationCandidate{id: "job", user: "tenant", root: "root", since: sql.NullTime{Valid: true, Time: time.Now().Add(-2 * indexPublicationMaxAge)}})
		if err == nil {
			t.Fatal("unknown cancellation must remain retryable")
		}
	})
	if !reflect.DeepEqual(calls, []string{"status", "abort"}) {
		t.Fatalf("calls: %v", calls)
	}
}

func expectPublicationPreflight(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("SELECT status, root_id, total_files").WithArgs("job", "tenant").WillReturnRows(
		sqlmock.NewRows([]string{"status", "root_id", "total_files", "indexed_files", "deleted_count", "deletions_sent", "cloud_revision"}).AddRow("running", "root", 1, 1, 0, true, 0))
	mock.ExpectExec("UPDATE index_jobs SET phase = 'publishing'.*publishing_since = COALESCE").WithArgs("job", "tenant", indexJobStatusRunning).WillReturnResult(sqlmock.NewResult(0, 1))
}

func TestPublicationConfirmedCancellationReleasesFence(t *testing.T) {
	var calls []string
	publicationUpstream(t, map[string]string{"status": `{"state":"leased"}`, "abort": `{"state":"aborted"}`}, &calls)
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		expectPublicationLease(mock, true)
		expectIndexUserLock(mock, "tenant")
		mock.ExpectExec("UPDATE index_jobs").WithArgs(indexJobStatusFailed, "cloud publication deadline exceeded; cancellation confirmed", sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), "job", "tenant", indexJobStatusRunning).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("DELETE FROM index_job_files").WithArgs("job").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()
		mock.ExpectQuery("SELECT id::text, workspace_id").WithArgs("job", "tenant").WillReturnRows(activeIndexJobRow(time.Now()))
		expectPublicationRelease(mock)
		if err := reconcileIndexPublication(context.Background(), publicationCandidate{id: "job", user: "tenant", root: "root", since: sql.NullTime{Valid: true, Time: time.Now().Add(-2 * indexPublicationMaxAge)}}); err != nil {
			t.Fatal(err)
		}
	})
	if !reflect.DeepEqual(calls, []string{"status", "abort"}) {
		t.Fatal(calls)
	}
}

func TestPublicationCompletionWinsCancellationRace(t *testing.T) {
	var calls []string
	publicationUpstream(t, map[string]string{"status": `{"state":"leased"}`, "abort": `{"state":"published"}`}, &calls)
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		expectPublicationLease(mock, true)
		// No failure write or deletion is allowed when the upstream committed first.
		expectPublicationRelease(mock)
		if err := reconcileIndexPublication(context.Background(), publicationCandidate{id: "job", user: "tenant", root: "root", since: sql.NullTime{Valid: true, Time: time.Now().Add(-2 * indexPublicationMaxAge)}}); err != nil {
			t.Fatal(err)
		}
	})
}

func TestPublicationReconcileResubmitsUnsubmittedIntent(t *testing.T) {
	var calls []string
	publicationUpstream(t, map[string]string{"status": `{"state":"not_submitted"}`, "publish": `{"state":"publishing"}`}, &calls)
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		expectPublicationLease(mock, true)
		expectPublicationPreflight(mock)
		mock.ExpectExec("UPDATE index_jobs SET phase = 'publishing'").WithArgs("job", "tenant", indexJobStatusRunning).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery("SELECT id::text, workspace_id").WithArgs("job", "tenant").WillReturnRows(activeIndexJobRow(time.Now()))
		expectPublicationRelease(mock)
		if err := reconcileIndexPublication(context.Background(), publicationCandidate{id: "job", user: "tenant", root: "root"}); err != nil {
			t.Fatal(err)
		}
	})
	if !reflect.DeepEqual(calls, []string{"status", "publish"}) {
		t.Fatalf("calls: %v", calls)
	}
}

func TestPublicationReconcileConsumesCompletedRevisionWithoutRepublishing(t *testing.T) {
	var calls []string
	publicationUpstream(t, map[string]string{"status": `{"state":"completed","revision":42}`}, &calls)
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		expectPublicationLease(mock, true)
		expectPublicationPreflight(mock)
		expectIndexUserLock(mock, "tenant")
		mock.ExpectQuery("SELECT workspace_id, workspace_name").WithArgs("job", "tenant").WillReturnRows(sqlmock.NewRows([]string{"workspace_id", "workspace_name", "branch", "revision", "status", "total_files", "indexed_files", "deleted_count", "deletions_sent", "chunk_count_fallback"}).AddRow("workspace", "Workspace", "feature", "git-sha", "running", 1, 1, 0, true, false))
		mock.ExpectExec("DELETE FROM indexed_files").WithArgs("tenant", "workspace").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("INSERT INTO indexed_files").WithArgs("tenant", "workspace", "job").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("INSERT INTO index_workspaces").WithArgs("tenant", "workspace", "Workspace", "root", "feature", "git-sha", int64(42)).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("UPDATE index_jobs").WithArgs("job", int64(42), indexJobStatusCompleted, true).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("DELETE FROM index_job_files").WithArgs("job").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()
		mock.ExpectQuery("SELECT id::text, workspace_id").WithArgs("job", "tenant").WillReturnRows(activeIndexJobRow(time.Now()))
		expectPublicationRelease(mock)
		if err := reconcileIndexPublication(context.Background(), publicationCandidate{id: "job", user: "tenant", root: "root"}); err != nil {
			t.Fatal(err)
		}
	})
	if !reflect.DeepEqual(calls, []string{"status"}) {
		t.Fatalf("calls: %v", calls)
	}
}

func TestBeginLCEIndexJobRetriesTransientPoolTimeout(t *testing.T) {
	previousBackoff := lceBeginRetryBackoff
	lceBeginRetryBackoff = [...]time.Duration{time.Millisecond, time.Millisecond}
	t.Cleanup(func() { lceBeginRetryBackoff = previousBackoff })

	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		text := `{"ok":true}`
		isError := false
		if calls < 3 {
			text = `{"error":{"message":"timeout exceeded when trying to connect"}}`
			isError = true
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": 1, "result": map[string]interface{}{
			"isError": isError,
			"content": []interface{}{map[string]string{"type": "text", "text": text}},
		}})
	}))
	previous, previousURL := lce, lceMCPURL
	lce, lceMCPURL = &mcpClient{http: server.Client(), sessionID: "begin-retry"}, server.URL
	t.Cleanup(func() { lce, lceMCPURL = previous, previousURL; server.Close() })

	if err := beginLCEIndexJob(context.Background(), "tenant", "job", "root", false); err != nil {
		t.Fatalf("begin should succeed after transient pool timeouts: %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 2 retries then success, got %d calls", calls)
	}
}

func TestBeginLCEIndexJobDoesNotRetryPermanentErrors(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": 1, "result": map[string]interface{}{
			"isError": true,
			"content": []interface{}{map[string]string{"type": "text", "text": `{"error":{"message":"cloud embedding space changed"}}`}},
		}})
	}))
	previous, previousURL := lce, lceMCPURL
	lce, lceMCPURL = &mcpClient{http: server.Client(), sessionID: "begin-permanent"}, server.URL
	t.Cleanup(func() { lce, lceMCPURL = previous, previousURL; server.Close() })

	err := beginLCEIndexJob(context.Background(), "tenant", "job", "root", false)
	if err == nil || !errors.As(err, new(*indexUpstreamError)) {
		t.Fatalf("expected upstream error, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("permanent errors must not be retried, got %d calls", calls)
	}
}
