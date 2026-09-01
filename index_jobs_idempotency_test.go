package main

import (
	"context"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func expectIndexUserLock(mock sqlmock.Sqlmock, userID string) {
	mock.ExpectBegin()
	mock.ExpectExec("pg_advisory_xact_lock").
		WithArgs(userID).
		WillReturnResult(sqlmock.NewResult(0, 0))
}

func activeIndexJobRow(now time.Time) *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"id", "workspace_id", "workspace_name", "root_id", "branch", "revision", "mode",
		"phase", "status", "workspace_files", "total_files", "indexed_files", "failed_files",
		"total_chunks", "indexed_chunks", "chunk_count_fallback", "deleted_count", "error",
		"error_code", "error_origin", "recovery", "cloud_revision", "started_at", "heartbeat_at", "completed_at",
	}).AddRow(
		"job-active", "workspace-a", "Workspace A", "root-a", "main", "revision-a", "incremental",
		"indexing", indexJobStatusRunning, 12, 5, 2, 0,
		int64(20), int64(8), false, 0, "", "", "", "", int64(1),
		now.Add(-time.Minute), now, nil,
	)
}

func TestIndexJobHeartbeatExpiredUsesShortCreatedWindow(t *testing.T) {
	now := time.Now()
	createdHeartbeat := now.Add(-indexJobInitialUploadTimeout - time.Second)
	if !indexJobHeartbeatExpired("created", createdHeartbeat, now) {
		t.Fatal("created job must expire when the client never uploads its first batch")
	}
	if indexJobHeartbeatExpired("indexing", createdHeartbeat, now) {
		t.Fatal("active upload/index work must retain the normal heartbeat window")
	}
	if got := timedOutIndexJobError("created"); got != "index client disconnected before first upload" {
		t.Fatalf("unexpected created timeout error: %q", got)
	}
}

func TestGetIndexJobStatusIsReadOnly(t *testing.T) {
	now := time.Now()
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("SELECT id::text, workspace_id, workspace_name, root_id").
			WithArgs("job-active", "user-a").
			WillReturnRows(activeIndexJobRow(now))

		job, err := getIndexJob(context.Background(), "user-a", "job-active")
		if err != nil {
			t.Fatalf("getIndexJob: %v", err)
		}
		if job.ID != "job-active" || job.Status != indexJobStatusRunning {
			t.Fatalf("unexpected job: %#v", job)
		}
	})
}

func TestInspectActiveIndexJobReturnsBusyOwnerWithoutMutatingIt(t *testing.T) {
	now := time.Now()
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		expectIndexUserLock(mock, "user-a")
		mock.ExpectQuery("SELECT id::text, root_id, cloud_revision, phase, heartbeat_at").
			WithArgs("user-a", "workspace-a", indexJobStatusRunning).
			WillReturnRows(sqlmock.NewRows([]string{"id", "root_id", "cloud_revision", "phase", "heartbeat_at"}).
				AddRow("job-active", "root-a", int64(1), "indexing", now))
		mock.ExpectQuery("SELECT id::text, workspace_id, workspace_name, root_id").
			WithArgs("job-active", "user-a").
			WillReturnRows(activeIndexJobRow(now))
		mock.ExpectRollback()

		job, err := inspectActiveIndexJob(context.Background(), "user-a", "workspace-a")
		if err != nil {
			t.Fatalf("inspectActiveIndexJob: %v", err)
		}
		if job == nil || job.ID != "job-active" || job.Phase != "indexing" {
			t.Fatalf("unexpected active job: %#v", job)
		}
	})
}

func TestInspectActiveIndexJobReclaimsExpiredOwnerBeforeReplacement(t *testing.T) {
	now := time.Now()
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		expectIndexUserLock(mock, "user-a")
		mock.ExpectQuery("SELECT id::text, root_id, cloud_revision, phase, heartbeat_at").
			WithArgs("user-a", "workspace-a", indexJobStatusRunning).
			WillReturnRows(sqlmock.NewRows([]string{"id", "root_id", "cloud_revision", "phase", "heartbeat_at"}).
				AddRow("job-expired", "root-a", int64(1), "indexing", now.Add(-indexJobHeartbeatTimeout-time.Minute)))
		mock.ExpectExec("UPDATE index_jobs").
			WithArgs(
				indexJobStatusTimedOut, "index job heartbeat timed out",
				"heartbeat_timeout", "relay", "restart_client",
				"job-expired", "user-a", indexJobStatusRunning,
			).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("DELETE FROM index_job_files WHERE job_id").
			WithArgs("job-expired").
			WillReturnResult(sqlmock.NewResult(0, 5))
		mock.ExpectCommit()

		job, err := inspectActiveIndexJob(context.Background(), "user-a", "workspace-a")
		if err != nil {
			t.Fatalf("inspectActiveIndexJob: %v", err)
		}
		if job != nil {
			t.Fatalf("expired job must not remain active: %#v", job)
		}
	})
}

func TestCreateIndexJobReturnsBusyWithoutCreatingProviderWork(t *testing.T) {
	now := time.Now()
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		// The request first owns the distributed start-decision lease.
		expectIndexUserLock(mock, "user-a")
		mock.ExpectQuery("WITH expired AS").
			WithArgs(
				sqlmock.AnyArg(), "user-a", "*", indexOperationExclusive, "create-job",
				indexOperationLeaseDuration.Milliseconds(), indexOperationExclusive,
			).
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
		mock.ExpectCommit()

		// A healthy job already owns this workspace. Returning it is terminal for
		// this start attempt: no workspace mutation, job INSERT, or LCE/provider
		// call may follow.
		expectIndexUserLock(mock, "user-a")
		mock.ExpectQuery("SELECT id::text, root_id, cloud_revision, phase, heartbeat_at").
			WithArgs("user-a", "workspace-a", indexJobStatusRunning).
			WillReturnRows(sqlmock.NewRows([]string{"id", "root_id", "cloud_revision", "phase", "heartbeat_at"}).
				AddRow("job-active", "root-a", int64(1), "indexing", now))
		mock.ExpectQuery("SELECT id::text, workspace_id, workspace_name, root_id").
			WithArgs("job-active", "user-a").
			WillReturnRows(activeIndexJobRow(now))
		mock.ExpectRollback()

		mock.ExpectExec("DELETE FROM index_operation_leases").
			WithArgs("user-a", sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 1))

		response, err := createIndexJob(context.Background(), "user-a", indexStartRequest{
			ProtocolVersion: indexProtocolVersion,
			WorkspaceID:     "workspace-a",
			WorkspaceName:   "Workspace A",
			RootID:          "root-a",
			Branch:          "main",
			Revision:        "revision-b",
			Files: []indexManifestFile{{
				Path:            "main.ts",
				Hash:            "hash-b",
				Size:            10,
				EstimatedChunks: 1,
			}},
		})
		if err != nil {
			t.Fatalf("createIndexJob: %v", err)
		}
		if !response.Busy || response.BusyReason != indexStartBusyActiveJob || response.ActiveJob == nil {
			t.Fatalf("unexpected busy response: %#v", response)
		}
		if response.Job != nil || response.Unchanged {
			t.Fatalf("busy start must not create or publish another job: %#v", response)
		}
	})
}

func TestCreateIndexJobReturnsUnchangedWithoutCreatingProviderWork(t *testing.T) {
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		// Distributed exclusive lease: this serializes the complete start decision
		// across relay instances, including the no-op fast path.
		expectIndexUserLock(mock, "user-a")
		mock.ExpectQuery("WITH expired AS").
			WithArgs(
				sqlmock.AnyArg(), "user-a", "*", indexOperationExclusive, "create-job",
				indexOperationLeaseDuration.Milliseconds(), indexOperationExclusive,
			).
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
		mock.ExpectCommit()

		// No healthy or expired job currently owns this workspace.
		expectIndexUserLock(mock, "user-a")
		mock.ExpectQuery("SELECT id::text, root_id, cloud_revision, phase, heartbeat_at").
			WithArgs("user-a", "workspace-a", indexJobStatusRunning).
			WillReturnRows(sqlmock.NewRows([]string{"id", "root_id", "cloud_revision", "phase", "heartbeat_at"}))
		mock.ExpectCommit()

		mock.ExpectQuery("SELECT root_id FROM index_workspaces").
			WithArgs("user-a", "workspace-a").
			WillReturnRows(sqlmock.NewRows([]string{"root_id"}).AddRow("root-a"))

		expectIndexUserLock(mock, "user-a")
		mock.ExpectQuery("SELECT path, hash, size, estimated_chunks").
			WithArgs("user-a", "workspace-a").
			WillReturnRows(sqlmock.NewRows([]string{"path", "hash", "size", "estimated_chunks"}).
				AddRow("main.ts", "hash-a", int64(10), 1))
		mock.ExpectQuery("SELECT EXISTS").
			WithArgs("user-a", "workspace-a").
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
		mock.ExpectExec("UPDATE index_workspaces").
			WithArgs("user-a", "workspace-a", "Workspace A", "root-a", "main", "revision-b").
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()

		// Releasing the distributed lease is the final database operation. Any
		// INSERT into index_jobs or LCE/provider-related query is unexpected.
		mock.ExpectExec("DELETE FROM index_operation_leases").
			WithArgs("user-a", sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 1))

		response, err := createIndexJob(context.Background(), "user-a", indexStartRequest{
			ProtocolVersion: indexProtocolVersion,
			WorkspaceID:     "workspace-a",
			WorkspaceName:   "Workspace A",
			RootID:          "root-a",
			Branch:          "main",
			Revision:        "revision-b",
			Files: []indexManifestFile{{
				Path:            "main.ts",
				Hash:            "hash-a",
				Size:            10,
				EstimatedChunks: 1,
			}},
		})
		if err != nil {
			t.Fatalf("createIndexJob: %v", err)
		}
		if !response.Unchanged || response.Job != nil || response.Busy {
			t.Fatalf("unexpected no-op response: %#v", response)
		}
	})
}
