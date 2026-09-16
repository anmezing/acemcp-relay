package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func platformConfigTestJob(embeddingChanged bool) platformConfigJob {
	return platformConfigJob{ID: "22ddeac8-e2d2-4273-8d2b-f0c04c616e65", Section: "promptEnhancer", EmbeddingChanged: embeddingChanged,
		Status: "running", AttemptID: "f223a6b2-a315-4c52-b40b-5dc43df041ba", AttemptCount: 1, Result: json.RawMessage(`null`), UpdatedAt: time.Now()}
}

func platformConfigTestRows(jobs ...platformConfigJob) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{"id", "section", "embedding_changed", "status", "attempt_id", "attempt_count", "recovery_required", "error", "result", "updated_at"})
	for _, job := range jobs {
		rows.AddRow(job.ID, job.Section, job.EmbeddingChanged, job.Status, job.AttemptID, job.AttemptCount, job.RecoveryRequired, job.Error, []byte(job.Result), job.UpdatedAt)
	}
	return rows
}

func expectPlatformConfigFinishStart(mock sqlmock.Sqlmock, job platformConfigJob) {
	mock.ExpectBegin()
	mock.ExpectExec("pg_advisory_xact_lock").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT id::text FROM platform_config_jobs").WithArgs(job.ID, job.AttemptID).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(job.ID))
}

func TestPlatformConfigCleanupAndReceiptAreAtomic(t *testing.T) {
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		job := platformConfigTestJob(true)
		expectPlatformConfigFinishStart(mock, job)
		for _, table := range []string{"index_operation_leases", "index_jobs", "indexed_files", "index_workspaces"} {
			mock.ExpectExec("DELETE FROM " + table).WillReturnResult(sqlmock.NewResult(0, 2))
		}
		mock.ExpectExec("UPDATE platform_config_jobs SET status=").WithArgs(job.ID, job.AttemptID, "succeeded", sqlmock.AnyArg(), "").WillReturnError(errors.New("receipt write failed"))
		mock.ExpectRollback()
		receipt := cloudPlatformConfigOperation{OperationID: job.ID, State: "completed", EmbeddingChanged: true, Result: json.RawMessage(`{"deletedRoots":2,"deletedFiles":2,"deletedCachedEmbeddings":0}`)}
		if err := finishPlatformConfigJob(context.Background(), job, receipt); err == nil {
			t.Fatal("failed receipt did not roll back cleanup")
		}
	})
}

func TestPlatformConfigDurableRejectionReleasesFenceWithoutDeleting(t *testing.T) {
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		job := platformConfigTestJob(true)
		expectPlatformConfigFinishStart(mock, job)
		mock.ExpectExec("UPDATE platform_config_jobs SET status=").WithArgs(job.ID, job.AttemptID, "rejected", sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()
		if err := finishPlatformConfigJob(context.Background(), job, cloudPlatformConfigOperation{OperationID: job.ID, State: "rejected", EmbeddingChanged: true}); err != nil {
			t.Fatal(err)
		}
	})
}

func TestPlatformConfigUnconfirmedReceiptNeverClearsState(t *testing.T) {
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		job := platformConfigTestJob(true)
		for _, receipt := range []cloudPlatformConfigOperation{
			{OperationID: job.ID, State: "prepared", EmbeddingChanged: true},
			{OperationID: "other", State: "completed", EmbeddingChanged: true},
			{OperationID: job.ID, State: "completed", EmbeddingChanged: false},
		} {
			if err := finishPlatformConfigJob(context.Background(), job, receipt); err == nil {
				t.Fatalf("accepted invalid receipt: %+v", receipt)
			}
		}
	})
}

func TestPlatformConfigRetryBudgetKeepsDurableFence(t *testing.T) {
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		job := platformConfigTestJob(true)
		job.AttemptCount = platformConfigJobMaxAttempts
		mock.ExpectExec(`(?s)UPDATE platform_config_jobs SET claim_until=NOW\(\).*recovery_required=\$4.*status='running'`).
			WithArgs(job.ID, job.AttemptID, rootDeletionRetryDelay(job.AttemptCount).Milliseconds(), true, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
		deferPlatformConfigJob(job, "lost commit response")
	})
}
