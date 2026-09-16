package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func rootDeletionCloudResult(job rootDeletionJob, count int64) *mcpToolResult {
	content, _ := json.Marshal(map[string]interface{}{
		"operationId": job.ID, "rootId": job.RootID, "deleted": count > 0, "deletedFiles": count,
	})
	return &mcpToolResult{Content: content}
}

func TestRootDeletionReceiptRequiresExactOperationAndRoot(t *testing.T) {
	job := rootDeletionTestJob("running")
	for _, body := range []string{`{}`, `{"deleted":true}`, `null`, `[]`, `{"operationId":"another"}`} {
		if _, err := rootDeletionReceipt([]byte(body), job); err == nil {
			t.Fatalf("accepted unconfirmed deletion: %s", body)
		}
	}
	for _, count := range []int64{0, 42} {
		result := rootDeletionCloudResult(job, count)
		if got, err := rootDeletionReceipt(result.Content, job); err != nil || got != count {
			t.Fatalf("receipt=%d error=%v", got, err)
		}
		other := job
		other.RootID = "another-root"
		if _, err := rootDeletionReceipt(result.Content, other); err == nil {
			t.Fatal("accepted another root's receipt")
		}
		other = job
		other.ID = "another-operation"
		if _, err := rootDeletionReceipt(result.Content, other); err == nil {
			t.Fatal("accepted another operation's receipt")
		}
	}
}

func TestRootDeletionRetryBudgetPausesWithoutReleasingFence(t *testing.T) {
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		job := rootDeletionTestJob("running")
		job.AttemptCount = rootDeletionMaxAttempts
		mock.ExpectExec(`(?s)UPDATE root_deletion_jobs SET status = \$4.*recovery_required=\$7.*attempt_id = \$3.*claim_until > NOW\(\)`).
			WithArgs(job.ID, job.TenantID, job.AttemptID, "running", sqlmock.AnyArg(), rootDeletionMaxAttempts, true, rootDeletionRetryDelay(rootDeletionMaxAttempts).Milliseconds()).
			WillReturnResult(sqlmock.NewResult(0, 1))
		recordRootDeletionError(job, false, "response lost")
	})
}

func TestRootDeletionLeaseContentionRefundsAttemptAndReschedules(t *testing.T) {
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		job := rootDeletionTestJob("running")
		stubRootDeletionLease(t, func(context.Context, string) (deleteRootOperationLease, error) { return nil, errIndexOperationBusy })
		mock.ExpectExec(`(?s)UPDATE root_deletion_jobs.*attempt_count=\$6.*next_attempt_at=NOW\(\)`).
			WithArgs(job.ID, job.TenantID, job.AttemptID, "running", sqlmock.AnyArg(), 0, false, int64(5000)).
			WillReturnResult(sqlmock.NewResult(0, 1))
		processRootDeletion(context.Background(), job)
	})
}

func TestRootDeletionExplicitRecoveryKeepsIdentity(t *testing.T) {
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		job := rootDeletionTestJob("running")
		job.AttemptCount = rootDeletionMaxAttempts
		job.RecoveryRequired = true
		expectIndexOperationLock(mock, job.TenantID)
		mock.ExpectQuery("FROM root_deletion_jobs WHERE user_id = ").WithArgs(job.TenantID).WillReturnRows(rootDeletionTestRows(job))
		resumed := job
		resumed.AttemptCount = 0
		resumed.RecoveryRequired = false
		mock.ExpectQuery(`(?s)UPDATE root_deletion_jobs.*recovery_required=FALSE.*WHERE id=\$1 AND user_id=\$2 AND recovery_required`).
			WithArgs(job.ID, job.TenantID).WillReturnRows(rootDeletionTestRows(resumed))
		mock.ExpectCommit()
		got, err := submitRootDeletion(context.Background(), job.TenantID, job.ActorID, job.RootID, job.ID)
		if err != nil || got.ID != job.ID || got.RecoveryRequired {
			t.Fatalf("recovery=%+v error=%v", got, err)
		}
	})
}

func TestRootDeletionRepeatedRecoveryAfterSuccessDoesNotEnqueue(t *testing.T) {
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		job := rootDeletionTestJob("succeeded")
		expectIndexOperationLock(mock, job.TenantID)
		mock.ExpectQuery("FROM root_deletion_jobs WHERE user_id = ").WithArgs(job.TenantID).WillReturnRows(rootDeletionTestRows())
		mock.ExpectQuery(`FROM root_deletion_jobs WHERE id=\$1 AND user_id=\$2 AND root_id=\$3`).
			WithArgs(job.ID, job.TenantID, job.RootID).WillReturnRows(rootDeletionTestRows(job))
		mock.ExpectCommit()
		got, err := submitRootDeletion(context.Background(), job.TenantID, job.ActorID, job.RootID, job.ID)
		if err != nil || got.Status != "succeeded" || got.ID != job.ID {
			t.Fatalf("replay=%+v error=%v", got, err)
		}
	})
}

func TestRootDeletionRetryBackoffIsBounded(t *testing.T) {
	if rootDeletionRetryDelay(1) != 5*time.Second || rootDeletionRetryDelay(5) != 80*time.Second || rootDeletionRetryDelay(1000) != 10*time.Minute {
		t.Fatal("unexpected retry schedule")
	}
}
