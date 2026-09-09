package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func rootDeletionTestJob(status string) rootDeletionJob {
	return rootDeletionJob{
		ID: "1aa907f1-5a18-4559-8939-ac6f6db93091", TenantID: "tenant-a", ActorID: "actor-a",
		RootID: "repo-a", Status: status, AttemptID: "ea47c3d4-e29e-4571-ac34-88779d01e478",
		CreatedAt: time.Date(2026, time.September, 8, 1, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2026, time.September, 8, 1, 1, 0, 0, time.UTC),
	}
}

func rootDeletionTestRows(jobs ...rootDeletionJob) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{
		"id", "user_id", "actor_id", "root_id", "status", "attempt_id", "deleted_files", "error", "created_at", "updated_at",
	})
	for _, job := range jobs {
		rows.AddRow(job.ID, job.TenantID, job.ActorID, job.RootID, job.Status,
			job.AttemptID, job.DeletedFiles, job.Error, job.CreatedAt, job.UpdatedAt)
	}
	return rows
}

func expectRootDeletionAdmission(mock sqlmock.Sqlmock, tenantID string, busy bool) {
	expectIndexOperationLock(mock, tenantID)
	mock.ExpectQuery(`FROM root_deletion_jobs WHERE user_id = \$1 AND status IN \('queued', 'running'\)`).
		WithArgs(tenantID).WillReturnRows(rootDeletionTestRows())
	mock.ExpectQuery(`(?s)EXISTS\(SELECT 1 FROM index_operation_leases WHERE user_id = \$1 AND lease_expires_at > NOW\(\)\).*OR EXISTS\(SELECT 1 FROM index_jobs WHERE user_id = \$1 AND status = 'running'\)`).
		WithArgs(tenantID).WillReturnRows(sqlmock.NewRows([]string{"busy"}).AddRow(busy))
}

func expectRootDeletionCurrent(mock sqlmock.Sqlmock, job rootDeletionJob, current bool) {
	mock.ExpectQuery(`(?s)SELECT EXISTS\(SELECT 1 FROM root_deletion_jobs.*id = \$1 AND user_id = \$2 AND attempt_id = \$3.*status = 'running' AND claim_until > NOW\(\)`).
		WithArgs(job.ID, job.TenantID, job.AttemptID).
		WillReturnRows(sqlmock.NewRows([]string{"current"}).AddRow(current))
}

func expectRootDeletionError(mock sqlmock.Sqlmock, job rootDeletionJob, status string) {
	mock.ExpectExec(`(?s)UPDATE root_deletion_jobs SET status = \$4, error = \$5.*id = \$1 AND user_id = \$2 AND attempt_id = \$3 AND status = 'running' AND claim_until > NOW\(\)`).
		WithArgs(job.ID, job.TenantID, job.AttemptID, status, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
}

func expectRootDeletionFinishStart(mock sqlmock.Sqlmock, job rootDeletionJob, current bool) {
	expectIndexOperationLock(mock, job.TenantID)
	rows := sqlmock.NewRows([]string{"id"})
	if current {
		rows.AddRow(job.ID)
	}
	mock.ExpectQuery(`(?s)SELECT id::text FROM root_deletion_jobs.*id = \$1 AND user_id = \$2 AND attempt_id = \$3.*status = 'running' AND claim_until > NOW\(\) FOR UPDATE`).
		WithArgs(job.ID, job.TenantID, job.AttemptID).WillReturnRows(rows)
}

func expectRootDeletionCleanup(mock sqlmock.Sqlmock, job rootDeletionJob, count int64) {
	mock.ExpectExec("DELETE FROM index_jobs").WithArgs(job.TenantID, job.RootID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("DELETE FROM indexed_files AS files").WithArgs(job.TenantID, job.RootID).
		WillReturnResult(sqlmock.NewResult(0, count))
	mock.ExpectExec("DELETE FROM index_workspaces").WithArgs(job.TenantID, job.RootID).
		WillReturnResult(sqlmock.NewResult(0, 1))
}

func expectRootDeletionSuccess(mock sqlmock.Sqlmock, job rootDeletionJob, count int64) {
	mock.ExpectExec(`(?s)UPDATE root_deletion_jobs SET status = 'succeeded'.*claim_until = NULL.*id = \$1 AND user_id = \$2 AND attempt_id = \$3`).
		WithArgs(job.ID, job.TenantID, job.AttemptID, count).
		WillReturnResult(sqlmock.NewResult(0, 1))
}

func stubRootDeletionLease(t *testing.T, fn func(context.Context, string) (deleteRootOperationLease, error)) {
	t.Helper()
	previous := acquireRootDeletionLease
	acquireRootDeletionLease = fn
	t.Cleanup(func() { acquireRootDeletionLease = previous })
}

func TestRootDeletionCreateRejectsUnauthorizedAndInvalidRequests(t *testing.T) {
	for _, test := range []struct {
		name, actorID, orgID, role, body string
		status                           int
	}{
		{name: "unauthenticated", body: `{"root_id":"repo-a"}`, status: http.StatusUnauthorized},
		{name: "empty root", actorID: "actor-a", body: `{"root_id":"  "}`, status: http.StatusBadRequest},
		{name: "malformed body", actorID: "actor-a", body: `{`, status: http.StatusBadRequest},
		{name: "long root", actorID: "actor-a", body: `{"root_id":"` + strings.Repeat("r", 129) + `"}`, status: http.StatusBadRequest},
		{name: "org member", actorID: "actor-a", orgID: "tenant-a", role: "member", body: `{"root_id":"repo-a"}`, status: http.StatusForbidden},
		{name: "org missing role", actorID: "actor-a", orgID: "tenant-a", body: `{"root_id":"repo-a"}`, status: http.StatusForbidden},
	} {
		t.Run(test.name, func(t *testing.T) {
			withMockDB(t, func(mock sqlmock.Sqlmock) {
				stubLCEClearIndexRoot(t, func(context.Context, string, string) (*mcpToolResult, error) {
					t.Fatal("rejected admission must not reach Cloud")
					return nil, nil
				})
				c, recorder := newRootAdminContext(t, test.actorID, "POST", test.body)
				c.Set(ContextKeyOrgID, test.orgID)
				c.Set(ContextKeyOrgRole, test.role)
				handleCreateRootDeletion(c)
				if recorder.Code != test.status {
					t.Fatalf("status=%d want=%d body=%s", recorder.Code, test.status, recorder.Body.String())
				}
			})
		})
	}
}

func TestRootDeletionCreateQueuesForAuthenticatedTenantWithoutCloudCall(t *testing.T) {
	for _, orgID := range []string{"", "tenant-a"} {
		t.Run("org="+orgID, func(t *testing.T) {
			resetDeleteRootRateLimit()
			withMockDB(t, func(mock sqlmock.Sqlmock) {
				job := rootDeletionTestJob("queued")
				job.AttemptID = ""
				if orgID == "" {
					job.TenantID = job.ActorID
				}
				expectRootDeletionAdmission(mock, job.TenantID, false)
				mock.ExpectQuery("INSERT INTO root_deletion_jobs").
					WithArgs(sqlmock.AnyArg(), job.TenantID, job.ActorID, job.RootID).
					WillReturnRows(rootDeletionTestRows(job))
				mock.ExpectCommit()
				stubLCEClearIndexRoot(t, func(context.Context, string, string) (*mcpToolResult, error) {
					t.Fatal("admission must commit the task without waiting for Cloud")
					return nil, nil
				})
				c, recorder := newRootAdminContext(t, job.ActorID, "POST", `{"root_id":" repo-a "}`)
				c.Set(ContextKeyOrgID, orgID)
				c.Set(ContextKeyTenantID, job.TenantID)
				c.Set(ContextKeyOrgRole, orgRoleOwner)
				handleCreateRootDeletion(c)
				if recorder.Code != http.StatusAccepted {
					t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
				}
				if recorder.Header().Get("Cache-Control") != "no-store" || recorder.Header().Get("Retry-After") == "" {
					t.Fatalf("missing polling/cache headers: %v", recorder.Header())
				}
				var response struct {
					Deletion rootDeletionJob `json:"deletion"`
				}
				if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil || response.Deletion.ID != job.ID || response.Deletion.Status != "queued" {
					t.Fatalf("unexpected queued response: %s error=%v", recorder.Body.String(), err)
				}
			})
		})
	}
}

func TestRootDeletionDuplicateReturnsExistingTaskDespiteRateLimit(t *testing.T) {
	for _, status := range []string{"queued", "running"} {
		t.Run(status, func(t *testing.T) {
			resetDeleteRootRateLimit()
			job := rootDeletionTestJob(status)
			checkDeleteRootRateLimit(job.TenantID, job.RootID, time.Now())
			withMockDB(t, func(mock sqlmock.Sqlmock) {
				expectIndexOperationLock(mock, job.TenantID)
				mock.ExpectQuery("FROM root_deletion_jobs WHERE user_id = ").WithArgs(job.TenantID).
					WillReturnRows(rootDeletionTestRows(job))
				mock.ExpectCommit()
				stubLCEClearIndexRoot(t, func(context.Context, string, string) (*mcpToolResult, error) {
					t.Fatal("duplicate admission must not call Cloud")
					return nil, nil
				})
				c, recorder := newRootAdminContext(t, job.ActorID, "POST", `{"root_id":"repo-a"}`)
				c.Set(ContextKeyOrgID, job.TenantID)
				c.Set(ContextKeyTenantID, job.TenantID)
				c.Set(ContextKeyOrgRole, orgRoleOwner)
				handleCreateRootDeletion(c)
				if recorder.Code != http.StatusAccepted || !strings.Contains(recorder.Body.String(), job.ID) {
					t.Fatalf("duplicate status=%d body=%s", recorder.Code, recorder.Body.String())
				}
			})
		})
	}
}

func TestRootDeletionBusyAdmissionReturnsConflictWithoutChargingRateLimit(t *testing.T) {
	for _, reason := range []string{"active deletion for another root", "active tenant index operation"} {
		t.Run(reason, func(t *testing.T) {
			resetDeleteRootRateLimit()
			job := rootDeletionTestJob("running")
			withMockDB(t, func(mock sqlmock.Sqlmock) {
				if reason == "active deletion for another root" {
					expectIndexOperationLock(mock, job.TenantID)
					other := job
					other.RootID = "repo-b"
					mock.ExpectQuery("FROM root_deletion_jobs WHERE user_id = ").WithArgs(job.TenantID).
						WillReturnRows(rootDeletionTestRows(other))
				} else {
					expectRootDeletionAdmission(mock, job.TenantID, true)
				}
				mock.ExpectRollback()
				c, recorder := newRootAdminContext(t, job.ActorID, "POST", `{"root_id":"repo-a"}`)
				c.Set(ContextKeyOrgID, job.TenantID)
				c.Set(ContextKeyTenantID, job.TenantID)
				c.Set(ContextKeyOrgRole, orgRoleOwner)
				handleCreateRootDeletion(c)
				if recorder.Code != http.StatusConflict {
					t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
				}
			})
			if wait := checkDeleteRootRateLimit(job.TenantID, job.RootID, time.Now()); wait != 0 {
				t.Fatalf("conflict consumed rate window: %d", wait)
			}
		})
	}
}

func TestRootDeletionRateLimitReturns429WithoutCreatingTask(t *testing.T) {
	resetDeleteRootRateLimit()
	job := rootDeletionTestJob("queued")
	checkDeleteRootRateLimit(job.TenantID, job.RootID, time.Now())
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		expectRootDeletionAdmission(mock, job.TenantID, false)
		mock.ExpectRollback()
		c, recorder := newRootAdminContext(t, job.ActorID, "POST", `{"root_id":"repo-a"}`)
		c.Set(ContextKeyOrgID, job.TenantID)
		c.Set(ContextKeyTenantID, job.TenantID)
		c.Set(ContextKeyOrgRole, orgRoleOwner)
		handleCreateRootDeletion(c)
		if recorder.Code != http.StatusTooManyRequests || recorder.Header().Get("Retry-After") != "60" {
			t.Fatalf("status=%d headers=%v body=%s", recorder.Code, recorder.Header(), recorder.Body.String())
		}
	})
}

func TestRootDeletionListUsesTenantAndHidesPrivateFields(t *testing.T) {
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		job := rootDeletionTestJob("running")
		mock.ExpectQuery(`(?s)SELECT DISTINCT ON \(root_id\).*FROM root_deletion_jobs WHERE user_id = \$1.*LIMIT 100`).
			WithArgs(job.TenantID).WillReturnRows(rootDeletionTestRows(job))
		c, recorder := newRootAdminContext(t, job.ActorID, "GET", "")
		c.Set(ContextKeyOrgID, job.TenantID)
		c.Set(ContextKeyTenantID, job.TenantID)
		c.Set(ContextKeyOrgRole, "member")
		handleListRootDeletions(c)
		if recorder.Code != http.StatusOK || recorder.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
		}
		var response struct {
			Deletions []map[string]interface{} `json:"deletions"`
		}
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil || len(response.Deletions) != 1 {
			t.Fatalf("unexpected list: %s error=%v", recorder.Body.String(), err)
		}
		for _, field := range []string{"tenant_id", "user_id", "actor_id", "attempt_id", "claim_until"} {
			if _, exists := response.Deletions[0][field]; exists {
				t.Fatalf("private field %q exposed: %s", field, recorder.Body.String())
			}
		}
		for _, secret := range []string{job.TenantID, job.ActorID, job.AttemptID} {
			if strings.Contains(recorder.Body.String(), secret) {
				t.Fatalf("private value exposed: %s", recorder.Body.String())
			}
		}
	})
}

func TestRootDeletionListUnauthenticatedAndDatabaseErrors(t *testing.T) {
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		c, recorder := newRootAdminContext(t, "", "GET", "")
		handleListRootDeletions(c)
		if recorder.Code != http.StatusUnauthorized {
			t.Fatalf("unauthenticated status=%d", recorder.Code)
		}
	})
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("FROM root_deletion_jobs WHERE user_id = ").WithArgs("actor-a").
			WillReturnError(errors.New("private database detail"))
		c, recorder := newRootAdminContext(t, "actor-a", "GET", "")
		handleListRootDeletions(c)
		if recorder.Code != http.StatusServiceUnavailable || strings.Contains(recorder.Body.String(), "private database detail") {
			t.Fatalf("unsafe database error response: status=%d body=%s", recorder.Code, recorder.Body.String())
		}
	})
}

func TestRootDeletionClaimUsesSkipLockedAndRecoversExpiredRunningTasks(t *testing.T) {
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		job := rootDeletionTestJob("running")
		mock.ExpectQuery(`(?s)WITH candidate AS \(.*status = 'queued' OR \(status = 'running' AND claim_until < NOW\(\)\).*LIMIT 1 FOR UPDATE SKIP LOCKED.*SET status = 'running', attempt_id = \$1.*claim_until = NOW\(\) \+ \(\$2 \* INTERVAL '1 millisecond'\)`).
			WithArgs(sqlmock.AnyArg(), rootDeletionClaimDuration.Milliseconds()).WillReturnRows(rootDeletionTestRows(job))
		claimed, err := claimRootDeletion(context.Background())
		if err != nil || claimed.ID != job.ID || claimed.AttemptID != job.AttemptID || claimed.Status != "running" {
			t.Fatalf("claim=%+v error=%v", claimed, err)
		}
		mock.ExpectQuery("WITH candidate AS").
			WithArgs(sqlmock.AnyArg(), rootDeletionClaimDuration.Milliseconds()).WillReturnRows(rootDeletionTestRows())
		if _, err := claimRootDeletion(context.Background()); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("empty queue error=%v", err)
		}
	})
}

func TestRootDeletionStaleAttemptDoesNotReachCloudAndReleasesLease(t *testing.T) {
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		job := rootDeletionTestJob("running")
		released := false
		stubRootDeletionLease(t, func(ctx context.Context, tenantID string) (deleteRootOperationLease, error) {
			if tenantID != job.TenantID {
				t.Fatalf("lease tenant=%s", tenantID)
			}
			return noopDeleteRootLease{ctx: ctx, release: func() { released = true }}, nil
		})
		expectRootDeletionCurrent(mock, job, false)
		stubLCEClearIndexRoot(t, func(context.Context, string, string) (*mcpToolResult, error) {
			t.Fatal("stale claim must not delete Cloud state")
			return nil, nil
		})
		processRootDeletion(context.Background(), job)
		if !released {
			t.Fatal("stale claim leaked operation lease")
		}
	})
}

func TestRootDeletionUnknownOutcomeRetainsRunningMarker(t *testing.T) {
	for _, outcome := range []string{"network error", "empty response", "panic"} {
		t.Run(outcome, func(t *testing.T) {
			withMockDB(t, func(mock sqlmock.Sqlmock) {
				job := rootDeletionTestJob("running")
				parent, cancel := context.WithCancel(context.Background())
				defer cancel()
				released := false
				stubRootDeletionLease(t, func(ctx context.Context, _ string) (deleteRootOperationLease, error) {
					return noopDeleteRootLease{ctx: ctx, release: func() { released = true }}, nil
				})
				expectRootDeletionCurrent(mock, job, true)
				expectRootDeletionError(mock, job, "running")
				stubLCEClearIndexRoot(t, func(context.Context, string, string) (*mcpToolResult, error) {
					cancel()
					switch outcome {
					case "network error":
						return nil, errors.New("Cloud response lost after committing deletion")
					case "panic":
						panic("interrupted response handler")
					default:
						return nil, nil
					}
				})
				processRootDeletion(parent, job)
				if !released {
					t.Fatal("unknown outcome leaked the operation lease")
				}
			})
		})
	}
}

func TestRootDeletionDefinitiveCloudFailureMarksTaskFailed(t *testing.T) {
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		job := rootDeletionTestJob("running")
		stubRootDeletionLease(t, func(ctx context.Context, _ string) (deleteRootOperationLease, error) {
			return noopDeleteRootLease{ctx: ctx}, nil
		})
		expectRootDeletionCurrent(mock, job, true)
		expectRootDeletionError(mock, job, "failed")
		stubLCEClearIndexRoot(t, func(context.Context, string, string) (*mcpToolResult, error) {
			return &mcpToolResult{IsError: true, Content: []byte(`{"error":"transaction rolled back"}`)}, nil
		})
		processRootDeletion(context.Background(), job)
	})
}

func TestRootDeletionLeaseFailureLeavesTaskForRetry(t *testing.T) {
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		job := rootDeletionTestJob("running")
		stubRootDeletionLease(t, func(context.Context, string) (deleteRootOperationLease, error) {
			return nil, errIndexOperationBusy
		})
		expectRootDeletionError(mock, job, "running")
		stubLCEClearIndexRoot(t, func(context.Context, string, string) (*mcpToolResult, error) {
			t.Fatal("lease failure must not reach Cloud")
			return nil, nil
		})
		processRootDeletion(context.Background(), job)
	})
}

func TestRootDeletionRequestCancellationDoesNotCancelQueuedWork(t *testing.T) {
	resetDeleteRootRateLimit()
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		job := rootDeletionTestJob("queued")
		expectRootDeletionAdmission(mock, job.TenantID, false)
		mock.ExpectQuery("INSERT INTO root_deletion_jobs").
			WithArgs(sqlmock.AnyArg(), job.TenantID, job.ActorID, job.RootID).WillReturnRows(rootDeletionTestRows(job))
		mock.ExpectCommit()
		requestCtx, cancelRequest := context.WithCancel(context.Background())
		defer cancelRequest()
		c, recorder := newRootAdminContext(t, job.ActorID, "POST", `{"root_id":"repo-a"}`)
		c.Set(ContextKeyOrgID, job.TenantID)
		c.Set(ContextKeyTenantID, job.TenantID)
		c.Set(ContextKeyOrgRole, orgRoleOwner)
		c.Request = c.Request.WithContext(requestCtx)
		handleCreateRootDeletion(c)
		if recorder.Code != http.StatusAccepted {
			t.Fatalf("queue failed: %d %s", recorder.Code, recorder.Body.String())
		}
		cancelRequest()
		job.Status = "running"
		stubRootDeletionLease(t, func(ctx context.Context, _ string) (deleteRootOperationLease, error) {
			return noopDeleteRootLease{ctx: ctx}, nil
		})
		expectRootDeletionCurrent(mock, job, true)
		expectRootDeletionFinishStart(mock, job, true)
		expectRootDeletionCleanup(mock, job, 7)
		expectRootDeletionSuccess(mock, job, 7)
		mock.ExpectCommit()
		called := false
		stubLCEClearIndexRoot(t, func(ctx context.Context, tenantID, rootID string) (*mcpToolResult, error) {
			called = true
			if requestCtx.Err() == nil || ctx.Err() != nil || tenantID != job.TenantID || rootID != job.RootID {
				t.Fatalf("queued work inherited request cancellation: request=%v worker=%v tenant=%s root=%s", requestCtx.Err(), ctx.Err(), tenantID, rootID)
			}
			if platformModelConfigBarrier.TryLock() {
				platformModelConfigBarrier.Unlock()
				t.Fatal("worker must hold the model-config read barrier during Cloud deletion")
			}
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > rootDeletionRunTimeout {
				t.Fatal("worker must have its own bounded operation deadline")
			}
			return &mcpToolResult{Content: []byte(`{"deleted":true}`)}, nil
		})
		processRootDeletion(context.Background(), job)
		if !called {
			t.Fatal("queued work did not run after HTTP request was canceled")
		}
	})
}

func TestRootDeletionFinishCommitsCleanupAndSuccessAtomically(t *testing.T) {
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		job := rootDeletionTestJob("running")
		expectRootDeletionFinishStart(mock, job, true)
		expectRootDeletionCleanup(mock, job, 7)
		expectRootDeletionSuccess(mock, job, 12)
		mock.ExpectCommit()
		if err := finishRootDeletion(context.Background(), job, 12); err != nil {
			t.Fatalf("finish: %v", err)
		}
	})
}

func TestRootDeletionFinishOldAttemptCannotDeleteRelayState(t *testing.T) {
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		job := rootDeletionTestJob("running")
		expectRootDeletionFinishStart(mock, job, false)
		mock.ExpectRollback()
		if err := finishRootDeletion(context.Background(), job, 12); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("stale finish error=%v", err)
		}
	})
}

func TestRootDeletionCleanupFailureRollsBackAndKeepsMarkerForRetry(t *testing.T) {
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		job := rootDeletionTestJob("running")
		stubRootDeletionLease(t, func(ctx context.Context, _ string) (deleteRootOperationLease, error) {
			return noopDeleteRootLease{ctx: ctx}, nil
		})
		expectRootDeletionCurrent(mock, job, true)
		expectRootDeletionFinishStart(mock, job, true)
		mock.ExpectExec("DELETE FROM index_jobs").WithArgs(job.TenantID, job.RootID).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("DELETE FROM indexed_files AS files").WithArgs(job.TenantID, job.RootID).
			WillReturnError(errors.New("database connection lost"))
		mock.ExpectRollback()
		expectRootDeletionError(mock, job, "running")
		stubLCEClearIndexRoot(t, func(context.Context, string, string) (*mcpToolResult, error) {
			return &mcpToolResult{Content: []byte(`{"deleted_files":12}`)}, nil
		})
		processRootDeletion(context.Background(), job)
	})
}

func TestRootDeletionSuccessUpdateFailureRollsBackCleanup(t *testing.T) {
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		job := rootDeletionTestJob("running")
		expectRootDeletionFinishStart(mock, job, true)
		expectRootDeletionCleanup(mock, job, 7)
		mock.ExpectExec("UPDATE root_deletion_jobs SET status = 'succeeded'").
			WithArgs(job.ID, job.TenantID, job.AttemptID, int64(7)).WillReturnError(errors.New("write failed"))
		mock.ExpectRollback()
		if err := finishRootDeletion(context.Background(), job, 0); err == nil {
			t.Fatal("success marker write failure must roll back Relay cleanup")
		}
	})
}

func TestRootDeletionWorkerCancellationWhileConfigIsChanging(t *testing.T) {
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		platformModelConfigBarrier.Lock()
		defer platformModelConfigBarrier.Unlock()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		stubRootDeletionLease(t, func(context.Context, string) (deleteRootOperationLease, error) {
			t.Fatal("canceled worker must not acquire an index operation during config reset")
			return nil, nil
		})
		processRootDeletion(ctx, rootDeletionTestJob("running"))
	})
}

func TestRootDeletionLegacyEndpointReturnsBusyImmediately(t *testing.T) {
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		acquireDeleteRootOperation = func(ctx context.Context, tenantID string) (deleteRootOperationLease, error) {
			return tryExclusiveIndexOperation(ctx, tenantID, "delete-root")
		}
		expectIndexOperationLock(mock, "actor-a")
		mock.ExpectQuery("WITH expired AS").
			WithArgs(sqlmock.AnyArg(), "actor-a", "*", indexOperationExclusive, "delete-root", indexOperationLeaseDuration.Milliseconds(), indexOperationExclusive).
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
		mock.ExpectCommit()
		c, recorder := newRootAdminContext(t, "actor-a", "POST", `{"root_id":"repo-a"}`)
		handleDeleteRoot(c)
		if recorder.Code != http.StatusConflict {
			t.Fatalf("busy status=%d body=%s", recorder.Code, recorder.Body.String())
		}
	})
}

func TestRootDeletionAcquiredLeaseOutlivesAcquireContext(t *testing.T) {
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		expectIndexOperationLock(mock, "actor-a")
		mock.ExpectQuery("WITH expired AS").
			WithArgs(sqlmock.AnyArg(), "actor-a", "*", indexOperationExclusive, "delete-root", indexOperationLeaseDuration.Milliseconds(), indexOperationExclusive).
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
		mock.ExpectCommit()
		mock.ExpectExec(regexp.QuoteMeta("DELETE FROM index_operation_leases WHERE user_id = $1 AND lease_token = $2")).
			WithArgs("actor-a", sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
		lease, err := tryExclusiveIndexOperation(context.Background(), "actor-a", "delete-root")
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
		defer lease.Release()
		if err := lease.Context().Err(); err != nil {
			t.Fatalf("operation context was canceled when acquisition returned: %v", err)
		}
		if _, hasDeadline := lease.Context().Deadline(); hasDeadline {
			t.Fatal("acquisition deadline leaked into operation context")
		}
	})
}

func TestRootDeletionMarkerRejectsNewIndexOperationsBeforeTakingTenantLock(t *testing.T) {
	for _, kind := range []string{"create-job", "upload-batch", "complete-job", "fail-job", "clear-index", "dismiss-root-failure"} {
		t.Run(kind, func(t *testing.T) {
			withMockDB(t, func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`(?s)SELECT EXISTS\(SELECT 1 FROM root_deletion_jobs.*WHERE user_id = \$1 AND status IN \('queued', 'running'\)`).
					WithArgs("tenant-a").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
				lease, err := acquireIndexOperation(context.Background(), "tenant-a", "job:job-a", kind, indexOperationShared)
				if lease != nil || !errors.Is(err, errIndexOperationBusy) {
					t.Fatalf("active deletion must reject %s immediately: lease=%v error=%v", kind, lease, err)
				}
			})
		})
	}
}

func TestRootDeletionMarkerLookupFailureDoesNotPermitIndexing(t *testing.T) {
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		failure := errors.New("deletion state unavailable")
		mock.ExpectQuery("SELECT EXISTS\\(SELECT 1 FROM root_deletion_jobs").
			WithArgs("tenant-a").WillReturnError(failure)
		lease, err := acquireExclusiveIndexOperation(context.Background(), "tenant-a", "create-job")
		if lease != nil || !errors.Is(err, failure) {
			t.Fatalf("marker lookup must fail closed: lease=%v error=%v", lease, err)
		}
	})
}

func TestRootDeletionWorkerMayAcquireLeaseThroughOwnDurableMarker(t *testing.T) {
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		expectIndexOperationLock(mock, "tenant-a")
		mock.ExpectQuery(`(?s)WITH expired AS.*AND NOT EXISTS \(.*FROM root_deletion_jobs.*AND \$5::text <> 'delete-root-job'`).
			WithArgs(sqlmock.AnyArg(), "tenant-a", "*", indexOperationExclusive, "delete-root-job", indexOperationLeaseDuration.Milliseconds(), indexOperationExclusive).
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
		mock.ExpectCommit()
		mock.ExpectExec("DELETE FROM index_operation_leases").
			WithArgs("tenant-a", sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
		lease, err := acquireExclusiveIndexOperation(context.Background(), "tenant-a", "delete-root-job")
		if err != nil {
			t.Fatalf("deletion worker could not acquire its operation lease: %v", err)
		}
		lease.Release()
	})
}

func TestRootDeletionTimeoutsPreserveDurableFence(t *testing.T) {
	if rootDeletionRequestTimeout >= 125*time.Second {
		t.Fatal("queue request timeout must stay below the edge proxy deadline")
	}
	if rootDeletionRunTimeout <= remoteIndexMCPCallTimeout {
		t.Fatal("worker deadline must leave time for Relay cleanup after the Cloud call")
	}
	if rootDeletionClaimDuration <= rootDeletionRunTimeout+remoteIndexMCPCallTimeout+indexOperationLeaseDuration {
		t.Fatal("claim must cover a late Cloud call continuing after the worker and operation lease expire")
	}
	if rootDeletionWorkerCount < 1 || rootDeletionWorkerCount > 2 {
		t.Fatal("deletion concurrency must remain bounded")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	startRootDeletionWorkers(ctx)
}
