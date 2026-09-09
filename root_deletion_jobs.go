package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const (
	rootDeletionRequestTimeout = 5 * time.Second
	rootDeletionRunTimeout     = 400 * time.Second
	// Keep the durable fence past both the worker deadline and the Cloud SQL
	// deadline. An interrupted attempt must not race a newly created index.
	rootDeletionClaimDuration = 15 * time.Minute
	rootDeletionWorkerCount   = 2
	rootDeletionPollInterval  = 2 * time.Second
)

var errRootDeletionRateLimited = errors.New("root deletion rate limited")

type rootDeletionJob struct {
	ID           string    `json:"id"`
	TenantID     string    `json:"-"`
	ActorID      string    `json:"-"`
	RootID       string    `json:"root_id"`
	Status       string    `json:"status"`
	AttemptID    string    `json:"-"`
	DeletedFiles int64     `json:"deleted_files"`
	Error        string    `json:"error,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

const rootDeletionColumns = `id::text, user_id, actor_id, root_id, status,
	COALESCE(attempt_id::text, '') AS attempt_id, deleted_files, error, created_at, updated_at`

func scanRootDeletion(row interface{ Scan(...interface{}) error }) (rootDeletionJob, error) {
	var job rootDeletionJob
	err := row.Scan(&job.ID, &job.TenantID, &job.ActorID, &job.RootID, &job.Status,
		&job.AttemptID, &job.DeletedFiles, &job.Error, &job.CreatedAt, &job.UpdatedAt)
	return job, err
}

func enqueueRootDeletion(ctx context.Context, tenantID, actorID, rootID string) (rootDeletionJob, error) {
	tx, err := beginLockedIndexUserTx(ctx, tenantID)
	if err != nil {
		return rootDeletionJob{}, err
	}
	defer tx.Rollback()
	job, err := scanRootDeletion(tx.QueryRowContext(ctx, `SELECT `+rootDeletionColumns+`
		FROM root_deletion_jobs WHERE user_id = $1 AND status IN ('queued', 'running')`, tenantID))
	if err == nil {
		if job.RootID != rootID {
			return rootDeletionJob{}, errIndexOperationBusy
		}
		return job, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return rootDeletionJob{}, err
	}

	// Admission and the durable marker use the same tenant lock as lease
	// acquisition. No new index operation can enter between these two steps.
	var busy bool
	err = tx.QueryRowContext(ctx, `SELECT
		EXISTS(SELECT 1 FROM index_operation_leases WHERE user_id = $1 AND lease_expires_at > NOW())
		OR EXISTS(SELECT 1 FROM index_jobs WHERE user_id = $1 AND status = 'running')`,
		tenantID).Scan(&busy)
	if err != nil {
		return rootDeletionJob{}, err
	}
	if busy {
		return rootDeletionJob{}, errIndexOperationBusy
	}
	if checkDeleteRootRateLimit(tenantID, rootID, time.Now()) > 0 {
		return rootDeletionJob{}, errRootDeletionRateLimited
	}
	job, err = scanRootDeletion(tx.QueryRowContext(ctx, `
		INSERT INTO root_deletion_jobs(id, user_id, actor_id, root_id, status)
		VALUES ($1, $2, $3, $4, 'queued') RETURNING `+rootDeletionColumns,
		uuid.NewString(), tenantID, actorID, rootID))
	if err != nil {
		return rootDeletionJob{}, err
	}
	return job, tx.Commit()
}

func handleCreateRootDeletion(c *gin.Context) {
	actorID := c.GetString(ContextKeyUserID)
	if actorID == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusUnauthorized))
		return
	}
	var req struct {
		RootID string `json:"root_id"`
	}
	if c.ShouldBindJSON(&req) != nil || strings.TrimSpace(req.RootID) == "" || utf8.RuneCountInString(strings.TrimSpace(req.RootID)) > 128 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "root_id is required and must be at most 128 characters"})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusBadRequest))
		return
	}
	if rejectNonOwnerIndexDeletion(c) {
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), rootDeletionRequestTimeout)
	defer cancel()
	job, err := enqueueRootDeletion(ctx, requestTenantID(c), actorID, lceIndexRootID(req.RootID))
	if err != nil {
		status, message := http.StatusServiceUnavailable, "删除服务暂时不可用，请稍后重试"
		switch {
		case errors.Is(err, errIndexOperationBusy):
			status, message = http.StatusConflict, "索引正在执行其他操作，请等待其结束后再删除"
		case errors.Is(err, errRootDeletionRateLimited):
			status, message = http.StatusTooManyRequests, "删除操作过于频繁，请稍后重试"
			c.Header("Retry-After", "60")
		default:
			log.Printf("[DELETE_ROOT] enqueue failed tenant=%s: %v", requestTenantID(c), err)
		}
		c.JSON(status, gin.H{"error": message})
		completeRequestLogAsync(getRequestLogEntry(c, status))
		return
	}
	c.Header("Cache-Control", "no-store")
	c.Header("Retry-After", "3")
	c.JSON(http.StatusAccepted, gin.H{"deletion": job})
	completeRequestLogAsync(getRequestLogEntry(c, http.StatusAccepted))
}

func handleListRootDeletions(c *gin.Context) {
	if c.GetString(ContextKeyUserID) == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusUnauthorized))
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), rootDeletionRequestTimeout)
	defer cancel()
	jobs, err := loadRootDeletions(ctx, requestTenantID(c))
	if err != nil {
		log.Printf("[DELETE_ROOT] status failed tenant=%s: %v", requestTenantID(c), err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "暂时无法读取删除进度，请稍后重试"})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusServiceUnavailable))
		return
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, gin.H{"deletions": jobs})
	completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))
}

func loadRootDeletions(ctx context.Context, tenantID string) ([]rootDeletionJob, error) {
	rows, err := db.QueryContext(ctx, `SELECT `+rootDeletionColumns+` FROM (
		SELECT DISTINCT ON (root_id) `+rootDeletionColumns+`
		FROM root_deletion_jobs WHERE user_id = $1
			AND (status IN ('queued', 'running') OR updated_at > NOW() - INTERVAL '1 day')
		ORDER BY root_id, created_at DESC
	) recent ORDER BY (status IN ('queued', 'running')) DESC, created_at DESC LIMIT 100`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := make([]rootDeletionJob, 0)
	for rows.Next() {
		job, scanErr := scanRootDeletion(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func claimRootDeletion(ctx context.Context) (rootDeletionJob, error) {
	return scanRootDeletion(db.QueryRowContext(ctx, `WITH candidate AS (
		SELECT id AS candidate_id FROM root_deletion_jobs
		WHERE status = 'queued' OR (status = 'running' AND claim_until < NOW())
		ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED
	) UPDATE root_deletion_jobs SET status = 'running', attempt_id = $1,
		claim_until = NOW() + ($2 * INTERVAL '1 millisecond'), updated_at = NOW()
	FROM candidate WHERE id = candidate_id RETURNING `+rootDeletionColumns,
		uuid.NewString(), rootDeletionClaimDuration.Milliseconds()))
}

func rootDeletionAttemptCurrent(ctx context.Context, job rootDeletionJob) (bool, error) {
	var current bool
	err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM root_deletion_jobs
		WHERE id = $1 AND user_id = $2 AND attempt_id = $3
			AND status = 'running' AND claim_until > NOW())`, job.ID, job.TenantID, job.AttemptID).Scan(&current)
	return current, err
}

func recordRootDeletionError(job rootDeletionJob, definitive bool, detail string) {
	status := "running"
	message := "删除结果暂无法确认，后台将自动重试。"
	if definitive {
		status, message = "failed", "清除云端索引失败，请重试；若持续失败请联系管理员。"
	}
	ctx, cancel := context.WithTimeout(context.Background(), rootDeletionRequestTimeout)
	defer cancel()
	_, err := db.ExecContext(ctx, `UPDATE root_deletion_jobs SET status = $4, error = $5, updated_at = NOW()
		WHERE id = $1 AND user_id = $2 AND attempt_id = $3 AND status = 'running' AND claim_until > NOW()`,
		job.ID, job.TenantID, job.AttemptID, status, message)
	log.Printf("[DELETE_ROOT] job=%s tenant=%s root=%s state=%s error=%s record_error=%v",
		job.ID, job.TenantID, job.RootID, status, detail, err)
}

// Success and Relay cleanup commit together, so a restarted worker cannot
// replay a completed deletion against a subsequently re-indexed root.
func finishRootDeletion(ctx context.Context, job rootDeletionJob, cloudCount int64) error {
	tx, err := beginLockedIndexUserTx(ctx, job.TenantID)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var id string
	err = tx.QueryRowContext(ctx, `SELECT id::text FROM root_deletion_jobs
		WHERE id = $1 AND user_id = $2 AND attempt_id = $3
			AND status = 'running' AND claim_until > NOW() FOR UPDATE`, job.ID, job.TenantID, job.AttemptID).Scan(&id)
	if err != nil {
		return err
	}
	count, err := clearRootIndexStateTx(ctx, tx, job.TenantID, job.RootID)
	if err != nil {
		return err
	}
	if cloudCount > 0 {
		count = cloudCount
	}
	_, err = tx.ExecContext(ctx, `UPDATE root_deletion_jobs SET status = 'succeeded',
		deleted_files = $4, error = '', updated_at = NOW(), claim_until = NULL
		WHERE id = $1 AND user_id = $2 AND attempt_id = $3`, job.ID, job.TenantID, job.AttemptID, count)
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	log.Printf("[DELETE_ROOT] job=%s user=%s tenant=%s root=%s deleted_files=%d", job.ID, job.ActorID, job.TenantID, job.RootID, count)
	return nil
}

var acquireRootDeletionLease = func(ctx context.Context, tenantID string) (deleteRootOperationLease, error) {
	return acquireExclusiveIndexOperation(ctx, tenantID, "delete-root-job")
}

func processRootDeletion(parent context.Context, job rootDeletionJob) {
	ctx, cancel := context.WithTimeout(parent, rootDeletionRunTimeout)
	defer cancel()
	defer func() {
		if recovered := recover(); recovered != nil {
			recordRootDeletionError(job, false, fmt.Sprintf("worker panic: %v", recovered))
		}
	}()
	// Background work needs the same model-config protection as HTTP handlers.
	for !platformModelConfigBarrier.TryRLock() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(indexOperationAcquirePoll):
		}
	}
	defer platformModelConfigBarrier.RUnlock()
	lease, err := acquireRootDeletionLease(ctx, job.TenantID)
	if err != nil {
		recordRootDeletionError(job, false, err.Error())
		return
	}
	defer lease.Release()
	ctx = lease.Context()
	current, err := rootDeletionAttemptCurrent(ctx, job)
	if err != nil || !current {
		return
	}
	started := time.Now()
	log.Printf("[DELETE_ROOT] job=%s tenant=%s root=%s stage=cloud", job.ID, job.TenantID, job.RootID)
	result, err := lceClearIndexRoot(ctx, job.TenantID, job.RootID)
	if err != nil || result == nil {
		// A missing reply is not proof of rollback. Keep the durable marker and
		// retry idempotently after the claim expires, even after process restart.
		recordRootDeletionError(job, false, fmt.Sprintf("cloud response: %v", err))
		return
	}
	if result.IsError {
		recordRootDeletionError(job, true, string(result.Content))
		return
	}
	count, _ := extractLCEDeletedFiles(result.Content)
	log.Printf("[DELETE_ROOT] job=%s stage=relay cloud_ms=%d", job.ID, time.Since(started).Milliseconds())
	if err := finishRootDeletion(ctx, job, count); err != nil {
		recordRootDeletionError(job, false, "relay cleanup: "+err.Error())
	}
}

func pruneRootDeletionHistory(ctx context.Context) error {
	_, err := db.ExecContext(ctx, `DELETE FROM root_deletion_jobs WHERE id IN (
		SELECT id FROM root_deletion_jobs WHERE status IN ('succeeded', 'failed')
			AND updated_at < NOW() - INTERVAL '7 days'
		ORDER BY updated_at LIMIT 1000 FOR UPDATE SKIP LOCKED)`)
	return err
}

func startRootDeletionWorkers(ctx context.Context) {
	var workers sync.WaitGroup
	for i := 0; i < rootDeletionWorkerCount; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			ticker := time.NewTicker(rootDeletionPollInterval)
			defer ticker.Stop()
			for {
				if ctx.Err() != nil {
					return
				}
				claimCtx, cancel := context.WithTimeout(ctx, rootDeletionRequestTimeout)
				job, err := claimRootDeletion(claimCtx)
				cancel()
				if err == nil {
					processRootDeletion(ctx, job)
				} else if !errors.Is(err, sql.ErrNoRows) && ctx.Err() == nil {
					log.Printf("[DELETE_ROOT] claim failed: %v", err)
				}
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
			}
		}()
	}
	pruneTicker := time.NewTicker(time.Hour)
	defer pruneTicker.Stop()
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
		case <-pruneTicker.C:
			pruneCtx, cancel := context.WithTimeout(ctx, rootDeletionRequestTimeout)
			err := pruneRootDeletionHistory(pruneCtx)
			cancel()
			if err != nil {
				log.Printf("[DELETE_ROOT] history cleanup failed: %v", err)
			}
		}
	}
	workers.Wait()
}
