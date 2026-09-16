package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
)

const platformConfigJobMaxAttempts = 5

type platformConfigJob struct {
	ID               string          `json:"id"`
	Section          string          `json:"section"`
	EmbeddingChanged bool            `json:"embeddingChanged"`
	Status           string          `json:"status"`
	AttemptID        string          `json:"-"`
	AttemptCount     int             `json:"attempt_count"`
	RecoveryRequired bool            `json:"recovery_required"`
	Error            string          `json:"error,omitempty"`
	Result           json.RawMessage `json:"result,omitempty"`
	UpdatedAt        time.Time       `json:"updated_at"`
}

const platformConfigJobColumns = `id::text,section,embedding_changed,status,COALESCE(attempt_id::text,''),attempt_count,recovery_required,error,COALESCE(result,'null'::jsonb),updated_at`

func scanPlatformConfigJob(row interface{ Scan(...interface{}) error }) (platformConfigJob, error) {
	var job platformConfigJob
	err := row.Scan(&job.ID, &job.Section, &job.EmbeddingChanged, &job.Status, &job.AttemptID, &job.AttemptCount, &job.RecoveryRequired, &job.Error, &job.Result, &job.UpdatedAt)
	return job, err
}

func lockPlatformConfigTx(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext('acemcp:platform-config'))`)
	return err
}

func loadPlatformConfigJob(ctx context.Context, id string) (platformConfigJob, error) {
	if id == "latest" {
		return scanPlatformConfigJob(db.QueryRowContext(ctx, `SELECT `+platformConfigJobColumns+` FROM platform_config_jobs ORDER BY created_at DESC LIMIT 1`))
	}
	if _, err := uuid.Parse(id); err != nil {
		return platformConfigJob{}, err
	}
	return scanPlatformConfigJob(db.QueryRowContext(ctx, `SELECT `+platformConfigJobColumns+` FROM platform_config_jobs WHERE id=$1`, id))
}

func enqueuePlatformConfigJob(ctx context.Context, id, section string, embeddingChanged bool) (platformConfigJob, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return platformConfigJob{}, err
	}
	defer tx.Rollback()
	if err := lockPlatformConfigTx(ctx, tx); err != nil {
		return platformConfigJob{}, err
	}
	existing, err := scanPlatformConfigJob(tx.QueryRowContext(ctx, `SELECT `+platformConfigJobColumns+` FROM platform_config_jobs WHERE id=$1`, id))
	if err == nil {
		if existing.Section != section || existing.EmbeddingChanged != embeddingChanged {
			return platformConfigJob{}, errors.New("configuration operation identity mismatch")
		}
		return existing, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return platformConfigJob{}, err
	}
	var busy bool
	err = tx.QueryRowContext(ctx, `SELECT
  EXISTS(SELECT 1 FROM platform_config_jobs WHERE status IN ('pending','running'))
  OR ($1 AND (EXISTS(SELECT 1 FROM index_operation_leases WHERE lease_expires_at>NOW())
    OR EXISTS(SELECT 1 FROM root_deletion_jobs WHERE status IN ('queued','running'))))`, embeddingChanged).Scan(&busy)
	if err != nil {
		return platformConfigJob{}, err
	}
	if busy {
		return platformConfigJob{}, errIndexOperationBusy
	}
	job, err := scanPlatformConfigJob(tx.QueryRowContext(ctx, `INSERT INTO platform_config_jobs(id,section,embedding_changed,status)
  VALUES ($1,$2,$3,'pending') RETURNING `+platformConfigJobColumns, id, section, embeddingChanged))
	if err != nil {
		return platformConfigJob{}, err
	}
	return job, tx.Commit()
}

func recoverPlatformConfigJob(ctx context.Context, id string) (platformConfigJob, error) {
	if _, err := uuid.Parse(id); err != nil {
		return platformConfigJob{}, err
	}
	// Keep the same identity and fence. A repeated recovery after completion is
	// a no-op, including when its HTTP reply was lost.
	_, err := db.ExecContext(ctx, `UPDATE platform_config_jobs SET recovery_required=FALSE,
  attempt_count=0,next_attempt_at=NOW(),updated_at=NOW(),error=''
  WHERE id=$1 AND status='running' AND recovery_required`, id)
	if err != nil {
		return platformConfigJob{}, err
	}
	return loadPlatformConfigJob(ctx, id)
}

func claimPlatformConfigJob(ctx context.Context) (platformConfigJob, error) {
	return scanPlatformConfigJob(db.QueryRowContext(ctx, `WITH exhausted AS (
  UPDATE platform_config_jobs SET recovery_required=TRUE,error='Configuration outcome is unconfirmed; recovery is required',updated_at=NOW()
  WHERE status='running' AND claim_until<NOW() AND attempt_count >= $3 AND NOT recovery_required
 ), candidate AS (
  SELECT id AS candidate_id FROM platform_config_jobs WHERE
   (status='pending' OR (status='running' AND claim_until<NOW())) AND next_attempt_at<=NOW()
   AND NOT recovery_required AND attempt_count<$3 ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED
 ) UPDATE platform_config_jobs SET status='running',attempt_id=$1,attempt_count=attempt_count+1,
  claim_until=NOW()+($2 * INTERVAL '1 millisecond'),updated_at=NOW()
 FROM candidate WHERE id=candidate_id RETURNING `+platformConfigJobColumns,
		uuid.NewString(), (15 * time.Minute).Milliseconds(), platformConfigJobMaxAttempts))
}

type cloudPlatformConfigOperation struct {
	OperationID      string          `json:"operationId"`
	State            string          `json:"state"`
	EmbeddingChanged bool            `json:"embeddingChanged"`
	Result           json.RawMessage `json:"result"`
	Error            string          `json:"error"`
}

func finishPlatformConfigJob(ctx context.Context, job platformConfigJob, receipt cloudPlatformConfigOperation) error {
	if receipt.OperationID != job.ID || receipt.EmbeddingChanged != job.EmbeddingChanged || (receipt.State != "completed" && receipt.State != "rejected") {
		return errors.New("configuration receipt does not confirm this operation")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := lockPlatformConfigTx(ctx, tx); err != nil {
		return err
	}
	var id string
	if err := tx.QueryRowContext(ctx, `SELECT id::text FROM platform_config_jobs
  WHERE id=$1 AND attempt_id=$2 AND status='running' AND claim_until>NOW() FOR UPDATE`, job.ID, job.AttemptID).Scan(&id); err != nil {
		return err
	}
	status, message := "succeeded", ""
	cleared := clearedRelayIndexes{}
	if receipt.State == "rejected" {
		status, message = "rejected", "Configuration changed before this operation committed; reload and submit again"
		if receipt.Error == "PLATFORM_MODEL_CONFIG_EXPIRED" {
			message = "Configuration preparation expired before commit; reload and validate again"
		}
	} else if job.EmbeddingChanged {
		cleared, err = clearAllRelayIndexStateTx(ctx, tx)
		if err != nil {
			return err
		}
	}
	result, err := json.Marshal(map[string]interface{}{"embeddingChanged": job.EmbeddingChanged, "cloud": receipt.Result, "clearedRelay": cleared})
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE platform_config_jobs SET status=$3,result=$4::jsonb,error=$5,
  claim_until=NULL,recovery_required=FALSE,updated_at=NOW() WHERE id=$1 AND attempt_id=$2`, job.ID, job.AttemptID, status, string(result), message)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func deferPlatformConfigJob(job platformConfigJob, detail string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	paused := job.AttemptCount >= platformConfigJobMaxAttempts
	message := "Configuration outcome is unconfirmed; reconciliation will retry"
	if paused {
		message = "Configuration outcome is unconfirmed; recovery is required"
	}
	_, err := db.ExecContext(ctx, `UPDATE platform_config_jobs SET claim_until=NOW(),
  next_attempt_at=NOW()+($3 * INTERVAL '1 millisecond'),recovery_required=$4,error=$5,updated_at=NOW()
  WHERE id=$1 AND attempt_id=$2 AND status='running' AND claim_until>NOW()`,
		job.ID, job.AttemptID, rootDeletionRetryDelay(job.AttemptCount).Milliseconds(), paused, message)
	log.Printf("[PLATFORM_CONFIG] job=%s recovery_required=%t detail=%s record_error=%v", job.ID, paused, detail, err)
}

func processPlatformConfigJob(parent context.Context, job platformConfigJob) {
	ctx, cancel := context.WithTimeout(parent, 400*time.Second)
	defer cancel()
	if job.EmbeddingChanged {
		for !platformModelConfigBarrier.TryLock() {
			select {
			case <-ctx.Done():
				deferPlatformConfigJob(job, "cancelled while draining local requests")
				return
			case <-time.After(indexOperationAcquirePoll):
			}
		}
		defer platformModelConfigBarrier.Unlock()
	}
	data, status, err := callLCEPlatformConfig(ctx, http.MethodPost, map[string]interface{}{"action": "commit", "operationId": job.ID})
	if err != nil || status != http.StatusOK {
		deferPlatformConfigJob(job, fmt.Sprintf("cloud commit status=%d error=%v", status, err))
		return
	}
	var response struct {
		Operation cloudPlatformConfigOperation `json:"operation"`
	}
	if json.Unmarshal(data, &response) != nil {
		deferPlatformConfigJob(job, "invalid commit response")
		return
	}
	if err := finishPlatformConfigJob(ctx, job, response.Operation); err != nil {
		deferPlatformConfigJob(job, err.Error())
	}
}

func startPlatformConfigWorker(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for ctx.Err() == nil {
		claimCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		job, err := claimPlatformConfigJob(claimCtx)
		cancel()
		if err == nil {
			processPlatformConfigJob(ctx, job)
		} else if !errors.Is(err, sql.ErrNoRows) && ctx.Err() == nil {
			log.Printf("[PLATFORM_CONFIG] claim: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
