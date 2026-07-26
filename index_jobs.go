package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const (
	indexJobStatusRunning    = "running"
	indexJobStatusCompleted  = "completed"
	indexJobStatusFailed     = "failed"
	indexJobStatusSuperseded = "superseded"
	indexJobStatusTimedOut   = "timed_out"

	indexJobHeartbeatTimeout = 10 * time.Minute
	indexJobSweepInterval    = time.Minute
	maxIndexManifestFiles    = 100000
	maxIndexBatchFiles       = 100
)

type indexManifestFile struct {
	Path            string `json:"path"`
	Hash            string `json:"hash"`
	Size            int64  `json:"size"`
	EstimatedChunks int    `json:"estimatedChunks"`
	Content         string `json:"content,omitempty"`
}

type createIndexJobRequest struct {
	WorkspaceID   string              `json:"workspaceId"`
	WorkspaceName string              `json:"workspaceName"`
	Branch        string              `json:"branch"`
	Revision      string              `json:"revision"`
	Files         []indexManifestFile `json:"files"`
}

type indexJobView struct {
	ID                  string     `json:"id"`
	WorkspaceID         string     `json:"workspaceId"`
	WorkspaceName       string     `json:"workspaceName"`
	Branch              string     `json:"branch"`
	Revision            string     `json:"revision"`
	Mode                string     `json:"mode"`
	Phase               string     `json:"phase"`
	Status              string     `json:"status"`
	WorkspaceFiles      int        `json:"workspaceFiles"`
	TotalFiles          int        `json:"totalFiles"`
	IndexedFiles        int        `json:"indexedFiles"`
	FailedFiles         int        `json:"failedFiles"`
	TotalChunks         int64      `json:"totalChunks"`
	IndexedChunks       int64      `json:"indexedChunks"`
	ChunkCountEstimated bool       `json:"chunkCountEstimated"`
	DeletedCount        int        `json:"deletedCount"`
	Error               string     `json:"error,omitempty"`
	StartedAt           time.Time  `json:"startedAt"`
	HeartbeatAt         time.Time  `json:"heartbeatAt"`
	CompletedAt         *time.Time `json:"completedAt,omitempty"`
}

type createIndexJobResponse struct {
	Job          indexJobView `json:"job"`
	PendingFiles []string     `json:"pendingFiles"`
	DeletedFiles []string     `json:"deletedFiles"`
}

func migrateIndexingTables() error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS index_workspaces (
			user_id VARCHAR(255) NOT NULL,
			workspace_id VARCHAR(128) NOT NULL,
			workspace_name TEXT NOT NULL DEFAULT '',
			branch TEXT NOT NULL DEFAULT '',
			revision TEXT NOT NULL DEFAULT '',
			indexed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			PRIMARY KEY (user_id, workspace_id)
		);

		CREATE TABLE IF NOT EXISTS index_jobs (
			id UUID PRIMARY KEY,
			user_id VARCHAR(255) NOT NULL,
			workspace_id VARCHAR(128) NOT NULL,
			workspace_name TEXT NOT NULL DEFAULT '',
			branch TEXT NOT NULL DEFAULT '',
			revision TEXT NOT NULL DEFAULT '',
			mode VARCHAR(20) NOT NULL,
			phase VARCHAR(32) NOT NULL DEFAULT 'created',
			status VARCHAR(20) NOT NULL DEFAULT 'running',
			workspace_files INTEGER NOT NULL DEFAULT 0,
			total_files INTEGER NOT NULL DEFAULT 0,
			indexed_files INTEGER NOT NULL DEFAULT 0,
			failed_files INTEGER NOT NULL DEFAULT 0,
			total_chunks BIGINT NOT NULL DEFAULT 0,
			indexed_chunks BIGINT NOT NULL DEFAULT 0,
			chunk_count_fallback BOOLEAN NOT NULL DEFAULT FALSE,
			deleted_count INTEGER NOT NULL DEFAULT 0,
			deletions_sent BOOLEAN NOT NULL DEFAULT FALSE,
			error TEXT NOT NULL DEFAULT '',
			started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			heartbeat_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			completed_at TIMESTAMP WITH TIME ZONE
		);
		CREATE INDEX IF NOT EXISTS idx_index_jobs_workspace
			ON index_jobs(user_id, workspace_id, started_at DESC);
		CREATE INDEX IF NOT EXISTS idx_index_jobs_running
			ON index_jobs(status, heartbeat_at) WHERE status = 'running';

		CREATE TABLE IF NOT EXISTS index_job_files (
			job_id UUID NOT NULL REFERENCES index_jobs(id) ON DELETE CASCADE,
			path TEXT NOT NULL,
			hash VARCHAR(128) NOT NULL,
			size BIGINT NOT NULL DEFAULT 0,
			estimated_chunks INTEGER NOT NULL DEFAULT 1,
			needs_index BOOLEAN NOT NULL DEFAULT FALSE,
			indexed BOOLEAN NOT NULL DEFAULT FALSE,
			PRIMARY KEY (job_id, path)
		);
		CREATE INDEX IF NOT EXISTS idx_index_job_files_pending
			ON index_job_files(job_id, needs_index, indexed);

		CREATE TABLE IF NOT EXISTS indexed_files (
			user_id VARCHAR(255) NOT NULL,
			workspace_id VARCHAR(128) NOT NULL,
			path TEXT NOT NULL,
			hash VARCHAR(128) NOT NULL,
			size BIGINT NOT NULL DEFAULT 0,
			estimated_chunks INTEGER NOT NULL DEFAULT 1,
			updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			PRIMARY KEY (user_id, workspace_id, path)
		);
		CREATE INDEX IF NOT EXISTS idx_indexed_files_workspace
			ON indexed_files(user_id, workspace_id);
	`)
	if err != nil {
		return fmt.Errorf("failed to migrate indexing tables: %w", err)
	}
	return nil
}

func normalizeIndexPath(value string) string {
	value = strings.TrimSpace(strings.ReplaceAll(value, `\`, "/"))
	value = strings.TrimPrefix(path.Clean("/"+value), "/")
	if value == "." {
		return ""
	}
	return value
}

func normalizeManifest(files []indexManifestFile) ([]indexManifestFile, error) {
	if len(files) > maxIndexManifestFiles {
		return nil, fmt.Errorf("manifest exceeds %d files", maxIndexManifestFiles)
	}
	seen := make(map[string]bool, len(files))
	out := make([]indexManifestFile, 0, len(files))
	for _, file := range files {
		file.Path = normalizeIndexPath(file.Path)
		file.Hash = strings.TrimSpace(file.Hash)
		if file.Path == "" || file.Hash == "" {
			return nil, fmt.Errorf("manifest file path and hash are required")
		}
		if seen[file.Path] {
			return nil, fmt.Errorf("duplicate manifest path: %s", file.Path)
		}
		seen[file.Path] = true
		if file.Size < 0 {
			file.Size = 0
		}
		if file.EstimatedChunks < 1 {
			file.EstimatedChunks = 1
		}
		file.Content = ""
		out = append(out, file)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, nil
}

func loadIndexedSnapshot(ctx context.Context, tx *sql.Tx, userID, workspaceID string) (map[string]indexManifestFile, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT path, hash, size, estimated_chunks
		FROM indexed_files
		WHERE user_id = $1 AND workspace_id = $2
	`, userID, workspaceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]indexManifestFile)
	for rows.Next() {
		var file indexManifestFile
		if err := rows.Scan(&file.Path, &file.Hash, &file.Size, &file.EstimatedChunks); err != nil {
			return nil, err
		}
		out[file.Path] = file
	}
	return out, rows.Err()
}

func diffManifest(previous map[string]indexManifestFile, current []indexManifestFile) ([]string, []string, int64) {
	pending := make([]string, 0)
	deletedMap := make(map[string]bool, len(previous))
	for filePath := range previous {
		deletedMap[filePath] = true
	}
	var estimatedChunks int64
	for _, file := range current {
		delete(deletedMap, file.Path)
		old, ok := previous[file.Path]
		if !ok || old.Hash != file.Hash || old.Size != file.Size {
			pending = append(pending, file.Path)
			estimatedChunks += int64(file.EstimatedChunks)
		}
	}
	deleted := make([]string, 0, len(deletedMap))
	for filePath := range deletedMap {
		deleted = append(deleted, filePath)
	}
	sort.Strings(deleted)
	return pending, deleted, estimatedChunks
}

func lockIndexUserTx(ctx context.Context, tx *sql.Tx, userID string) error {
	_, err := tx.ExecContext(ctx, `
		SELECT pg_advisory_xact_lock(hashtext('acemcp:index-user'), hashtext($1))
	`, userID)
	return err
}

func beginLockedIndexUserTx(ctx context.Context, userID string) (*sql.Tx, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	if err := lockIndexUserTx(ctx, tx, userID); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return tx, nil
}

func handleCreateIndexJob(c *gin.Context) {
	userID := c.GetString(ContextKeyUserID)
	var req createIndexJobRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		finishJSON(c, http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}
	req.WorkspaceID = strings.TrimSpace(req.WorkspaceID)
	req.WorkspaceName = strings.TrimSpace(req.WorkspaceName)
	req.Branch = strings.TrimSpace(req.Branch)
	req.Revision = strings.TrimSpace(req.Revision)
	if req.WorkspaceID == "" || len(req.WorkspaceID) > 128 {
		finishJSON(c, http.StatusBadRequest, gin.H{"error": "workspaceId is required"})
		return
	}
	files, err := normalizeManifest(req.Files)
	if err != nil {
		finishJSON(c, http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	tx, err := beginLockedIndexUserTx(c.Request.Context(), userID)
	if err != nil {
		finishIndexError(c, err)
		return
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(c.Request.Context(), `
		DELETE FROM index_job_files
		WHERE job_id IN (
			SELECT id FROM index_jobs
			WHERE user_id = $1 AND workspace_id = $2 AND status = $3
		)
	`, userID, req.WorkspaceID, indexJobStatusRunning)
	if err != nil {
		finishIndexError(c, err)
		return
	}
	_, err = tx.ExecContext(c.Request.Context(), `
		UPDATE index_jobs
		SET status = $1, phase = 'done', error = 'superseded by a newer workspace scan',
			completed_at = NOW(), heartbeat_at = NOW()
		WHERE user_id = $2 AND workspace_id = $3 AND status = $4
	`, indexJobStatusSuperseded, userID, req.WorkspaceID, indexJobStatusRunning)
	if err != nil {
		finishIndexError(c, err)
		return
	}

	previous, err := loadIndexedSnapshot(c.Request.Context(), tx, userID, req.WorkspaceID)
	if err != nil {
		finishIndexError(c, err)
		return
	}
	var workspaceExists bool
	err = tx.QueryRowContext(c.Request.Context(), `
		SELECT EXISTS(
			SELECT 1 FROM index_workspaces WHERE user_id = $1 AND workspace_id = $2
		)
	`, userID, req.WorkspaceID).Scan(&workspaceExists)
	if err != nil {
		finishIndexError(c, err)
		return
	}

	pending, deleted, estimatedChunks := diffManifest(previous, files)
	pendingSet := make(map[string]bool, len(pending))
	for _, filePath := range pending {
		pendingSet[filePath] = true
	}
	mode := "incremental"
	if !workspaceExists {
		mode = "full"
	}
	jobID := uuid.New().String()
	_, err = tx.ExecContext(c.Request.Context(), `
		INSERT INTO index_jobs (
			id, user_id, workspace_id, workspace_name, branch, revision, mode,
			workspace_files, total_files, total_chunks, deleted_count
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`, jobID, userID, req.WorkspaceID, req.WorkspaceName, req.Branch, req.Revision, mode,
		len(files), len(pending), estimatedChunks, len(deleted))
	if err != nil {
		finishIndexError(c, err)
		return
	}

	stmt, err := tx.PrepareContext(c.Request.Context(), `
		INSERT INTO index_job_files (
			job_id, path, hash, size, estimated_chunks, needs_index
		) VALUES ($1, $2, $3, $4, $5, $6)
	`)
	if err != nil {
		finishIndexError(c, err)
		return
	}
	for _, file := range files {
		if _, err := stmt.ExecContext(c.Request.Context(), jobID, file.Path, file.Hash, file.Size, file.EstimatedChunks, pendingSet[file.Path]); err != nil {
			stmt.Close()
			finishIndexError(c, err)
			return
		}
	}
	stmt.Close()

	if err := tx.Commit(); err != nil {
		finishIndexError(c, err)
		return
	}
	job, err := loadIndexJob(c.Request.Context(), userID, jobID)
	if err != nil {
		finishIndexError(c, err)
		return
	}
	finishJSON(c, http.StatusCreated, createIndexJobResponse{Job: job, PendingFiles: pending, DeletedFiles: deleted})
}

func handleGetIndexJob(c *gin.Context) {
	userID := c.GetString(ContextKeyUserID)
	jobID := strings.TrimSpace(c.Param("id"))
	_, _ = db.ExecContext(c.Request.Context(), `
		UPDATE index_jobs SET heartbeat_at = NOW()
		WHERE id = $1 AND user_id = $2 AND status = $3
	`, jobID, userID, indexJobStatusRunning)
	job, err := loadIndexJob(c.Request.Context(), userID, jobID)
	if err == sql.ErrNoRows {
		finishJSON(c, http.StatusNotFound, gin.H{"error": "index job not found"})
		return
	}
	if err != nil {
		finishIndexError(c, err)
		return
	}
	finishJSON(c, http.StatusOK, gin.H{"job": job})
}

func loadIndexJob(ctx context.Context, userID, jobID string) (indexJobView, error) {
	return loadIndexJobFrom(ctx, db, userID, jobID)
}

type indexJobQueryer interface {
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
}

func loadIndexJobFrom(ctx context.Context, queryer indexJobQueryer, userID, jobID string) (indexJobView, error) {
	var job indexJobView
	var fallback bool
	err := queryer.QueryRowContext(ctx, `
		SELECT id::text, workspace_id, workspace_name, branch, revision, mode, phase, status,
			workspace_files, total_files, indexed_files, failed_files, total_chunks,
			indexed_chunks, chunk_count_fallback, deleted_count, error,
			started_at, heartbeat_at, completed_at
		FROM index_jobs
		WHERE id = $1 AND user_id = $2
	`, jobID, userID).Scan(
		&job.ID, &job.WorkspaceID, &job.WorkspaceName, &job.Branch, &job.Revision,
		&job.Mode, &job.Phase, &job.Status, &job.WorkspaceFiles, &job.TotalFiles,
		&job.IndexedFiles, &job.FailedFiles, &job.TotalChunks, &job.IndexedChunks,
		&fallback, &job.DeletedCount, &job.Error, &job.StartedAt, &job.HeartbeatAt,
		&job.CompletedAt,
	)
	job.ChunkCountEstimated = fallback || job.Status != indexJobStatusCompleted
	return job, err
}

type remoteIndexRequest struct {
	JobID string              `json:"jobId"`
	Files []indexManifestFile `json:"files"`
}

func handleRemoteIndex(c *gin.Context) {
	userID := c.GetString(ContextKeyUserID)
	var req remoteIndexRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		finishJSON(c, http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}
	req.JobID = strings.TrimSpace(req.JobID)
	if req.JobID == "" || len(req.Files) > maxIndexBatchFiles {
		finishJSON(c, http.StatusBadRequest, gin.H{"error": "invalid index batch"})
		return
	}

	modelConfig, err := resolveModelConfigArg(c.Request.Context(), userID)
	if err != nil {
		finishJSON(c, http.StatusServiceUnavailable, gin.H{"error": err.Error()})
		return
	}

	tx, err := beginLockedIndexUserTx(c.Request.Context(), userID)
	if err != nil {
		finishIndexError(c, err)
		return
	}
	defer tx.Rollback()

	job, staged, deleted, err := prepareIndexBatch(c.Request.Context(), tx, userID, req)
	if err == sql.ErrNoRows {
		finishJSON(c, http.StatusNotFound, gin.H{"error": "index job not found"})
		return
	}
	if err != nil {
		finishJSON(c, http.StatusConflict, gin.H{"error": err.Error()})
		return
	}

	filesArg := make([]map[string]interface{}, 0, len(staged))
	for _, file := range staged {
		filesArg = append(filesArg, map[string]interface{}{
			"path": file.Path, "content": file.Content, "hash": file.Hash,
		})
	}
	args := map[string]interface{}{"tenant_id": userID, "files": filesArg}
	if len(deleted) > 0 {
		args["deleted_files"] = deleted
	}
	if modelConfig != nil {
		args["model_config"] = modelConfig
	}
	result, err := lce.callTool(c.Request.Context(), "codebase_remote_index", args)
	if err != nil {
		finishIndexToolError(c, err)
		return
	}
	if result.IsError {
		finishIndexToolError(c, fmt.Errorf("%s", string(result.Content)))
		return
	}

	chunks, exact := extractChunkCount(result.Content)
	if !exact {
		for _, file := range staged {
			chunks += int64(file.EstimatedChunks)
		}
	}
	if err := commitIndexBatch(c.Request.Context(), tx, userID, job.ID, staged, chunks, !exact, len(deleted) > 0); err != nil {
		finishIndexError(c, err)
		return
	}
	if err := tx.Commit(); err != nil {
		finishIndexError(c, err)
		return
	}
	updated, err := loadIndexJob(c.Request.Context(), userID, job.ID)
	if err != nil {
		finishIndexError(c, err)
		return
	}
	var lceResponse interface{}
	if err := json.Unmarshal(result.Content, &lceResponse); err != nil {
		lceResponse = string(result.Content)
	}
	finishJSON(c, http.StatusOK, gin.H{"job": updated, "lce": lceResponse})
}

func prepareIndexBatch(ctx context.Context, tx *sql.Tx, userID string, req remoteIndexRequest) (indexJobView, []indexManifestFile, []string, error) {
	job, err := loadIndexJobFrom(ctx, tx, userID, req.JobID)
	if err != nil {
		return job, nil, nil, err
	}
	if job.Status != indexJobStatusRunning {
		return job, nil, nil, fmt.Errorf("index job is %s", job.Status)
	}
	if len(req.Files) == 0 && job.DeletedCount == 0 {
		return job, nil, nil, fmt.Errorf("empty index batch")
	}

	staged := make([]indexManifestFile, 0, len(req.Files))
	seen := make(map[string]bool, len(req.Files))
	for _, input := range req.Files {
		input.Path = normalizeIndexPath(input.Path)
		if input.Path == "" || seen[input.Path] {
			return job, nil, nil, fmt.Errorf("invalid or duplicate batch path")
		}
		seen[input.Path] = true
		var expectedHash string
		var estimatedChunks int
		var needsIndex, indexed bool
		err := tx.QueryRowContext(ctx, `
			SELECT hash, estimated_chunks, needs_index, indexed
			FROM index_job_files
			WHERE job_id = $1 AND path = $2
		`, req.JobID, input.Path).Scan(&expectedHash, &estimatedChunks, &needsIndex, &indexed)
		if err != nil {
			return job, nil, nil, err
		}
		if !needsIndex || indexed || strings.TrimSpace(input.Hash) != expectedHash {
			return job, nil, nil, fmt.Errorf("batch file is stale or already indexed: %s", input.Path)
		}
		input.Hash = expectedHash
		input.EstimatedChunks = estimatedChunks
		staged = append(staged, input)
	}

	var deletionsSent bool
	err = tx.QueryRowContext(ctx, `SELECT deletions_sent FROM index_jobs WHERE id = $1`, req.JobID).Scan(&deletionsSent)
	if err != nil {
		return job, nil, nil, err
	}
	var deleted []string
	if !deletionsSent && job.DeletedCount > 0 {
		rows, err := tx.QueryContext(ctx, `
			SELECT old.path
			FROM indexed_files old
			WHERE old.user_id = $1 AND old.workspace_id = $2
			  AND NOT EXISTS (
				SELECT 1 FROM index_job_files current
				WHERE current.job_id = $3 AND current.path = old.path
			  )
			ORDER BY old.path
		`, userID, job.WorkspaceID, req.JobID)
		if err != nil {
			return job, nil, nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var filePath string
			if err := rows.Scan(&filePath); err != nil {
				return job, nil, nil, err
			}
			deleted = append(deleted, filePath)
		}
		if err := rows.Err(); err != nil {
			return job, nil, nil, err
		}
	}
	return job, staged, deleted, nil
}

func commitIndexBatch(ctx context.Context, tx *sql.Tx, userID, jobID string, files []indexManifestFile, chunks int64, fallback, sentDeletions bool) error {
	var status string
	if err := tx.QueryRowContext(ctx, `
		SELECT status FROM index_jobs WHERE id = $1 AND user_id = $2 FOR UPDATE
	`, jobID, userID).Scan(&status); err != nil {
		return err
	}
	if status != indexJobStatusRunning {
		return fmt.Errorf("index job became %s", status)
	}
	for _, file := range files {
		result, err := tx.ExecContext(ctx, `
			UPDATE index_job_files SET indexed = TRUE
			WHERE job_id = $1 AND path = $2 AND needs_index = TRUE AND indexed = FALSE
		`, jobID, file.Path)
		if err != nil {
			return err
		}
		rows, _ := result.RowsAffected()
		if rows != 1 {
			return fmt.Errorf("index batch was already committed: %s", file.Path)
		}
	}
	_, err := tx.ExecContext(ctx, `
		UPDATE index_jobs
		SET phase = 'indexing',
			indexed_files = indexed_files + $1,
			indexed_chunks = indexed_chunks + $2,
			chunk_count_fallback = chunk_count_fallback OR $3,
			deletions_sent = deletions_sent OR $4,
			heartbeat_at = NOW()
		WHERE id = $5
	`, len(files), chunks, fallback, sentDeletions, jobID)
	if err != nil {
		return err
	}
	return nil
}

type finishIndexJobRequest struct {
	Error string `json:"error"`
}

func handleCompleteIndexJob(c *gin.Context) {
	userID := c.GetString(ContextKeyUserID)
	jobID := strings.TrimSpace(c.Param("id"))
	tx, err := beginLockedIndexUserTx(c.Request.Context(), userID)
	if err != nil {
		finishIndexError(c, err)
		return
	}
	defer tx.Rollback()

	var workspaceID, workspaceName, branch, revision, status string
	var totalFiles, indexedFiles, deletedCount int
	var deletionsSent, fallback bool
	err = tx.QueryRowContext(c.Request.Context(), `
		SELECT workspace_id, workspace_name, branch, revision, status,
			total_files, indexed_files, deleted_count, deletions_sent, chunk_count_fallback
		FROM index_jobs
		WHERE id = $1 AND user_id = $2
		FOR UPDATE
	`, jobID, userID).Scan(
		&workspaceID, &workspaceName, &branch, &revision, &status,
		&totalFiles, &indexedFiles, &deletedCount, &deletionsSent, &fallback,
	)
	if err == sql.ErrNoRows {
		finishJSON(c, http.StatusNotFound, gin.H{"error": "index job not found"})
		return
	}
	if err != nil {
		finishIndexError(c, err)
		return
	}
	if status != indexJobStatusRunning {
		finishJSON(c, http.StatusConflict, gin.H{"error": "index job is " + status})
		return
	}
	if indexedFiles != totalFiles {
		finishJSON(c, http.StatusConflict, gin.H{"error": "index job still has pending files"})
		return
	}
	if deletedCount > 0 && !deletionsSent {
		finishJSON(c, http.StatusConflict, gin.H{"error": "index job still has pending deletions"})
		return
	}

	if _, err = tx.ExecContext(c.Request.Context(), `
		DELETE FROM indexed_files WHERE user_id = $1 AND workspace_id = $2
	`, userID, workspaceID); err != nil {
		finishIndexError(c, err)
		return
	}
	if _, err = tx.ExecContext(c.Request.Context(), `
		INSERT INTO indexed_files (
			user_id, workspace_id, path, hash, size, estimated_chunks
		)
		SELECT $1, $2, path, hash, size, estimated_chunks
		FROM index_job_files
		WHERE job_id = $3
	`, userID, workspaceID, jobID); err != nil {
		finishIndexError(c, err)
		return
	}
	if _, err = tx.ExecContext(c.Request.Context(), `
		INSERT INTO index_workspaces (
			user_id, workspace_id, workspace_name, branch, revision, indexed_at
		) VALUES ($1, $2, $3, $4, $5, NOW())
		ON CONFLICT (user_id, workspace_id) DO UPDATE SET
			workspace_name = EXCLUDED.workspace_name,
			branch = EXCLUDED.branch,
			revision = EXCLUDED.revision,
			indexed_at = NOW()
	`, userID, workspaceID, workspaceName, branch, revision); err != nil {
		finishIndexError(c, err)
		return
	}
	totalChunkExpr := "total_chunks"
	if !fallback {
		totalChunkExpr = "indexed_chunks"
	}
	if _, err = tx.ExecContext(c.Request.Context(), fmt.Sprintf(`
		UPDATE index_jobs
		SET status = '%s', phase = 'done', total_chunks = %s,
			heartbeat_at = NOW(), completed_at = NOW()
		WHERE id = $1
	`, indexJobStatusCompleted, totalChunkExpr), jobID); err != nil {
		finishIndexError(c, err)
		return
	}
	if _, err = tx.ExecContext(c.Request.Context(), `DELETE FROM index_job_files WHERE job_id = $1`, jobID); err != nil {
		finishIndexError(c, err)
		return
	}
	if err = tx.Commit(); err != nil {
		finishIndexError(c, err)
		return
	}
	reclaimIndexResources()
	job, err := loadIndexJob(c.Request.Context(), userID, jobID)
	if err != nil {
		finishIndexError(c, err)
		return
	}
	finishJSON(c, http.StatusOK, gin.H{"job": job})
}

func handleFailIndexJob(c *gin.Context) {
	userID := c.GetString(ContextKeyUserID)
	jobID := strings.TrimSpace(c.Param("id"))
	var req finishIndexJobRequest
	_ = c.ShouldBindJSON(&req)
	req.Error = strings.TrimSpace(req.Error)
	if len(req.Error) > 2000 {
		req.Error = req.Error[:2000]
	}
	if req.Error == "" {
		req.Error = "client reported indexing failure"
	}
	tx, err := beginLockedIndexUserTx(c.Request.Context(), userID)
	if err != nil {
		finishIndexError(c, err)
		return
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(c.Request.Context(), `
		UPDATE index_jobs
		SET status = $1, phase = 'done', failed_files = total_files - indexed_files,
			error = $2, heartbeat_at = NOW(), completed_at = NOW()
		WHERE id = $3 AND user_id = $4 AND status = $5
	`, indexJobStatusFailed, req.Error, jobID, userID, indexJobStatusRunning)
	if err != nil {
		finishIndexError(c, err)
		return
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		finishJSON(c, http.StatusConflict, gin.H{"error": "index job is not running"})
		return
	}
	if _, err = tx.ExecContext(c.Request.Context(), `DELETE FROM index_job_files WHERE job_id = $1`, jobID); err != nil {
		finishIndexError(c, err)
		return
	}
	if err = tx.Commit(); err != nil {
		finishIndexError(c, err)
		return
	}
	reclaimIndexResources()
	job, err := loadIndexJob(c.Request.Context(), userID, jobID)
	if err != nil {
		finishIndexError(c, err)
		return
	}
	finishJSON(c, http.StatusOK, gin.H{"job": job})
}

func clearUserIndexStateTx(ctx context.Context, tx *sql.Tx, userID string) error {
	_, err := tx.ExecContext(ctx, `
		DELETE FROM index_jobs WHERE user_id = $1;
		DELETE FROM indexed_files WHERE user_id = $1;
		DELETE FROM index_workspaces WHERE user_id = $1;
	`, userID)
	return err
}

func clearUserIndexState(ctx context.Context, userID string) error {
	tx, err := beginLockedIndexUserTx(ctx, userID)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := clearUserIndexStateTx(ctx, tx, userID); err != nil {
		return err
	}
	return tx.Commit()
}

func sweepExpiredIndexJobs(ctx context.Context) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Printf("[INDEX] Failed to begin timeout sweep: %v", err)
		return
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `
		UPDATE index_jobs
		SET status = $1, phase = 'done', failed_files = total_files - indexed_files,
			error = 'index job heartbeat timed out', completed_at = NOW()
		WHERE status = $2 AND heartbeat_at < $3
		RETURNING id::text
	`, indexJobStatusTimedOut, indexJobStatusRunning, time.Now().Add(-indexJobHeartbeatTimeout))
	if err != nil {
		log.Printf("[INDEX] Failed to sweep timed out jobs: %v", err)
		return
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err == nil {
			ids = append(ids, id)
		}
	}
	rows.Close()
	for _, id := range ids {
		if _, err := tx.ExecContext(ctx, `DELETE FROM index_job_files WHERE job_id = $1`, id); err != nil {
			log.Printf("[INDEX] Failed to clean timed out job %s: %v", id, err)
			return
		}
	}
	if err := tx.Commit(); err != nil {
		log.Printf("[INDEX] Failed to commit timeout sweep: %v", err)
		return
	}
	if len(ids) > 0 {
		reclaimIndexResources()
		log.Printf("[INDEX] Reclaimed %d timed out indexing jobs", len(ids))
	}
}

func startIndexJobSweeper(ctx context.Context) {
	ticker := time.NewTicker(indexJobSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweepExpiredIndexJobs(ctx)
		}
	}
}

func reclaimIndexResources() {
	if lce != nil && lce.http != nil {
		lce.http.CloseIdleConnections()
	}
}

var chunkTextPattern = regexp.MustCompile(`(?i)(\d+)\s+chunks?`)

func extractChunkCount(content []byte) (int64, bool) {
	var value interface{}
	if json.Unmarshal(content, &value) == nil {
		if count, ok := findChunkCount(value); ok {
			return count, true
		}
	}
	match := chunkTextPattern.FindSubmatch(content)
	if len(match) == 2 {
		var count int64
		if _, err := fmt.Sscanf(string(match[1]), "%d", &count); err == nil {
			return count, true
		}
	}
	return 0, false
}

func findChunkCount(value interface{}) (int64, bool) {
	switch typed := value.(type) {
	case map[string]interface{}:
		keys := []string{"chunk_count", "chunkCount", "indexed_chunks", "indexedChunks", "total_chunks", "totalChunks"}
		for _, key := range keys {
			if count, ok := numberAsInt64(typed[key]); ok {
				return count, true
			}
		}
		if chunks, ok := typed["chunks"].([]interface{}); ok {
			return int64(len(chunks)), true
		}
		for _, child := range typed {
			if count, ok := findChunkCount(child); ok {
				return count, true
			}
		}
	case []interface{}:
		for _, child := range typed {
			if count, ok := findChunkCount(child); ok {
				return count, true
			}
		}
	}
	return 0, false
}

func numberAsInt64(value interface{}) (int64, bool) {
	number, ok := value.(float64)
	if !ok || number < 0 {
		return 0, false
	}
	return int64(number), true
}

func finishIndexToolError(c *gin.Context, err error) {
	logID := c.GetString(ContextKeyLogID)
	saveErrorDetailsAsync(logID, "lce", err.Error(), getInsertDone(c))
	finishJSON(c, http.StatusBadGateway, gin.H{"error": err.Error()})
}

func finishIndexError(c *gin.Context, err error) {
	logID := c.GetString(ContextKeyLogID)
	saveErrorDetailsAsync(logID, "relay", err.Error(), getInsertDone(c))
	finishJSON(c, http.StatusInternalServerError, gin.H{"error": err.Error()})
}

func finishJSON(c *gin.Context, status int, body interface{}) {
	c.JSON(status, body)
	completeRequestLogAsync(getRequestLogEntry(c, status))
}
