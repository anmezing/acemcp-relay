package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

const (
	indexJobStatusRunning    = "running"
	indexJobStatusCompleted  = "completed"
	indexJobStatusFailed     = "failed"
	indexJobStatusSuperseded = "superseded"
	indexJobStatusTimedOut   = "timed_out"

	indexJobHeartbeatTimeout = 10 * time.Minute
	indexJobSweepInterval    = time.Minute
	indexJobRenewCallTimeout = 15 * time.Second
	maxIndexManifestFiles    = 100000
	maxIndexBatchFiles       = 50
	// 单个文件的内容上限。源码文件极少接近这个量级，而 embedding 本身也有
	// token 上限——超过这个大小的"源码"要么是生成物要么是灌进来的负载。
	maxIndexFileBytes = 512 << 10 // 512 KiB
	// 单批内容总量上限。它也决定 Agent 报告进度和失败重试的粒度。
	maxIndexBatchBytes = 512 << 10 // 512 KiB
	// LCE's container entry point accepts a 4 MiB complete JSON-RPC body. Raw
	// source is capped much lower because JSON escaping can expand a byte to a
	// six-byte \\u00xx sequence. The serialized-body check below remains the
	// authoritative guard for paths, model config, and all other envelope data.
	maxLCEMCPRequestBodyBytes = 4 << 20 // 4 MiB
	// 每用户每日索引字节的默认上限。正常项目首次全量索引通常在百 MB 量级，
	// 之后只传变更；这个默认值对真实使用足够宽松，但能挡住批量灌数据。
	defaultDailyIndexBytes = 2 << 30 // 2 GiB
	indexProtocolVersion   = 1
)

type indexUpstreamError struct {
	err error
}

func (e *indexUpstreamError) Error() string { return e.err.Error() }
func (e *indexUpstreamError) Unwrap() error { return e.err }

func newIndexUpstreamError(format string, args ...interface{}) error {
	return &indexUpstreamError{err: fmt.Errorf(format, args...)}
}

type indexManifestFile struct {
	Path            string `json:"path"`
	Hash            string `json:"hash"`
	Size            int64  `json:"size"`
	EstimatedChunks int    `json:"estimated_chunks"`
	Content         string `json:"content,omitempty"`
}

type indexStartRequest struct {
	ProtocolVersion int
	WorkspaceID     string
	WorkspaceName   string
	Branch          string
	Revision        string
	Files           []indexManifestFile
	UnreadableFiles []string
	RootID          string
}

type indexJobView struct {
	ID                  string     `json:"id"`
	WorkspaceID         string     `json:"workspace_id"`
	WorkspaceName       string     `json:"workspace_name"`
	RootID              string     `json:"root_id"`
	Branch              string     `json:"branch"`
	Revision            string     `json:"revision"`
	Mode                string     `json:"mode"`
	Phase               string     `json:"phase"`
	Status              string     `json:"status"`
	WorkspaceFiles      int        `json:"workspace_files"`
	TotalFiles          int        `json:"total_files"`
	IndexedFiles        int        `json:"indexed_files"`
	FailedFiles         int        `json:"failed_files"`
	TotalChunks         int64      `json:"total_chunks"`
	IndexedChunks       int64      `json:"indexed_chunks"`
	ChunkCountEstimated bool       `json:"chunk_count_estimated"`
	DeletedCount        int        `json:"deleted_count"`
	Error               string     `json:"error,omitempty"`
	StartedAt           time.Time  `json:"started_at"`
	HeartbeatAt         time.Time  `json:"heartbeat_at"`
	CompletedAt         *time.Time `json:"completed_at,omitempty"`
	CloudRevision       int64      `json:"cloud_revision,omitempty"`
}

type createIndexJobResponse struct {
	Job               *indexJobView `json:"job,omitempty"`
	ActiveJob         *indexJobView `json:"active_job,omitempty"`
	PendingFiles      []string      `json:"pending_files,omitempty"`
	DeletedFiles      []string      `json:"deleted_files,omitempty"`
	Unchanged         bool          `json:"unchanged,omitempty"`
	Busy              bool          `json:"busy,omitempty"`
	BusyReason        string        `json:"busy_reason,omitempty"`
	RetryAfterSeconds int           `json:"retry_after_seconds,omitempty"`
}

const (
	indexStartBusyActiveJob   = "active_job"
	indexStartBusyRateLimited = "rate_limited"
)

func migrateIndexingTables() error {
	// 租户归并说明：本文件所有表的 user_id 列（index_workspaces / index_jobs /
	// indexed_files / index_operation_leases）以及各函数的 userID 参数，语义都是
	// "租户"（tenant_id := org_id ?? user_id）。个人用户租户 = user_id，存量数据
	// 与行为完全不变；组织密钥写入的是 org_id。列名不改（已部署迁移不可变）。
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS index_workspaces (
			user_id VARCHAR(255) NOT NULL,
			workspace_id VARCHAR(128) NOT NULL,
			workspace_name TEXT NOT NULL DEFAULT '',
			root_id VARCHAR(128) NOT NULL DEFAULT '',
			branch TEXT NOT NULL DEFAULT '',
			revision TEXT NOT NULL DEFAULT '',
			cloud_revision BIGINT NOT NULL DEFAULT 0,
			indexed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			PRIMARY KEY (user_id, workspace_id)
		);
		CREATE TABLE IF NOT EXISTS index_jobs (
			id UUID PRIMARY KEY,
			user_id VARCHAR(255) NOT NULL,
			workspace_id VARCHAR(128) NOT NULL,
			workspace_name TEXT NOT NULL DEFAULT '',
			root_id VARCHAR(128) NOT NULL DEFAULT '',
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
			cloud_revision BIGINT NOT NULL DEFAULT 0,
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

		CREATE TABLE IF NOT EXISTS index_operation_leases (
			lease_token UUID PRIMARY KEY,
			user_id VARCHAR(255) NOT NULL,
			resource TEXT NOT NULL,
			mode VARCHAR(16) NOT NULL CHECK (mode IN ('shared', 'exclusive')),
			kind VARCHAR(64) NOT NULL,
			acquired_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			lease_expires_at TIMESTAMP WITH TIME ZONE NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_index_operation_leases_user
			ON index_operation_leases(user_id, lease_expires_at);

		-- 存量库补列。CREATE TABLE IF NOT EXISTS 对已存在的表不生效，
		-- 因此列的新增必须同时出现在 CREATE 文本（新库）和 ALTER（旧库）两处；
		-- 只改 CREATE 文本会让升级后的旧库在引用新列的 SQL 上直接报错。
		ALTER TABLE index_workspaces ADD COLUMN IF NOT EXISTS cloud_revision BIGINT NOT NULL DEFAULT 0;
		ALTER TABLE index_jobs ADD COLUMN IF NOT EXISTS cloud_revision BIGINT NOT NULL DEFAULT 0;
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

// graftUnreadablePaths 把"本次扫描读不了、但上次已索引"的文件按旧快照条目并入
// manifest 当作未变更处理：不重传、不判删，完成时快照保留旧哈希；文件恢复可读
// 后由正常 diff 按哈希差异重新上传。不在旧快照里的 unreadable 路径没有可保留
// 的状态，直接忽略。
func graftUnreadablePaths(previous map[string]indexManifestFile, files []indexManifestFile, unreadable []string) ([]indexManifestFile, error) {
	if len(unreadable) == 0 {
		return files, nil
	}
	if len(unreadable) > maxIndexManifestFiles {
		return nil, fmt.Errorf("unreadable file list exceeds %d files", maxIndexManifestFiles)
	}
	current := make(map[string]bool, len(files))
	for _, file := range files {
		current[file.Path] = true
	}
	grafted := false
	for _, rawPath := range unreadable {
		filePath := normalizeIndexPath(rawPath)
		if filePath == "" || current[filePath] {
			continue
		}
		current[filePath] = true
		old, ok := previous[filePath]
		if !ok {
			continue
		}
		files = append(files, old)
		grafted = true
	}
	if len(files) > maxIndexManifestFiles {
		return nil, fmt.Errorf("manifest exceeds %d files", maxIndexManifestFiles)
	}
	if grafted {
		sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	}
	return files, nil
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

func loadIndexedWorkspaceRoot(ctx context.Context, userID, workspaceID string) (string, bool, error) {
	var rootID string
	err := db.QueryRowContext(ctx, `
		SELECT root_id FROM index_workspaces WHERE user_id = $1 AND workspace_id = $2
	`, userID, workspaceID).Scan(&rootID)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return normalizeIndexRootID(rootID), true, nil
}

func workspaceRootBindingChanged(exists bool, storedRootID, requestedRootID string) bool {
	return exists && normalizeIndexRootID(storedRootID) != normalizeIndexRootID(requestedRootID)
}

// A workspace moving between roots invalidates the old root, not the whole
// tenant. Legacy data may bind several workspaces to the default root, so
// every service snapshot bound to oldRootID is invalidated together. The old
// binding remains the durable retry marker until both LCE and service cleanup
// succeed.
func resetWorkspaceRootBinding(ctx context.Context, userID, workspaceID, oldRootID, newRootID string) error {
	result, err := lce.callToolWithTimeout(
		ctx,
		"codebase_clear_index",
		map[string]interface{}{"tenant_id": userID, "root_id": lceIndexRootID(oldRootID)},
		remoteIndexMCPCallTimeout,
	)
	if err != nil {
		return newIndexUpstreamError("clear old LCE root for workspace root migration: %w", err)
	}
	if result == nil || result.IsError {
		detail := "empty LCE response"
		if result != nil {
			detail = string(result.Content)
		}
		return newIndexUpstreamError("clear old LCE root for workspace root migration: %s", detail)
	}
	if err := clearRootIndexState(ctx, userID, oldRootID); err != nil {
		return fmt.Errorf("clear service snapshots bound to old root: %w", err)
	}
	log.Printf(
		"[INDEX] Reset old root for tenant %s after workspace %s changed from %q to %q",
		userID,
		workspaceID,
		oldRootID,
		newRootID,
	)
	return nil
}

type expiredActiveIndexJob struct {
	id            string
	rootID        string
	cloudRevision int64
}

// inspectActiveIndexJob makes start idempotent across client processes and relay
// instances. A healthy running job owns the workspace until it completes or its
// heartbeat expires; a repeated start must never supersede it and create another
// provider-consuming LCE job. Expired jobs are reclaimed under the same database
// lock before a replacement is allowed.
func inspectActiveIndexJob(
	ctx context.Context,
	userID string,
	workspaceID string,
) (*indexJobView, error) {
	tx, err := beginLockedIndexUserTx(ctx, userID)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		SELECT id::text, root_id, cloud_revision, heartbeat_at
		FROM index_jobs
		WHERE user_id = $1 AND workspace_id = $2 AND status = $3
		ORDER BY started_at DESC
		FOR UPDATE
	`, userID, workspaceID, indexJobStatusRunning)
	if err != nil {
		return nil, err
	}
	cutoff := time.Now().Add(-indexJobHeartbeatTimeout)
	var activeJobID string
	var expired []expiredActiveIndexJob
	for rows.Next() {
		var job expiredActiveIndexJob
		var heartbeatAt time.Time
		if err := rows.Scan(&job.id, &job.rootID, &job.cloudRevision, &heartbeatAt); err != nil {
			rows.Close()
			return nil, err
		}
		if !heartbeatAt.Before(cutoff) {
			if activeJobID == "" {
				activeJobID = job.id
			}
			continue
		}
		expired = append(expired, job)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if activeJobID != "" {
		job, err := loadIndexJobFrom(ctx, tx, userID, activeJobID)
		if err != nil {
			return nil, err
		}
		return &job, nil
	}

	for _, job := range expired {
		if _, err := tx.ExecContext(ctx, `
			UPDATE index_jobs
			SET status = $1, phase = 'done', failed_files = total_files - indexed_files,
				error = 'index job heartbeat timed out', completed_at = NOW()
			WHERE id = $2 AND user_id = $3 AND status = $4
		`, indexJobStatusTimedOut, job.id, userID, indexJobStatusRunning); err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM index_job_files WHERE job_id = $1`, job.id); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	for _, job := range expired {
		if job.cloudRevision > 0 {
			continue
		}
		if abortErr := abortLCEIndexJob(context.Background(), userID, job.id, job.rootID); abortErr != nil {
			log.Printf("[INDEX] cloud abort cleanup failed for expired job %s: %v", job.id, abortErr)
		}
	}
	return nil, nil
}

func createIndexJob(ctx context.Context, userID string, req indexStartRequest) (createIndexJobResponse, error) {
	if req.ProtocolVersion != indexProtocolVersion {
		return createIndexJobResponse{}, fmt.Errorf(
			"unsupported indexing protocol version %d (server requires %d)",
			req.ProtocolVersion,
			indexProtocolVersion,
		)
	}
	req.WorkspaceID = strings.TrimSpace(req.WorkspaceID)
	req.WorkspaceName = strings.TrimSpace(req.WorkspaceName)
	req.Branch = strings.TrimSpace(req.Branch)
	req.Revision = strings.TrimSpace(req.Revision)
	req.RootID = normalizeIndexRootID(req.RootID)
	if req.WorkspaceID == "" || len(req.WorkspaceID) > 128 {
		return createIndexJobResponse{}, fmt.Errorf("workspace_id is required and must not exceed 128 bytes")
	}
	if req.RootID == "" {
		return createIndexJobResponse{}, fmt.Errorf("root_id is required by indexing protocol v1")
	}
	if len(req.RootID) > maxIndexRootIDLen {
		return createIndexJobResponse{}, fmt.Errorf("root_id exceeds %d bytes", maxIndexRootIDLen)
	}
	files, err := normalizeManifest(req.Files)
	if err != nil {
		return createIndexJobResponse{}, err
	}
	lease, err := acquireExclusiveIndexOperation(ctx, userID, "create-job")
	if err != nil {
		return createIndexJobResponse{}, err
	}
	defer lease.Release()
	opCtx := lease.Context()
	activeJob, err := inspectActiveIndexJob(opCtx, userID, req.WorkspaceID)
	if err != nil {
		return createIndexJobResponse{}, err
	}
	if activeJob != nil {
		return createIndexJobResponse{
			ActiveJob:         activeJob,
			Busy:              true,
			BusyReason:        indexStartBusyActiveJob,
			RetryAfterSeconds: int(indexJobSweepInterval / time.Second),
		}, nil
	}
	storedRootID, workspaceExistsBeforeReset, err := loadIndexedWorkspaceRoot(opCtx, userID, req.WorkspaceID)
	if err != nil {
		return createIndexJobResponse{}, err
	}
	if workspaceRootBindingChanged(workspaceExistsBeforeReset, storedRootID, req.RootID) {
		if err := resetWorkspaceRootBinding(
			opCtx,
			userID,
			req.WorkspaceID,
			storedRootID,
			req.RootID,
		); err != nil {
			return createIndexJobResponse{}, err
		}
	}

	tx, err := beginLockedIndexUserTx(opCtx, userID)
	if err != nil {
		return createIndexJobResponse{}, err
	}
	defer tx.Rollback()

	previous, err := loadIndexedSnapshot(opCtx, tx, userID, req.WorkspaceID)
	if err != nil {
		return createIndexJobResponse{}, err
	}
	var workspaceExists bool
	err = tx.QueryRowContext(opCtx, `
		SELECT EXISTS(
			SELECT 1 FROM index_workspaces WHERE user_id = $1 AND workspace_id = $2
		)
	`, userID, req.WorkspaceID).Scan(&workspaceExists)
	if err != nil {
		return createIndexJobResponse{}, err
	}

	files, err = graftUnreadablePaths(previous, files, req.UnreadableFiles)
	if err != nil {
		return createIndexJobResponse{}, err
	}

	pending, deleted, estimatedChunks := diffManifest(previous, files)
	if workspaceExists && len(pending) == 0 && len(deleted) == 0 {
		if _, err := tx.ExecContext(opCtx, `
			UPDATE index_workspaces
			SET workspace_name = $3, root_id = $4, branch = $5, revision = $6
			WHERE user_id = $1 AND workspace_id = $2
		`, userID, req.WorkspaceID, req.WorkspaceName, req.RootID, req.Branch, req.Revision); err != nil {
			return createIndexJobResponse{}, err
		}
		if err := tx.Commit(); err != nil {
			return createIndexJobResponse{}, err
		}
		return createIndexJobResponse{Unchanged: true}, nil
	}
	if wait := checkIndexStartRateLimit(userID, req.RootID, time.Now()); wait > 0 {
		return createIndexJobResponse{
			Busy:              true,
			BusyReason:        indexStartBusyRateLimited,
			RetryAfterSeconds: wait,
		}, nil
	}
	pendingSet := make(map[string]bool, len(pending))
	for _, filePath := range pending {
		pendingSet[filePath] = true
	}
	mode := "incremental"
	if !workspaceExists {
		mode = "full"
	}
	jobID := uuid.New().String()
	_, err = tx.ExecContext(opCtx, `
		INSERT INTO index_jobs (
			id, user_id, workspace_id, workspace_name, root_id, branch, revision, mode,
			workspace_files, total_files, total_chunks, deleted_count
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`, jobID, userID, req.WorkspaceID, req.WorkspaceName, req.RootID, req.Branch, req.Revision, mode,
		len(files), len(pending), estimatedChunks, len(deleted))
	if err != nil {
		return createIndexJobResponse{}, err
	}

	// COPY 批量写入 manifest：10 万文件逐条 INSERT 要 10 万次网络往返，
	// 会把创建 job 的事务拖到分钟级并一直占着 advisory 锁。
	stmt, err := tx.PrepareContext(opCtx, pq.CopyIn(
		"index_job_files", "job_id", "path", "hash", "size", "estimated_chunks", "needs_index",
	))
	if err != nil {
		return createIndexJobResponse{}, err
	}
	for _, file := range files {
		if _, err := stmt.ExecContext(opCtx, jobID, file.Path, file.Hash, file.Size, file.EstimatedChunks, pendingSet[file.Path]); err != nil {
			_ = stmt.Close()
			return createIndexJobResponse{}, err
		}
	}
	// 无参 Exec 刷出 COPY 缓冲；错误（如约束冲突）大多在这里才暴露。
	if _, err := stmt.ExecContext(opCtx); err != nil {
		_ = stmt.Close()
		return createIndexJobResponse{}, err
	}
	if err := stmt.Close(); err != nil {
		return createIndexJobResponse{}, err
	}

	if err := tx.Commit(); err != nil {
		return createIndexJobResponse{}, err
	}
	if err := beginLCEIndexJob(opCtx, userID, jobID, req.RootID, mode == "full"); err != nil {
		if abortErr := abortLCEIndexJob(context.Background(), userID, jobID, req.RootID); abortErr != nil {
			log.Printf("[INDEX] cloud abort cleanup failed after begin error for job %s: %v", jobID, abortErr)
		}
		_, _ = db.ExecContext(context.Background(), `
			UPDATE index_jobs
			SET status = $1, phase = 'done', error = $2, heartbeat_at = NOW(), completed_at = NOW()
			WHERE id = $3 AND user_id = $4 AND status = $5
		`, indexJobStatusFailed, "LCE cloud index begin failed: "+err.Error(), jobID, userID, indexJobStatusRunning)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM index_job_files WHERE job_id = $1`, jobID)
		return createIndexJobResponse{}, err
	}
	job, err := loadIndexJob(opCtx, userID, jobID)
	if err != nil {
		return createIndexJobResponse{}, err
	}
	return createIndexJobResponse{
		Job:          &job,
		PendingFiles: pending,
		DeletedFiles: deleted,
	}, nil
}

func getIndexJob(ctx context.Context, userID, jobID string) (indexJobView, error) {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return indexJobView{}, fmt.Errorf("index job id is required")
	}
	job, err := loadIndexJob(ctx, userID, jobID)
	if err == sql.ErrNoRows {
		return indexJobView{}, fmt.Errorf("index job not found")
	}
	if err != nil {
		return indexJobView{}, err
	}
	if job.Status == indexJobStatusRunning {
		if renewErr := renewLCEIndexJob(ctx, userID, job.ID, job.RootID); renewErr != nil {
			log.Printf("[INDEX] cloud renew best-effort failed for job %s (status query continues): %v", jobID, renewErr)
		}
		if _, err := db.ExecContext(ctx, `
			UPDATE index_jobs SET heartbeat_at = NOW()
			WHERE id = $1 AND user_id = $2 AND status = $3
		`, jobID, userID, indexJobStatusRunning); err != nil {
			return indexJobView{}, err
		}
		job, err = loadIndexJob(ctx, userID, jobID)
		if err != nil {
			return indexJobView{}, err
		}
	}
	return job, nil
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
		SELECT id::text, workspace_id, workspace_name, root_id, branch, revision, mode, phase, status,
			workspace_files, total_files, indexed_files, failed_files, total_chunks,
			indexed_chunks, chunk_count_fallback, deleted_count, error, cloud_revision,
			started_at, heartbeat_at, completed_at
		FROM index_jobs
		WHERE id = $1 AND user_id = $2
	`, jobID, userID).Scan(
		&job.ID, &job.WorkspaceID, &job.WorkspaceName, &job.RootID, &job.Branch, &job.Revision,
		&job.Mode, &job.Phase, &job.Status, &job.WorkspaceFiles, &job.TotalFiles,
		&job.IndexedFiles, &job.FailedFiles, &job.TotalChunks, &job.IndexedChunks,
		&fallback, &job.DeletedCount, &job.Error, &job.CloudRevision, &job.StartedAt, &job.HeartbeatAt,
		&job.CompletedAt,
	)
	job.ChunkCountEstimated = fallback || job.Status != indexJobStatusCompleted
	return job, err
}

type indexUploadRequest struct {
	JobID string
	Files []indexManifestFile
	// 仓库维度：多 root 工作区按文件夹拆分索引任务，每个 job 一个稳定 root_id。
	// 同一 job 的所有批次必须携带相同且非空的 root_id。
	RootID string
}

type indexBatchResponse struct {
	Job indexJobView `json:"job"`
	LCE interface{}  `json:"lce"`
}

const maxIndexRootIDLen = 128
const defaultLCEIndexRootID = "default"

// normalizeIndexRootID only trims. Length is validated separately so two distinct
// identities can never be silently truncated into the same tenant root.
func normalizeIndexRootID(value string) string {
	return strings.TrimSpace(value)
}

func lceIndexRootID(value string) string {
	normalized := normalizeIndexRootID(value)
	if normalized == "" {
		return defaultLCEIndexRootID
	}
	return normalized
}

// validateIndexBatchSize 校验一个批次的体积并返回其内容总字节数。
//
// 两道上限缺一不可：只限单文件的话，凑满一批同样能推送巨量内容；只限单批的话，
// 一个超大文件就能占满整批。返回的字节数即该批次要计入索引配额的量。
func validateIndexBatchSize(files []indexManifestFile) (int64, error) {
	var total int64
	for _, file := range files {
		size := int64(len(file.Content))
		if size > maxIndexFileBytes {
			return 0, fmt.Errorf("file exceeds the %d byte limit: %s", maxIndexFileBytes, file.Path)
		}
		total += size
	}
	if total > maxIndexBatchBytes {
		return 0, fmt.Errorf("index batch exceeds the %d byte limit", maxIndexBatchBytes)
	}
	return total, nil
}

func uploadIndexBatch(ctx context.Context, userID string, req indexUploadRequest) (indexBatchResponse, error) {
	req.JobID = strings.TrimSpace(req.JobID)
	req.RootID = normalizeIndexRootID(req.RootID)
	if req.JobID == "" || req.RootID == "" || len(req.Files) > maxIndexBatchFiles || len(req.RootID) > maxIndexRootIDLen {
		return indexBatchResponse{}, fmt.Errorf("invalid index batch")
	}

	// 体积校验放在最前：这些字节最终都会变成 embedding 调用，超限的批次不该
	// 走到建事务、查 manifest 这些更贵的步骤。
	_, err := validateIndexBatchSize(req.Files)
	if err != nil {
		return indexBatchResponse{}, err
	}
	lease, err := acquireSharedIndexOperation(
		ctx, userID, indexJobOperationResource(req.JobID), "upload-batch",
	)
	if err != nil {
		return indexBatchResponse{}, err
	}
	defer lease.Release()
	opCtx := lease.Context()
	// 事务A：只做校验与读取，随后立即提交释放连接和 advisory 锁。
	// 决不能让事务横跨下面那次 LCE 调用：连接池只有 25 个连接，
	// 几十个并发 embedding 批次就会把连接占光，阻塞全站的 DB 访问。
	tx, err := beginLockedIndexUserTx(opCtx, userID)
	if err != nil {
		return indexBatchResponse{}, err
	}
	job, staged, deleted, err := prepareIndexBatch(opCtx, tx, userID, req)
	// 不变量：prepareIndexBatch 只在"job 不存在或不属于该用户"这一种情况下
	// 外传 sql.ErrNoRows；它内部的按文件、按 job 字段查询都会把各自的 ErrNoRows
	// 包装成带上下文的错误。新增查询时请保持这条，否则会被误报为 job 不存在。
	if err == sql.ErrNoRows {
		_ = tx.Rollback()
		return indexBatchResponse{}, fmt.Errorf("index job not found")
	}
	if err != nil {
		_ = tx.Rollback()
		return indexBatchResponse{}, err
	}
	if err := tx.Commit(); err != nil {
		return indexBatchResponse{}, err
	}
	batchBytes, err := validateIndexBatchSize(staged)
	if err != nil {
		return indexBatchResponse{}, err
	}
	filesArg := make([]map[string]interface{}, 0, len(staged))
	for _, file := range staged {
		filesArg = append(filesArg, map[string]interface{}{
			"path": file.Path, "content": file.Content,
		})
	}
	args := lceIndexJobArgs(userID, job.ID, req.RootID, "stage")
	args["files"] = filesArg
	if len(deleted) > 0 {
		args["deleted_files"] = deleted
	}
	if _, err := validateMCPToolCallBody("codebase_remote_index", args); err != nil {
		return indexBatchResponse{}, err
	}
	// 字节配额按租户池计（uploadIndexBatch 的 userID 参数即租户）；org 归属
	// 从认证中间件放进 request context 的身份里取，缺失时按个人租户处理。
	callerIdentity := authIdentityFromContext(ctx)
	quota := chargeIndexBytes(userID, callerIdentity.OrgID, userTierFromContext(ctx), batchBytes)
	if quota.Unavailable {
		return indexBatchResponse{}, fmt.Errorf("index quota accounting temporarily unavailable; retry later")
	}
	if !quota.Allowed {
		return indexBatchResponse{}, fmt.Errorf(
			"daily index quota exceeded (%d/%d bytes); retry after %s seconds",
			quota.Used,
			quota.Limit,
			quotaRetryAfterHeader(time.Now()),
		)
	}
	result, err := lce.callToolWithTimeout(opCtx, "codebase_remote_index", args, remoteIndexMCPCallTimeout)
	if err != nil {
		return indexBatchResponse{}, newIndexUpstreamError("LCE index call failed: %w", err)
	}
	if result.IsError {
		return indexBatchResponse{}, newIndexUpstreamError("LCE index call failed: %s", string(result.Content))
	}

	chunks, exact := extractChunkCount(result.Content)
	if !exact {
		for _, file := range staged {
			chunks += int64(file.EstimatedChunks)
		}
	}
	// 事务B：重新加锁提交进度。commitIndexBatch 内部会 SELECT...FOR UPDATE
	// 复查 job 状态、并对每个文件的 UPDATE 断言 rows==1，因此事务A提交后到
	// 这里之间发生的 supersede/清除/重放都会被拒绝。配额按已发起的上游
	// embedding 工作计费，即使后续提交失败也不回退，避免重放绕过成本上限。
	tx2, err := beginLockedIndexUserTx(opCtx, userID)
	if err != nil {
		return indexBatchResponse{}, err
	}
	defer tx2.Rollback()
	if err := commitIndexBatch(opCtx, tx2, userID, job.ID, staged, chunks, !exact, len(deleted) > 0); err != nil {
		return indexBatchResponse{}, err
	}
	if err := tx2.Commit(); err != nil {
		return indexBatchResponse{}, err
	}
	updated, err := loadIndexJob(opCtx, userID, job.ID)
	if err != nil {
		return indexBatchResponse{}, err
	}
	var lceResponse interface{}
	if err := json.Unmarshal(result.Content, &lceResponse); err != nil {
		lceResponse = string(result.Content)
	}
	return indexBatchResponse{Job: updated, LCE: lceResponse}, nil
}

func prepareIndexBatch(ctx context.Context, tx *sql.Tx, userID string, req indexUploadRequest) (indexJobView, []indexManifestFile, []string, error) {
	job, err := loadIndexJobFrom(ctx, tx, userID, req.JobID)
	if err != nil {
		return job, nil, nil, err
	}
	if job.Status != indexJobStatusRunning {
		return job, nil, nil, fmt.Errorf("index job is %s", job.Status)
	}
	if normalizeIndexRootID(req.RootID) != job.RootID {
		return job, nil, nil, fmt.Errorf("index batch root_id does not match the job")
	}
	if len(req.Files) == 0 && job.DeletedCount == 0 {
		return job, nil, nil, fmt.Errorf("empty index batch")
	}

	staged := make([]indexManifestFile, 0, len(req.Files))
	seen := make(map[string]bool, len(req.Files))
	paths := make([]string, 0, len(req.Files))
	inputs := make([]indexManifestFile, 0, len(req.Files))
	for _, input := range req.Files {
		input.Path = normalizeIndexPath(input.Path)
		if input.Path == "" || seen[input.Path] {
			return job, nil, nil, fmt.Errorf("invalid or duplicate batch path")
		}
		seen[input.Path] = true
		paths = append(paths, input.Path)
		inputs = append(inputs, input)
	}

	// 一次性把整批文件的服务端记录查出来再内存比对：逐文件 SELECT 会给
	// 每批数十个文件各来一次网络往返，全部发生在持 advisory 锁的事务里。
	type jobFileRow struct {
		hash            string
		size            int64
		estimatedChunks int
		needsIndex      bool
		indexed         bool
	}
	records := make(map[string]jobFileRow, len(paths))
	if len(paths) > 0 {
		rows, err := tx.QueryContext(ctx, `
			SELECT path, hash, size, estimated_chunks, needs_index, indexed
			FROM index_job_files
			WHERE job_id = $1 AND path = ANY($2)
		`, req.JobID, pq.Array(paths))
		if err != nil {
			return job, nil, nil, err
		}
		for rows.Next() {
			var p string
			var rec jobFileRow
			if err := rows.Scan(&p, &rec.hash, &rec.size, &rec.estimatedChunks, &rec.needsIndex, &rec.indexed); err != nil {
				rows.Close()
				return job, nil, nil, err
			}
			records[p] = rec
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return job, nil, nil, err
		}
	}
	for _, input := range inputs {
		// "查不到"表示这个文件不在该 job 的清单里，不是"job 不存在"。
		// 若表现为 sql.ErrNoRows 会被 codebase_index upload 判成 index job not
		// found，把一个批次错误报成任务丢失，排查时指向错误方向。
		rec, ok := records[input.Path]
		if !ok {
			return job, nil, nil, fmt.Errorf("batch file is not part of this job: %s", input.Path)
		}
		if !rec.needsIndex || rec.indexed || strings.TrimSpace(input.Hash) != rec.hash {
			return job, nil, nil, fmt.Errorf("batch file is stale or already indexed: %s", input.Path)
		}
		if err := validateIndexContent(rec.hash, rec.size, input.Content); err != nil {
			return job, nil, nil, fmt.Errorf("batch file content does not match manifest: %s: %w", input.Path, err)
		}
		input.Hash = rec.hash
		input.EstimatedChunks = rec.estimatedChunks
		staged = append(staged, input)
	}

	var deletionsSent bool
	err = tx.QueryRowContext(ctx, `SELECT deletions_sent FROM index_jobs WHERE id = $1`, req.JobID).Scan(&deletionsSent)
	// job 在本事务开头已确认存在，这里的 ErrNoRows 只可能是并发删除；同样不能
	// 让它伪装成上面那次 job 不存在错误。
	if err == sql.ErrNoRows {
		return job, nil, nil, fmt.Errorf("index job disappeared while staging the batch")
	}
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

func validateIndexContent(expectedHash string, expectedSize int64, content string) error {
	contentBytes := []byte(content)
	if int64(len(contentBytes)) != expectedSize {
		return fmt.Errorf("size is %d bytes, expected %d", len(contentBytes), expectedSize)
	}
	actualHash := fmt.Sprintf("%x", sha256.Sum256(contentBytes))
	if !strings.EqualFold(actualHash, strings.TrimSpace(expectedHash)) {
		return fmt.Errorf("SHA-256 is %s, expected %s", actualHash, expectedHash)
	}
	return nil
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

func completeIndexJob(ctx context.Context, userID, jobID string) (indexJobView, error) {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return indexJobView{}, fmt.Errorf("index job id is required")
	}
	lease, err := acquireSharedIndexOperation(
		ctx, userID, indexJobOperationResource(jobID), "complete-job",
	)
	if err != nil {
		return indexJobView{}, err
	}
	defer lease.Release()
	opCtx := lease.Context()

	// Validate first without holding a transaction, publish the staged PostgreSQL revision,
	// then revalidate every completion invariant before committing Relay's manifest snapshot.
	var preStatus, rootID string
	var preTotalFiles, preIndexedFiles, preDeletedCount int
	var preDeletionsSent bool
	var preCloudRevision int64
	err = db.QueryRowContext(opCtx, `
		SELECT status, root_id, total_files, indexed_files, deleted_count, deletions_sent, cloud_revision
		FROM index_jobs WHERE id = $1 AND user_id = $2
	`, jobID, userID).Scan(
		&preStatus, &rootID, &preTotalFiles, &preIndexedFiles, &preDeletedCount, &preDeletionsSent, &preCloudRevision,
	)
	if err == sql.ErrNoRows {
		return indexJobView{}, fmt.Errorf("index job not found")
	}
	if err != nil {
		return indexJobView{}, err
	}
	if preStatus != indexJobStatusRunning {
		return indexJobView{}, fmt.Errorf("index job is %s", preStatus)
	}
	if preIndexedFiles != preTotalFiles {
		return indexJobView{}, fmt.Errorf("index job still has pending files")
	}
	if preDeletedCount > 0 && !preDeletionsSent {
		return indexJobView{}, fmt.Errorf("index job still has pending deletions")
	}
	var cloudRevision int64
	if preCloudRevision > 0 {
		cloudRevision = preCloudRevision
	} else {
		args := lceIndexJobArgs(userID, jobID, rootID, "publish")
		if _, err := validateMCPToolCallBody("codebase_remote_index", args); err != nil {
			return indexJobView{}, err
		}
		result, err := lce.callToolWithTimeout(opCtx, "codebase_remote_index", args, remoteIndexMCPCallTimeout)
		if err != nil {
			return indexJobView{}, newIndexUpstreamError("LCE cloud index publish failed: %w", err)
		}
		if result.IsError {
			return indexJobView{}, newIndexUpstreamError("LCE cloud index publish failed: %s", string(result.Content))
		}
		cloudRevision, err = extractCloudRevision(result.Content)
		if err != nil {
			return indexJobView{}, newIndexUpstreamError("LCE cloud index publish returned an invalid revision: %w", err)
		}
		// 这条写入是防重复 publish（preCloudRevision > 0 短路）和防 sweeper
		// 误 abort（heartbeat 续期）的前提：失败不阻断本次 complete，但必须可见。
		if _, err := db.ExecContext(opCtx, `
			UPDATE index_jobs SET cloud_revision = $1, heartbeat_at = NOW()
			WHERE id = $2 AND user_id = $3 AND status = $4
		`, cloudRevision, jobID, userID, indexJobStatusRunning); err != nil {
			log.Printf("[INDEX] failed to record cloud_revision %d for job %s (user=%s): %v",
				cloudRevision, jobID, userID, err)
		}
	}

	tx, err := beginLockedIndexUserTx(opCtx, userID)
	if err != nil {
		return indexJobView{}, err
	}
	defer tx.Rollback()

	var workspaceID, workspaceName, branch, revision, status string
	var totalFiles, indexedFiles, deletedCount int
	var deletionsSent, fallback bool
	err = tx.QueryRowContext(opCtx, `
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
		return indexJobView{}, fmt.Errorf("index job not found")
	}
	if err != nil {
		return indexJobView{}, err
	}
	if status != indexJobStatusRunning {
		return indexJobView{}, fmt.Errorf("index job is %s", status)
	}
	if indexedFiles != totalFiles {
		return indexJobView{}, fmt.Errorf("index job still has pending files")
	}
	if deletedCount > 0 && !deletionsSent {
		return indexJobView{}, fmt.Errorf("index job still has pending deletions")
	}

	if _, err = tx.ExecContext(opCtx, `
		DELETE FROM indexed_files WHERE user_id = $1 AND workspace_id = $2
	`, userID, workspaceID); err != nil {
		return indexJobView{}, err
	}
	if _, err = tx.ExecContext(opCtx, `
		INSERT INTO indexed_files (
			user_id, workspace_id, path, hash, size, estimated_chunks
		)
		SELECT $1, $2, path, hash, size, estimated_chunks
		FROM index_job_files
		WHERE job_id = $3
	`, userID, workspaceID, jobID); err != nil {
		return indexJobView{}, err
	}
	if _, err = tx.ExecContext(opCtx, `
			INSERT INTO index_workspaces (
				user_id, workspace_id, workspace_name, root_id, branch, revision, cloud_revision, indexed_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
		ON CONFLICT (user_id, workspace_id) DO UPDATE SET
			workspace_name = EXCLUDED.workspace_name,
			root_id = EXCLUDED.root_id,
			branch = EXCLUDED.branch,
				revision = EXCLUDED.revision,
				cloud_revision = EXCLUDED.cloud_revision,
				indexed_at = NOW()
		`, userID, workspaceID, workspaceName, rootID, branch, revision, cloudRevision); err != nil {
		return indexJobView{}, err
	}
	if _, err = tx.ExecContext(opCtx, `
		UPDATE index_jobs
		SET status = $3, phase = 'done',
			total_chunks = CASE WHEN $4 THEN indexed_chunks ELSE total_chunks END,
			cloud_revision = $2, heartbeat_at = NOW(), completed_at = NOW()
		WHERE id = $1
	`, jobID, cloudRevision, indexJobStatusCompleted, !fallback); err != nil {
		return indexJobView{}, err
	}
	if _, err = tx.ExecContext(opCtx, `DELETE FROM index_job_files WHERE job_id = $1`, jobID); err != nil {
		return indexJobView{}, err
	}
	if err = tx.Commit(); err != nil {
		return indexJobView{}, err
	}
	job, err := loadIndexJob(opCtx, userID, jobID)
	if err != nil {
		return indexJobView{}, err
	}
	return job, nil
}

func beginLCEIndexJob(ctx context.Context, userID, jobID, rootID string, replaceRoot bool) error {
	args := lceBeginIndexJobArgs(userID, jobID, rootID, replaceRoot)
	if _, err := validateMCPToolCallBody("codebase_remote_index", args); err != nil {
		return err
	}
	result, err := lce.callToolWithTimeout(ctx, "codebase_remote_index", args, remoteIndexMCPCallTimeout)
	if err != nil {
		return newIndexUpstreamError("LCE cloud index begin failed: %w", err)
	}
	if result.IsError {
		return newIndexUpstreamError("LCE cloud index begin failed: %s", string(result.Content))
	}
	return nil
}

func lceBeginIndexJobArgs(userID, jobID, rootID string, replaceRoot bool) map[string]interface{} {
	args := lceIndexJobArgs(userID, jobID, rootID, "begin")
	if replaceRoot {
		args["replace_root"] = true
	}
	return args
}

func renewLCEIndexJob(ctx context.Context, userID, jobID, rootID string) error {
	args := lceIndexJobArgs(userID, jobID, rootID, "renew")
	if _, err := validateMCPToolCallBody("codebase_remote_index", args); err != nil {
		return err
	}
	result, err := lce.callToolWithTimeout(ctx, "codebase_remote_index", args, indexJobRenewCallTimeout)
	if err != nil {
		return newIndexUpstreamError("LCE cloud index renew failed: %w", err)
	}
	if result.IsError {
		return newIndexUpstreamError("LCE cloud index renew failed: %s", string(result.Content))
	}
	return nil
}

func abortLCEIndexJob(ctx context.Context, userID, jobID, rootID string) error {
	args := lceIndexJobArgs(userID, jobID, rootID, "abort")
	if _, err := validateMCPToolCallBody("codebase_remote_index", args); err != nil {
		return err
	}
	result, err := lce.callToolWithTimeout(ctx, "codebase_remote_index", args, remoteIndexMCPCallTimeout)
	if err != nil {
		return newIndexUpstreamError("LCE cloud index abort failed: %w", err)
	}
	if result.IsError {
		return newIndexUpstreamError("LCE cloud index abort failed: %s", string(result.Content))
	}
	return nil
}

func lceIndexJobArgs(userID, jobID, rootID, operation string) map[string]interface{} {
	return map[string]interface{}{
		"tenant_id": userID,
		"job_id":    jobID,
		"operation": operation,
		"root_id":   rootID,
	}
}

func extractCloudRevision(content []byte) (int64, error) {
	dec := json.NewDecoder(bytes.NewReader(content))
	dec.UseNumber()
	var value interface{}
	if err := dec.Decode(&value); err != nil {
		return 0, err
	}
	object, ok := value.(map[string]interface{})
	if !ok {
		return 0, fmt.Errorf("publish response is not an object")
	}
	if nested, ok := object["payload"].(map[string]interface{}); ok {
		object = nested
	}
	raw, ok := object["revision"]
	if !ok {
		return 0, fmt.Errorf("publish response has no revision")
	}
	number, ok := raw.(json.Number)
	if !ok {
		return 0, fmt.Errorf("revision has an invalid type")
	}
	rev, err := number.Int64()
	if err != nil || rev < 0 {
		return 0, fmt.Errorf("revision is not a non-negative integer")
	}
	return rev, nil
}

func failIndexJob(ctx context.Context, userID, jobID, failure string) (indexJobView, error) {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return indexJobView{}, fmt.Errorf("index job id is required")
	}
	failure = strings.TrimSpace(failure)
	if len(failure) > 2000 {
		failure = truncateUTF8(failure, 2000)
	}
	if failure == "" {
		failure = "client reported indexing failure"
	}
	lease, err := acquireSharedIndexOperation(
		ctx, userID, indexJobOperationResource(jobID), "fail-job",
	)
	if err != nil {
		return indexJobView{}, err
	}
	defer lease.Release()
	opCtx := lease.Context()
	tx, err := beginLockedIndexUserTx(opCtx, userID)
	if err != nil {
		return indexJobView{}, err
	}
	defer tx.Rollback()
	var rootID string
	if err := tx.QueryRowContext(opCtx, `
		SELECT root_id FROM index_jobs WHERE id = $1 AND user_id = $2 FOR UPDATE
	`, jobID, userID).Scan(&rootID); err != nil {
		if err == sql.ErrNoRows {
			return indexJobView{}, fmt.Errorf("index job not found")
		}
		return indexJobView{}, err
	}
	result, err := tx.ExecContext(opCtx, `
		UPDATE index_jobs
		SET status = $1, phase = 'done', failed_files = total_files - indexed_files,
			error = $2, heartbeat_at = NOW(), completed_at = NOW()
		WHERE id = $3 AND user_id = $4 AND status = $5
	`, indexJobStatusFailed, failure, jobID, userID, indexJobStatusRunning)
	if err != nil {
		return indexJobView{}, err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return indexJobView{}, fmt.Errorf("index job is not running")
	}
	if _, err = tx.ExecContext(opCtx, `DELETE FROM index_job_files WHERE job_id = $1`, jobID); err != nil {
		return indexJobView{}, err
	}
	if err = tx.Commit(); err != nil {
		return indexJobView{}, err
	}
	if abortErr := abortLCEIndexJob(opCtx, userID, jobID, rootID); abortErr != nil {
		log.Printf("[INDEX] cloud abort cleanup failed for job %s: %v", jobID, abortErr)
	}
	job, err := loadIndexJob(opCtx, userID, jobID)
	if err != nil {
		return indexJobView{}, err
	}
	return job, nil
}

func clearUserIndexStateTx(ctx context.Context, tx *sql.Tx, userID string) error {
	statements := []string{
		`DELETE FROM index_jobs WHERE user_id = $1`,
		`DELETE FROM indexed_files WHERE user_id = $1`,
		`DELETE FROM index_workspaces WHERE user_id = $1`,
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement, userID); err != nil {
			return err
		}
	}
	return nil
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

// clearRootIndexStateTx 删除该 root 的所有 relay 侧行，返回 indexed_files 的删除
// 行数（供 delete-root 上报 deleted_files）。index_jobs 必须直接按 jobs.root_id
// 匹配：首次索引失败时还没有 index_workspaces 行，若通过 workspace JOIN 删除会
// 留下无法从控制台清掉的失败任务。
func clearRootIndexStateTx(ctx context.Context, tx *sql.Tx, userID, rootID string) (int64, error) {
	normalizedRootID := lceIndexRootID(rootID)
	statements := []string{
		`DELETE FROM index_jobs
		 WHERE user_id = $1
		   AND CASE WHEN BTRIM(root_id) = '' THEN 'default' ELSE BTRIM(root_id) END = $2`,
		`DELETE FROM indexed_files AS files
		 USING index_workspaces AS workspaces
		 WHERE files.user_id = $1
		   AND files.workspace_id = workspaces.workspace_id
		   AND workspaces.user_id = $1
		   AND CASE WHEN BTRIM(workspaces.root_id) = '' THEN 'default' ELSE BTRIM(workspaces.root_id) END = $2`,
		`DELETE FROM index_workspaces
		 WHERE user_id = $1
		   AND CASE WHEN BTRIM(root_id) = '' THEN 'default' ELSE BTRIM(root_id) END = $2`,
	}
	var deletedFiles int64
	for i, statement := range statements {
		result, err := tx.ExecContext(ctx, statement, userID, normalizedRootID)
		if err != nil {
			return 0, err
		}
		if i == 1 { // indexed_files 那条
			deletedFiles, _ = result.RowsAffected()
		}
	}
	return deletedFiles, nil
}

func clearRootIndexState(ctx context.Context, userID, rootID string) error {
	tx, err := beginLockedIndexUserTx(ctx, userID)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := clearRootIndexStateTx(ctx, tx, userID, rootID); err != nil {
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
		RETURNING id::text, user_id, root_id, cloud_revision
	`, indexJobStatusTimedOut, indexJobStatusRunning, time.Now().Add(-indexJobHeartbeatTimeout))
	if err != nil {
		log.Printf("[INDEX] Failed to sweep timed out jobs: %v", err)
		return
	}
	type expiredJob struct {
		id            string
		userID        string
		rootID        string
		cloudRevision int64
	}
	var jobs []expiredJob
	for rows.Next() {
		var job expiredJob
		if err := rows.Scan(&job.id, &job.userID, &job.rootID, &job.cloudRevision); err != nil {
			// 出错必须整体放弃：吞掉 Scan 错误会让对应 job 被标成 timed_out
			// 却漏删 index_job_files，永久泄漏。回滚后下一轮 sweep 重来。
			rows.Close()
			log.Printf("[INDEX] Failed to scan timed out job id: %v", err)
			return
		}
		jobs = append(jobs, job)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		log.Printf("[INDEX] Failed to iterate timed out jobs: %v", err)
		return
	}
	for _, job := range jobs {
		if _, err := tx.ExecContext(ctx, `DELETE FROM index_job_files WHERE job_id = $1`, job.id); err != nil {
			log.Printf("[INDEX] Failed to clean timed out job %s: %v", job.id, err)
			return
		}
	}
	if err := tx.Commit(); err != nil {
		log.Printf("[INDEX] Failed to commit timeout sweep: %v", err)
		return
	}
	for _, job := range jobs {
		if job.cloudRevision > 0 {
			log.Printf("[INDEX] skipping cloud abort for timed out job %s: LCE publish already succeeded (revision=%d)", job.id, job.cloudRevision)
			continue
		}
		if abortErr := abortLCEIndexJob(ctx, job.userID, job.id, job.rootID); abortErr != nil {
			log.Printf("[INDEX] cloud abort cleanup failed for timed out job %s: %v", job.id, abortErr)
		}
	}
	if len(jobs) > 0 {
		log.Printf("[INDEX] Reclaimed %d timed out indexing jobs", len(jobs))
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
			platformModelConfigBarrier.RLock()
			sweepExpiredIndexJobs(ctx)
			platformModelConfigBarrier.RUnlock()
		}
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
