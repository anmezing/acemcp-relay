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
	maxIndexManifestFiles    = 100000
	maxIndexBatchFiles       = 100
	// 单个文件的内容上限。源码文件极少接近这个量级，而 embedding 本身也有
	// token 上限——超过这个大小的"源码"要么是生成物要么是灌进来的负载。
	maxIndexFileBytes = 1 << 20 // 1 MiB
	// 单批内容总量上限。没有这一条时，每批 100 个文件可以各带 100MB，
	// 单个请求就能推送上 GB 的 embedding 负载。
	maxIndexBatchBytes = 8 << 20 // 8 MiB
	// 每用户每日索引字节的默认上限。正常项目首次全量索引通常在百 MB 量级，
	// 之后只传变更；这个默认值对真实使用足够宽松，但能挡住批量灌数据。
	defaultDailyIndexBytes = 2 << 30 // 2 GiB
)

type indexManifestFile struct {
	Path            string `json:"path"`
	Hash            string `json:"hash"`
	Size            int64  `json:"size"`
	EstimatedChunks int    `json:"estimatedChunks"`
	Content         string `json:"content,omitempty"`
}

type createIndexJobRequest struct {
	WorkspaceID     string              `json:"workspaceId"`
	WorkspaceName   string              `json:"workspaceName"`
	Branch          string              `json:"branch"`
	Revision        string              `json:"revision"`
	Files           []indexManifestFile `json:"files"`
	UnreadableFiles []string            `json:"unreadableFiles"`
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

	files, err = graftUnreadablePaths(previous, files, req.UnreadableFiles)
	if err != nil {
		finishJSON(c, http.StatusBadRequest, gin.H{"error": err.Error()})
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

	// COPY 批量写入 manifest：10 万文件逐条 INSERT 要 10 万次网络往返，
	// 会把创建 job 的事务拖到分钟级并一直占着 advisory 锁。
	stmt, err := tx.PrepareContext(c.Request.Context(), pq.CopyIn(
		"index_job_files", "job_id", "path", "hash", "size", "estimated_chunks", "needs_index",
	))
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
	// 无参 Exec 刷出 COPY 缓冲；错误（如约束冲突）大多在这里才暴露。
	if _, err := stmt.ExecContext(c.Request.Context()); err != nil {
		stmt.Close()
		finishIndexError(c, err)
		return
	}
	if err := stmt.Close(); err != nil {
		finishIndexError(c, err)
		return
	}

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
	// 仓库维度：多 root 工作区按文件夹拆分索引任务，每个 job 一个稳定 rootId。
	// 同一 job 的所有批次应携带相同 rootId；缺省 = 单仓默认 root（向后兼容）。
	RootID string `json:"rootId"`
}

const maxIndexRootIDLen = 128

// normalizeIndexRootID 清洗客户端上报的仓库标识：去空白、限长；空则返回 ""，
// 由 LCE 侧落默认 root。
func normalizeIndexRootID(value string) string {
	v := strings.TrimSpace(value)
	if len(v) > maxIndexRootIDLen {
		v = v[:maxIndexRootIDLen]
	}
	return v
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

	// 体积校验放在最前：这些字节最终都会变成 embedding 调用，超限的批次不该
	// 走到建事务、查 manifest 这些更贵的步骤。
	batchBytes, err := validateIndexBatchSize(req.Files)
	if err != nil {
		finishJSON(c, http.StatusRequestEntityTooLarge, gin.H{"error": err.Error()})
		return
	}
	// 按实际内容量扣当日索引配额。计费点必须在调用 LCE 之前：超限的负载不能
	// 先付出 embedding 成本再被拒绝。此后任何失败路径都必须 refund，否则客户
	// 端重试会被双重计费。
	if ok, used, limit := chargeIndexBytes(userID, batchBytes); !ok {
		refundIndexBytes(userID, batchBytes)
		finishJSON(c, http.StatusTooManyRequests, gin.H{
			"error": fmt.Sprintf("daily index quota exceeded (%d/%d bytes)", used, limit),
		})
		return
	}

	modelConfig, err := resolveModelConfigArg(c.Request.Context(), userID)
	if err != nil {
		refundIndexBytes(userID, batchBytes)
		finishJSON(c, http.StatusServiceUnavailable, gin.H{"error": err.Error()})
		return
	}

	// 事务A：只做校验与读取，随后立即提交释放连接和 advisory 锁。
	// 决不能让事务横跨下面那次 LCE 调用（超时 120s）：连接池只有 25 个连接，
	// 几十个并发 embedding 批次就会把连接占光，阻塞全站的 DB 访问。
	tx, err := beginLockedIndexUserTx(c.Request.Context(), userID)
	if err != nil {
		refundIndexBytes(userID, batchBytes)
		finishIndexError(c, err)
		return
	}
	job, staged, deleted, err := prepareIndexBatch(c.Request.Context(), tx, userID, req)
	// 不变量：prepareIndexBatch 只在"job 不存在或不属于该用户"这一种情况下
	// 外传 sql.ErrNoRows；它内部的按文件、按 job 字段查询都会把各自的 ErrNoRows
	// 包装成带上下文的错误。新增查询时请保持这条，否则 404 会指向错误的原因。
	if err == sql.ErrNoRows {
		_ = tx.Rollback()
		refundIndexBytes(userID, batchBytes)
		finishJSON(c, http.StatusNotFound, gin.H{"error": "index job not found"})
		return
	}
	if err != nil {
		_ = tx.Rollback()
		refundIndexBytes(userID, batchBytes)
		finishJSON(c, http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	if err := tx.Commit(); err != nil {
		refundIndexBytes(userID, batchBytes)
		finishIndexError(c, err)
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
	if rootID := normalizeIndexRootID(req.RootID); rootID != "" {
		args["root_id"] = rootID
	}
	if modelConfig != nil {
		args["model_config"] = modelConfig
	}
	result, err := lce.callTool(c.Request.Context(), "codebase_remote_index", args)
	if err != nil {
		refundIndexBytes(userID, batchBytes)
		finishIndexToolError(c, err)
		return
	}
	if result.IsError {
		refundIndexBytes(userID, batchBytes)
		finishIndexToolError(c, fmt.Errorf("%s", string(result.Content)))
		return
	}

	chunks, exact := extractChunkCount(result.Content)
	if !exact {
		for _, file := range staged {
			chunks += int64(file.EstimatedChunks)
		}
	}
	// 事务B：重新加锁提交进度。commitIndexBatch 内部会 SELECT...FOR UPDATE
	// 复查 job 状态、并对每个文件的 UPDATE 断言 rows==1，因此事务A提交后到
	// 这里之间发生的 supersede/清除/重放都会被拒绝（拒绝路径同样 refund）。
	tx2, err := beginLockedIndexUserTx(c.Request.Context(), userID)
	if err != nil {
		refundIndexBytes(userID, batchBytes)
		finishIndexError(c, err)
		return
	}
	defer tx2.Rollback()
	if err := commitIndexBatch(c.Request.Context(), tx2, userID, job.ID, staged, chunks, !exact, len(deleted) > 0); err != nil {
		refundIndexBytes(userID, batchBytes)
		finishIndexError(c, err)
		return
	}
	if err := tx2.Commit(); err != nil {
		refundIndexBytes(userID, batchBytes)
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
	// 每批 100 个文件各来一次网络往返，全部发生在持 advisory 锁的事务里。
	type jobFileRow struct {
		hash            string
		estimatedChunks int
		needsIndex      bool
		indexed         bool
	}
	records := make(map[string]jobFileRow, len(paths))
	if len(paths) > 0 {
		rows, err := tx.QueryContext(ctx, `
			SELECT path, hash, estimated_chunks, needs_index, indexed
			FROM index_job_files
			WHERE job_id = $1 AND path = ANY($2)
		`, req.JobID, pq.Array(paths))
		if err != nil {
			return job, nil, nil, err
		}
		for rows.Next() {
			var p string
			var rec jobFileRow
			if err := rows.Scan(&p, &rec.hash, &rec.estimatedChunks, &rec.needsIndex, &rec.indexed); err != nil {
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
		// 若表现为 sql.ErrNoRows 会被 handleRemoteIndex 判成 404 index job
		// not found，把一个客户端批次错误报成任务丢失，排查时指向错误方向。
		rec, ok := records[input.Path]
		if !ok {
			return job, nil, nil, fmt.Errorf("batch file is not part of this job: %s", input.Path)
		}
		if !rec.needsIndex || rec.indexed || strings.TrimSpace(input.Hash) != rec.hash {
			return job, nil, nil, fmt.Errorf("batch file is stale or already indexed: %s", input.Path)
		}
		input.Hash = rec.hash
		input.EstimatedChunks = rec.estimatedChunks
		staged = append(staged, input)
	}

	var deletionsSent bool
	err = tx.QueryRowContext(ctx, `SELECT deletions_sent FROM index_jobs WHERE id = $1`, req.JobID).Scan(&deletionsSent)
	// job 在本事务开头已确认存在，这里的 ErrNoRows 只可能是并发删除；同样不能
	// 让它伪装成上面那次 job 查询的 404。
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
		req.Error = truncateUTF8(req.Error, 2000)
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
		if err := rows.Scan(&id); err != nil {
			// 出错必须整体放弃：吞掉 Scan 错误会让对应 job 被标成 timed_out
			// 却漏删 index_job_files，永久泄漏。回滚后下一轮 sweep 重来。
			rows.Close()
			log.Printf("[INDEX] Failed to scan timed out job id: %v", err)
			return
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		log.Printf("[INDEX] Failed to iterate timed out jobs: %v", err)
		return
	}
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
