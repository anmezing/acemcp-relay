package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// ── 索引进度可见（_index_status / active_job）───────────────────────────────

type activeIndexJob struct {
	RootID       string
	IndexedFiles int
	TotalFiles   int
	Phase        string
}

// loadActiveIndexJob 取该租户最近一个 running 任务；没有则返回 (nil, nil)。
// userID 参数语义为租户（org_id ?? user_id），见 migrateIndexingTables 说明。
func loadActiveIndexJob(ctx context.Context, userID string) (*activeIndexJob, error) {
	var job activeIndexJob
	err := db.QueryRowContext(ctx, `
		SELECT root_id, indexed_files, total_files, phase
		FROM index_jobs
		WHERE user_id = $1 AND status = 'running'
		ORDER BY started_at DESC LIMIT 1
	`, userID).Scan(&job.RootID, &job.IndexedFiles, &job.TotalFiles, &job.Phase)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &job, nil
}

// indexStatusPayload 是检索响应顶层 _index_status 的契约字段，前端依赖，不可改名。
func (j *activeIndexJob) indexStatusPayload() map[string]interface{} {
	return map[string]interface{}{
		"state":         "building",
		"root_id":       j.RootID,
		"indexed_files": j.IndexedFiles,
		"total_files":   j.TotalFiles,
	}
}

// activeJobPayload 是 tenant-stats 响应里 active_job 的契约字段，不可改名。
func (j *activeIndexJob) activeJobPayload() map[string]interface{} {
	return map[string]interface{}{
		"root_id":       j.RootID,
		"indexed_files": j.IndexedFiles,
		"total_files":   j.TotalFiles,
		"phase":         j.Phase,
	}
}

// injectRetrievalIndexStatus 把 _index_status 注入 JSON 对象顶层。响应不是
// JSON 对象、或没有运行中的索引任务时原样返回。
func injectRetrievalIndexStatus(content []byte, active *activeIndexJob) string {
	if active == nil {
		return string(content)
	}
	var parsed map[string]interface{}
	if json.Unmarshal(content, &parsed) != nil || parsed == nil {
		return string(content)
	}
	parsed["_index_status"] = active.indexStatusPayload()
	reEncoded, err := json.Marshal(parsed)
	if err != nil {
		return string(content)
	}
	return string(reEncoded)
}

// ── GET /mcp/roots ─────────────────────────────────────────────────────────

type indexRootView struct {
	WorkspaceID    string `json:"workspace_id"`
	RootID         string `json:"root_id"`
	Branch         string `json:"branch"`
	Revision       string `json:"revision"`
	CloudRevision  int64  `json:"cloud_revision"`
	IndexedAt      string `json:"indexed_at"`
	FileCount      int64  `json:"file_count"`
	TotalSizeBytes int64  `json:"total_size_bytes"`
	IndexAvailable bool   `json:"index_available"`
	// revision/cloud_revision 始终表示最后一次已发布、可检索的快照；最近同步
	// 正在构建或失败时使用 sync_* 字段，避免把不可用的新 revision 冒充成已发布。
	SyncRevision      string `json:"sync_revision,omitempty"`
	SyncCloudRevision int64  `json:"sync_cloud_revision,omitempty"`
	// index_state/index_phase 是该根最近一次索引任务的状态快照。它来自
	// index_jobs，而不是仅依据 index_workspaces 推断，因此“索引中”和“失败”
	// 即使尚未发布新版本也能被控制台和 Agent 看到。
	IndexState       string `json:"index_state"`
	IndexPhase       string `json:"index_phase,omitempty"`
	IndexedFiles     int    `json:"indexed_files"`
	TotalFiles       int    `json:"total_files"`
	FailedFiles      int    `json:"failed_files"`
	ProgressPercent  int    `json:"progress_percent"`
	IndexError       string `json:"index_error,omitempty"`
	IndexErrorCode   string `json:"index_error_code,omitempty"`
	IndexErrorOrigin string `json:"index_error_origin,omitempty"`
	IndexRecovery    string `json:"index_recovery,omitempty"`
	// base_root_id / view_branch 是纯派生字段（分组展示用），由 root_id 按最后
	// 一个 '@' 拆分得到；无 '@' 时 base=root_id、view_branch="default"。
	// 既有 branch 字段继续携带 start 上报的 Git 分支元数据，语义不变。
	BaseRootID string `json:"base_root_id"`
	ViewBranch string `json:"view_branch"`
}

type latestIndexJobView struct {
	WorkspaceID   string
	RootID        string
	Branch        string
	Revision      string
	Phase         string
	Status        string
	TotalFiles    int
	IndexedFiles  int
	FailedFiles   int
	Error         string
	ErrorCode     string
	ErrorOrigin   string
	Recovery      string
	CloudRevision int64
	StartedAt     time.Time
}

const indexRootStateEmpty = "empty"

// splitRootIDView 从 root_id 派生分组视图：按最后一个 '@' 拆成 (base, branch)。
// 无 '@'、或拆出的任一侧为空（畸形/历史数据）时不猜测，整个 root_id 作为 base，
// 分支归入 "default" 视图。
func splitRootIDView(rootID string) (base string, branch string) {
	if i := strings.LastIndex(rootID, "@"); i > 0 && i < len(rootID)-1 {
		return rootID[:i], rootID[i+1:]
	}
	return rootID, "default"
}

func loadIndexRoots(ctx context.Context, userID string) ([]indexRootView, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT w.workspace_id, w.root_id, w.branch, w.revision, w.cloud_revision, w.indexed_at,
			COALESCE(f.file_count, 0), COALESCE(f.total_size, 0)
		FROM index_workspaces w
		LEFT JOIN (
			SELECT workspace_id, COUNT(*) AS file_count, SUM(size) AS total_size
			FROM indexed_files
			WHERE user_id = $1
			GROUP BY workspace_id
		) f ON f.workspace_id = w.workspace_id
		WHERE w.user_id = $1
		ORDER BY w.indexed_at DESC
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	roots := make([]indexRootView, 0)
	rootByKey := make(map[string]int)
	for rows.Next() {
		var root indexRootView
		var indexedAt time.Time
		if err := rows.Scan(
			&root.WorkspaceID, &root.RootID, &root.Branch, &root.Revision,
			&root.CloudRevision, &indexedAt, &root.FileCount, &root.TotalSizeBytes,
		); err != nil {
			return nil, err
		}
		root.IndexedAt = indexedAt.UTC().Format(time.RFC3339)
		// index_workspaces is written when a job publishes, but an empty
		// manifest can also publish successfully. An empty snapshot has no
		// searchable content and must not be advertised as available.
		root.IndexAvailable = root.FileCount > 0
		root.BaseRootID, root.ViewBranch = splitRootIDView(root.RootID)
		root.IndexState = "ready"
		root.ProgressPercent = 100
		if !root.IndexAvailable {
			root.IndexState = indexRootStateEmpty
			root.ProgressPercent = 0
		}
		root.IndexedFiles = int(root.FileCount)
		root.TotalFiles = int(root.FileCount)
		rootByKey[root.WorkspaceID+"\x00"+root.RootID] = len(roots)
		roots = append(roots, root)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	jobs, err := loadLatestIndexJobs(ctx, userID)
	if err != nil {
		return nil, err
	}
	for _, job := range jobs {
		key := job.WorkspaceID + "\x00" + job.RootID
		index, exists := rootByKey[key]
		if !exists {
			if job.Status != "running" && job.Status != "failed" && job.Status != "timed_out" {
				continue
			}
			base, viewBranch := splitRootIDView(job.RootID)
			root := indexRootView{
				WorkspaceID:       job.WorkspaceID,
				RootID:            job.RootID,
				Branch:            job.Branch,
				SyncRevision:      job.Revision,
				SyncCloudRevision: job.CloudRevision,
				IndexState:        indexJobViewState(job.Status),
				IndexPhase:        job.Phase,
				IndexedFiles:      job.IndexedFiles,
				TotalFiles:        job.TotalFiles,
				FailedFiles:       job.FailedFiles,
				ProgressPercent:   indexProgressPercent(job.IndexedFiles, job.TotalFiles, job.Status),
				IndexError:        job.Error,
				BaseRootID:        base,
				ViewBranch:        viewBranch,
			}
			applyIndexFailureDiagnostic(&root, job.Status, job.Error, job.ErrorCode, job.ErrorOrigin, job.Recovery)
			roots = append(roots, root)
			continue
		}
		applyIndexJobToRoot(&roots[index], job)
	}
	return roots, nil
}

func loadLatestIndexJobs(ctx context.Context, userID string) ([]latestIndexJobView, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT workspace_id, root_id, COALESCE(branch, ''), COALESCE(revision, ''),
			COALESCE(phase, ''), status, COALESCE(total_files, 0),
			COALESCE(indexed_files, 0), COALESCE(failed_files, 0), COALESCE(error, ''),
			COALESCE(error_code, ''), COALESCE(error_origin, ''), COALESCE(recovery, ''),
			COALESCE(cloud_revision, 0), started_at
		FROM (
			SELECT DISTINCT ON (workspace_id, root_id)
				workspace_id, root_id, branch, revision, phase, status, total_files,
				indexed_files, failed_files, error, error_code, error_origin, recovery, cloud_revision, started_at
			FROM index_jobs
			WHERE user_id = $1
			ORDER BY workspace_id, root_id, started_at DESC, id DESC
		) latest
		ORDER BY started_at DESC
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := make([]latestIndexJobView, 0)
	for rows.Next() {
		var job latestIndexJobView
		if err := rows.Scan(
			&job.WorkspaceID, &job.RootID, &job.Branch, &job.Revision, &job.Phase,
			&job.Status, &job.TotalFiles, &job.IndexedFiles, &job.FailedFiles,
			&job.Error, &job.ErrorCode, &job.ErrorOrigin, &job.Recovery,
			&job.CloudRevision, &job.StartedAt,
		); err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func indexJobViewState(status string) string {
	switch status {
	case "running":
		return "building"
	case "completed":
		return "ready"
	case "failed", "timed_out":
		return "failed"
	case "superseded":
		return "superseded"
	default:
		return status
	}
}

func indexProgressPercent(indexed, total int, status string) int {
	if status == "completed" {
		return 100
	}
	if total <= 0 {
		return 0
	}
	percent := indexed * 100 / total
	if percent < 0 {
		return 0
	}
	if percent > 100 {
		return 100
	}
	return percent
}

func applyIndexFailureDiagnostic(root *indexRootView, status, detail, code, origin, recovery string) {
	if status != indexJobStatusFailed && status != indexJobStatusTimedOut {
		return
	}
	diagnostic, ok := persistedIndexFailureDiagnostic(code, origin, recovery)
	if !ok {
		diagnostic = classifyIndexFailure(status, detail)
	}
	root.IndexErrorCode = diagnostic.Code
	root.IndexErrorOrigin = diagnostic.Origin
	root.IndexRecovery = diagnostic.Recovery
}

func applyIndexJobToRoot(root *indexRootView, job latestIndexJobView) {
	root.IndexState = indexJobViewState(job.Status)
	root.IndexPhase = job.Phase
	root.IndexedFiles = job.IndexedFiles
	root.TotalFiles = job.TotalFiles
	root.FailedFiles = job.FailedFiles
	root.ProgressPercent = indexProgressPercent(job.IndexedFiles, job.TotalFiles, job.Status)
	if job.Status == indexJobStatusCompleted && !root.IndexAvailable {
		root.IndexState = indexRootStateEmpty
		root.ProgressPercent = 0
	}
	root.IndexError = job.Error
	applyIndexFailureDiagnostic(root, job.Status, job.Error, job.ErrorCode, job.ErrorOrigin, job.Recovery)
	root.SyncRevision = job.Revision
	root.SyncCloudRevision = job.CloudRevision
	if job.Branch != "" {
		root.Branch = job.Branch
	}
}

func handleListRoots(c *gin.Context) {
	userID := c.GetString(ContextKeyUserID)
	if userID == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusUnauthorized))
		return
	}
	// 列表按租户：组织成员看到的是组织共享的索引根。
	roots, err := loadIndexRoots(c.Request.Context(), requestTenantID(c))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "获取索引根列表失败: " + err.Error()})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusInternalServerError))
		return
	}
	c.JSON(http.StatusOK, gin.H{"roots": roots})
	completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))
}

// ── POST /mcp/delete-root ──────────────────────────────────────────────────

const (
	deleteRootMinInterval  = time.Minute
	deleteRootSeenMaxEntry = 4096
)

var (
	deleteRootSeenMu sync.Mutex
	deleteRootSeen   = make(map[string]time.Time)
)

// checkDeleteRootRateLimit 与 checkIndexStartRateLimit 同模式，key 为
// (tenant, root)：每租户每 root 1 次/分钟，组织成员共享同一窗口。
// 返回 0 表示放行并记录；正数表示还需等待的秒数（向上取整）。
func checkDeleteRootRateLimit(tenantID, rootID string, now time.Time) int {
	key := tenantID + "\x00" + lceIndexRootID(rootID)
	deleteRootSeenMu.Lock()
	defer deleteRootSeenMu.Unlock()
	if last, ok := deleteRootSeen[key]; ok {
		if wait := deleteRootMinInterval - now.Sub(last); wait > 0 {
			return int((wait + time.Second - 1) / time.Second)
		}
	}
	if len(deleteRootSeen) >= deleteRootSeenMaxEntry {
		for k, t := range deleteRootSeen {
			if now.Sub(t) >= deleteRootMinInterval {
				delete(deleteRootSeen, k)
			}
		}
	}
	deleteRootSeen[key] = now
	return 0
}

// rejectNonOwnerIndexManagement 对组织密钥的索引管理操作做角色门禁：
// 只有 org_role='owner' 可以修改组织共享索引状态；member（或角色缺失/脏值，
// fail-closed）一律 403。个人密钥（org_id 空）不受影响。
// 返回 true 表示已写出 403 响应，调用方直接 return。
func rejectNonOwnerIndexManagement(c *gin.Context, event, message string) bool {
	orgID := c.GetString(ContextKeyOrgID)
	if orgID == "" || c.GetString(ContextKeyOrgRole) == orgRoleOwner {
		return false
	}
	logEvent(event,
		"user_id", c.GetString(ContextKeyUserID),
		"tenant", orgID,
		"role", c.GetString(ContextKeyOrgRole),
		"path", c.Request.URL.Path,
	)
	c.JSON(http.StatusForbidden, gin.H{"error": message})
	completeRequestLogAsync(getRequestLogEntry(c, http.StatusForbidden))
	return true
}

func rejectNonOwnerIndexDeletion(c *gin.Context) bool {
	return rejectNonOwnerIndexManagement(c, "org_delete_forbidden", "仅组织所有者可删除索引")
}

func rejectNonOwnerFailureDismissal(c *gin.Context) bool {
	return rejectNonOwnerIndexManagement(c, "org_dismiss_failure_forbidden", "仅组织所有者可清理索引失败记录")
}

// hasRunningJobForRoot 判断该用户是否有对应此 root 的 running 任务。
// root 匹配沿用 clearRootIndexStateTx 的规则：空/legacy root 映射到 "default"。
func hasRunningJobForRoot(ctx context.Context, userID, rootID string) (bool, error) {
	var running bool
	err := db.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM index_jobs
			WHERE user_id = $1 AND status = 'running'
			  AND CASE WHEN BTRIM(root_id) = '' THEN 'default' ELSE BTRIM(root_id) END = $2
		)
	`, userID, lceIndexRootID(rootID)).Scan(&running)
	return running, err
}

// lceClearIndexRoot 是可注入的 LCE 调用缝，测试里替换以避免真实网络调用。
var lceClearIndexRoot = func(ctx context.Context, userID, rootID string) (*mcpToolResult, error) {
	return lce.callToolWithTimeout(ctx, "codebase_clear_index", map[string]interface{}{
		"tenant_id": userID,
		"root_id":   lceIndexRootID(rootID),
	}, remoteIndexMCPCallTimeout)
}

type deleteRootOperationLease interface {
	Context() context.Context
	Release()
}

var acquireDeleteRootOperation = func(ctx context.Context, tenantID string) (deleteRootOperationLease, error) {
	return acquireExclusiveIndexOperation(ctx, tenantID, "delete-root")
}

var acquireDismissRootFailureOperation = func(ctx context.Context, tenantID string) (deleteRootOperationLease, error) {
	return acquireExclusiveIndexOperation(ctx, tenantID, "dismiss-root-failure")
}

// extractLCEDeletedFiles 尽力从 LCE clear 响应里取删除文件数。LCE 代码不在本仓库，
// 响应结构无法静态断定，因此只认几个常见字段名；取不到时调用方回退到 relay 侧
// indexed_files 的删除行数（对 UI 而言两者语义一致：该 root 下已索引文件数）。
func extractLCEDeletedFiles(content []byte) (int64, bool) {
	var value interface{}
	if json.Unmarshal(content, &value) != nil {
		return 0, false
	}
	return findDeletedFileCount(value)
}

func findDeletedFileCount(value interface{}) (int64, bool) {
	switch typed := value.(type) {
	case map[string]interface{}:
		for _, key := range []string{"deleted_files", "deletedFiles", "deleted_count", "deletedCount"} {
			if count, ok := numberAsInt64(typed[key]); ok {
				return count, true
			}
		}
		for _, child := range typed {
			if count, ok := findDeletedFileCount(child); ok {
				return count, true
			}
		}
	case []interface{}:
		for _, child := range typed {
			if count, ok := findDeletedFileCount(child); ok {
				return count, true
			}
		}
	}
	return 0, false
}

// deleteRootIndexState 在一个事务里删除该 root 对应 workspace 的 relay 侧行，
// 返回 indexed_files 的删除行数。
func deleteRootIndexState(ctx context.Context, userID, rootID string) (int64, error) {
	tx, err := beginLockedIndexUserTx(ctx, userID)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	deletedFiles, err := clearRootIndexStateTx(ctx, tx, userID, rootID)
	if err != nil {
		return 0, err
	}
	return deletedFiles, tx.Commit()
}

// dismissRootFailureState 清理该 root 的失败/超时任务。如果该 root 没有任何已发布的
// 索引数据（首次索引失败），则完全清理 workspace 和相关状态，让用户重启 IDE 后能够
// 重新开始干净的索引；如果有已发布的快照（更新失败但旧索引可用），则只删除失败任务，
// 保留 workspace、indexed_files 与 LCE 云端快照，不会误删仍可检索的数据。
func dismissRootFailureState(ctx context.Context, userID, rootID string) (int64, error) {
	tx, err := beginLockedIndexUserTx(ctx, userID)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	normalizedRootID := lceIndexRootID(rootID)

	// 检查该 root 是否有已发布的索引数据
	var hasPublishedIndex bool
	err = tx.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM indexed_files AS files
			JOIN index_workspaces AS workspaces
			  ON files.user_id = workspaces.user_id
			  AND files.workspace_id = workspaces.workspace_id
			WHERE workspaces.user_id = $1
			  AND CASE WHEN BTRIM(workspaces.root_id) = '' THEN 'default' ELSE BTRIM(workspaces.root_id) END = $2
		)
	`, userID, normalizedRootID).Scan(&hasPublishedIndex)
	if err != nil {
		return 0, err
	}

	// 删除失败/超时的任务
	result, err := tx.ExecContext(ctx, `
		DELETE FROM index_jobs
		WHERE user_id = $1
		  AND CASE WHEN BTRIM(root_id) = '' THEN 'default' ELSE BTRIM(root_id) END = $2
		  AND status IN ($3, $4)
	`, userID, normalizedRootID, indexJobStatusFailed, indexJobStatusTimedOut)
	if err != nil {
		return 0, err
	}
	dismissed, _ := result.RowsAffected()

	// 如果没有已发布的索引（首次索引失败），完全清理 workspace，让用户能重新开始
	if !hasPublishedIndex {
		_, err = tx.ExecContext(ctx, `
			DELETE FROM index_workspaces
			WHERE user_id = $1
			  AND CASE WHEN BTRIM(root_id) = '' THEN 'default' ELSE BTRIM(root_id) END = $2
		`, userID, normalizedRootID)
		if err != nil {
			return 0, err
		}
	}

	return dismissed, tx.Commit()
}

func handleDismissRootFailure(c *gin.Context) {
	userID := c.GetString(ContextKeyUserID)
	if userID == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusUnauthorized))
		return
	}
	tenantID := requestTenantID(c)

	var req struct {
		RootID string `json:"root_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.RootID) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "root_id is required"})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusBadRequest))
		return
	}
	rootID := normalizeIndexRootID(req.RootID)

	if rejectNonOwnerFailureDismissal(c) {
		return
	}

	lease, err := acquireDismissRootFailureOperation(c.Request.Context(), tenantID)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "索引正在执行其他操作: " + err.Error()})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusServiceUnavailable))
		return
	}
	defer lease.Release()
	opCtx := lease.Context()

	running, err := hasRunningJobForRoot(opCtx, tenantID, rootID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "检查运行中任务失败: " + err.Error()})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusInternalServerError))
		return
	}
	if running {
		c.JSON(http.StatusConflict, gin.H{"error": "该 root 有正在运行的索引任务，请等待其结束后再清理失败记录"})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusConflict))
		return
	}

	dismissed, err := dismissRootFailureState(opCtx, tenantID, rootID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "清理索引失败记录失败: " + err.Error()})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusInternalServerError))
		return
	}
	c.JSON(http.StatusOK, gin.H{"dismissed": true, "dismissed_jobs": dismissed})
	completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))
	log.Printf("[DISMISS_ROOT_FAILURE] user=%s tenant=%s root=%s dismissed_jobs=%d", userID, tenantID, rootID, dismissed)
}

func handleDeleteRoot(c *gin.Context) {
	userID := c.GetString(ContextKeyUserID)
	if userID == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusUnauthorized))
		return
	}
	// 数据归属按租户：组织密钥删的是组织共享 root，个人密钥行为不变。
	tenantID := requestTenantID(c)

	var req struct {
		RootID string `json:"root_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.RootID) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "root_id is required"})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusBadRequest))
		return
	}
	rootID := normalizeIndexRootID(req.RootID)

	if rejectNonOwnerIndexDeletion(c) {
		return
	}

	lease, err := acquireDeleteRootOperation(c.Request.Context(), tenantID)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "索引正在执行其他操作: " + err.Error()})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusServiceUnavailable))
		return
	}
	defer lease.Release()
	opCtx := lease.Context()

	running, err := hasRunningJobForRoot(opCtx, tenantID, rootID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "检查运行中任务失败: " + err.Error()})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusInternalServerError))
		return
	}
	if running {
		c.JSON(http.StatusConflict, gin.H{"error": "该 root 有正在运行的索引任务，请等待其结束后再删除"})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusConflict))
		return
	}

	// 频率限制放在 409 检查之后：被 409 拒绝的请求不消耗时间窗，任务结束后可
	// 立即重试；通过检查后才计一次，保护 LCE 与删除事务不被连续触发。
	if wait := checkDeleteRootRateLimit(tenantID, rootID, time.Now()); wait > 0 {
		c.JSON(http.StatusTooManyRequests, gin.H{
			"error":               "删除操作过于频繁，请稍后重试",
			"retry_after_seconds": wait,
		})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusTooManyRequests))
		return
	}

	// 顺序必须是"先删云端再删本地"：LCE 失败时本地记录还在，UI 仍显示该 root，
	// 用户可重试，方向一致；反过来会出现"云端已删而本地记录仍在"之外更糟的
	// "本地消失但云端数据还在"的静默残留。
	result, err := lceClearIndexRoot(opCtx, tenantID, rootID)
	if err != nil || result == nil || result.IsError {
		detail := "empty LCE response"
		if err != nil {
			detail = err.Error()
		} else if result != nil {
			detail = string(result.Content)
		}
		c.JSON(http.StatusBadGateway, gin.H{"error": "清除云端索引失败: " + detail})
		completeRequestLogWithErrorAsync(getRequestLogEntry(c, http.StatusBadGateway), "lce", detail)
		return
	}

	relayDeleted, err := deleteRootIndexState(opCtx, tenantID, rootID)
	if err != nil {
		// 云端已清、relay 行未删：该 root 仍显示在列表里，用户重试即可自愈
		//（下一次 LCE clear 幂等，再删 relay 行）。
		c.JSON(http.StatusInternalServerError, gin.H{"error": "清除 Relay 索引状态失败: " + err.Error()})
		completeRequestLogWithErrorAsync(getRequestLogEntry(c, http.StatusInternalServerError), "relay", err.Error())
		return
	}

	deletedFiles := relayDeleted
	if lceCount, ok := extractLCEDeletedFiles(result.Content); ok && lceCount > 0 {
		deletedFiles = lceCount
	}
	c.JSON(http.StatusOK, gin.H{"deleted": true, "deleted_files": deletedFiles})
	completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))
	log.Printf("[DELETE_ROOT] user=%s tenant=%s root=%s relay_files=%d", userID, tenantID, rootID, relayDeleted)
}
