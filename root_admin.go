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

// injectRetrievalExtras 在一次 unmarshal/marshal 里把 _client_update_available
// 与 _index_status 注入 JSON 对象顶层。响应不是 JSON 对象、或没有要注入的
// 字段时原样返回。
func injectRetrievalExtras(content []byte, updateAvailable bool, active *activeIndexJob) string {
	if !updateAvailable && active == nil {
		return string(content)
	}
	var parsed map[string]interface{}
	if json.Unmarshal(content, &parsed) != nil || parsed == nil {
		return string(content)
	}
	if updateAvailable {
		parsed["_client_update_available"] = true
	}
	if active != nil {
		parsed["_index_status"] = active.indexStatusPayload()
	}
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
	// base_root_id / view_branch 是纯派生字段（分组展示用），由 root_id 按最后
	// 一个 '@' 拆分得到；无 '@' 时 base=root_id、view_branch="default"。
	// 既有 branch 字段继续携带 start 上报的 Git 分支元数据，语义不变。
	BaseRootID string `json:"base_root_id"`
	ViewBranch string `json:"view_branch"`
}

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
		root.BaseRootID, root.ViewBranch = splitRootIDView(root.RootID)
		roots = append(roots, root)
	}
	return roots, rows.Err()
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

// rejectNonOwnerIndexDeletion 对组织密钥的删除类操作做角色门禁：
// 只有 org_role='owner' 可以删除组织共享索引；member（或角色缺失/脏值，
// fail-closed）一律 403。个人密钥（org_id 空）不受影响。
// 返回 true 表示已写出 403 响应，调用方直接 return。
func rejectNonOwnerIndexDeletion(c *gin.Context) bool {
	orgID := c.GetString(ContextKeyOrgID)
	if orgID == "" || c.GetString(ContextKeyOrgRole) == orgRoleOwner {
		return false
	}
	logEvent("org_delete_forbidden",
		"user_id", c.GetString(ContextKeyUserID),
		"tenant", orgID,
		"role", c.GetString(ContextKeyOrgRole),
		"path", c.Request.URL.Path,
	)
	c.JSON(http.StatusForbidden, gin.H{"error": "仅组织所有者可删除索引"})
	completeRequestLogAsync(getRequestLogEntry(c, http.StatusForbidden))
	return true
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
		logIDStr, _ := c.Get(ContextKeyLogID)
		logIDVal, _ := logIDStr.(string)
		saveErrorDetailsAsync(logIDVal, "lce", detail, getInsertDone(c))
		c.JSON(http.StatusBadGateway, gin.H{"error": "清除云端索引失败: " + detail})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusBadGateway))
		return
	}

	relayDeleted, err := deleteRootIndexState(opCtx, tenantID, rootID)
	if err != nil {
		// 云端已清、relay 行未删：该 root 仍显示在列表里，用户重试即可自愈
		//（下一次 LCE clear 幂等，再删 relay 行）。
		c.JSON(http.StatusInternalServerError, gin.H{"error": "清除 Relay 索引状态失败: " + err.Error()})
		completeRequestLogAsync(getRequestLogEntry(c, http.StatusInternalServerError))
		return
	}

	deletedFiles := relayDeleted
	if lceCount, ok := extractLCEDeletedFiles(result.Content); ok {
		deletedFiles = lceCount
	}
	c.JSON(http.StatusOK, gin.H{"deleted": true, "deleted_files": deletedFiles})
	completeRequestLogAsync(getRequestLogEntry(c, http.StatusOK))
	log.Printf("[DELETE_ROOT] user=%s tenant=%s root=%s relay_files=%d", userID, tenantID, rootID, relayDeleted)
}
