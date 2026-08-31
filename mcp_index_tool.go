package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	codebaseIndexToolName = "codebase_index"
	// 客户端与 Relay 共享该估算单位；文件上限可配置，因此最大分块数运行时计算。
	estimatedIndexChunkBytes = 4096

	// indexEnvelopeSchemaVersion 是跨仓库契约值（docs/contracts/cloud-protocol.json
	// 的 responseEnvelope.schemaVersion），由 contract_pin_test.go 钉住。
	indexEnvelopeSchemaVersion = "1.0"
)

var (
	sha256Pattern      = regexp.MustCompile(`^[a-fA-F0-9]{64}$`)
	windowsDrivePrefix = regexp.MustCompile(`^[A-Za-z]:`)
	// '@' 允许出现在非首位：客户端把分支视图编码进 root_id（<root>@<branch>），
	// /mcp/roots 按最后一个 '@' 拆出 base_root_id 与视图分支。分支名里不在
	// 本字符集内的字符（如 '/'）必须由客户端在编码时清洗。
	indexRootIDPattern     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}$`)
	indexRetryAfterPattern = regexp.MustCompile(`(?i)retry after\s+(\d+)\s+seconds?`)

	indexStartSeenMu sync.Mutex
	indexStartSeen   = make(map[string]time.Time)
)

// checkIndexStartRateLimit 对同一 (tenant, root) 的 start 施加最小间隔
// （userID 参数即租户：org_id ?? user_id，组织成员共享窗口）。
// 返回 0 表示放行并记录本次调用；返回正数表示还需等待的秒数（向上取整）。
func checkIndexStartRateLimit(userID, rootID string, now time.Time) int {
	key := userID + "\x00" + rootID
	indexStartSeenMu.Lock()
	defer indexStartSeenMu.Unlock()
	if last, ok := indexStartSeen[key]; ok {
		if wait := indexStartMinInterval - now.Sub(last); wait > 0 {
			return int((wait + time.Second - 1) / time.Second)
		}
	}
	if len(indexStartSeen) >= indexStartSeenMaxEntries {
		for k, t := range indexStartSeen {
			if now.Sub(t) >= indexStartMinInterval {
				delete(indexStartSeen, k)
			}
		}
	}
	indexStartSeen[key] = now
	return 0
}

// checkIndexClientVersion 用最低安全版本门禁 start args 上报的客户端版本。
// 一旦管理员配置最低版本，缺少 client_version 的旧客户端也必须拒绝；否则旧版
// 可以绕过版本门禁并继续触发已修复的重复索引/供应商消耗问题。
func checkIndexClientVersion(clientVersion string) error {
	clientVersion = strings.TrimSpace(clientVersion)
	minimumVersion := currentMinClientVersion()
	if minimumVersion == "" {
		return nil
	}
	if clientVersion == "" {
		return fmt.Errorf("client_version is required; minimum supported version is %s; update @anmezing/lce-cloud and restart the MCP client", minimumVersion)
	}
	if compareVersions(clientVersion, minimumVersion) < 0 {
		return fmt.Errorf("client version %s is below minimum %s; update @anmezing/lce-cloud and restart the MCP client", clientVersion, minimumVersion)
	}
	return nil
}

type mcpIndexManifestFile struct {
	Path            string `json:"path"`
	Hash            string `json:"hash"`
	Size            int64  `json:"size"`
	EstimatedChunks int    `json:"estimated_chunks"`
}

type mcpIndexUploadFile struct {
	Path    string `json:"path"`
	Hash    string `json:"hash"`
	Content string `json:"content"`
}

type mcpIndexStartArgs struct {
	Operation       string                 `json:"operation"`
	RootID          string                 `json:"root_id"`
	WorkspaceName   string                 `json:"workspace_name,omitempty"`
	Branch          string                 `json:"branch,omitempty"`
	Revision        string                 `json:"revision,omitempty"`
	ClientVersion   string                 `json:"client_version,omitempty"`
	Files           []mcpIndexManifestFile `json:"files"`
	UnreadableFiles []string               `json:"unreadable_files,omitempty"`
}

type mcpIndexUploadArgs struct {
	Operation string               `json:"operation"`
	JobID     string               `json:"job_id"`
	RootID    string               `json:"root_id"`
	Files     []mcpIndexUploadFile `json:"files"`
}

type mcpIndexJobArgs struct {
	Operation string `json:"operation"`
	JobID     string `json:"job_id"`
}

type mcpIndexFailArgs struct {
	Operation string `json:"operation"`
	JobID     string `json:"job_id"`
	Error     string `json:"error,omitempty"`
}

// codebaseIndexEnvelope 构造 codebase_index 的响应信封。字段名与
// docs/contracts/cloud-protocol.json 的 responseEnvelope 契约一致。失败除 message
// 外返回稳定诊断字段，让客户端无需解析供应商文案即可决定重试/修复动作。
func codebaseIndexEnvelope(ok bool, payload interface{}, errMessage string) map[string]interface{} {
	envelope := map[string]interface{}{
		"schemaVersion":  indexEnvelopeSchemaVersion,
		"toolName":       codebaseIndexToolName,
		"ok":             ok,
		"tool":           codebaseIndexToolName,
		"responseFormat": "json",
	}
	if ok {
		envelope["payload"] = payload
	} else {
		diagnostic := classifyIndexFailure(indexJobStatusFailed, errMessage)
		errorPayload := map[string]interface{}{
			"message":  errMessage,
			"code":     diagnostic.Code,
			"origin":   diagnostic.Origin,
			"recovery": diagnostic.Recovery,
		}
		if retryAfterSeconds, ok := indexRetryAfterSeconds(errMessage); ok {
			errorPayload["retry_after_seconds"] = retryAfterSeconds
		}
		envelope["error"] = errorPayload
	}
	return envelope
}

func indexRetryAfterSeconds(detail string) (int, bool) {
	match := indexRetryAfterPattern.FindStringSubmatch(detail)
	if len(match) != 2 {
		return 0, false
	}
	seconds, err := strconv.Atoi(match[1])
	if err != nil || seconds <= 0 {
		return 0, false
	}
	return seconds, true
}

func codebaseIndexLimitMetadata() map[string]interface{} {
	return map[string]interface{}{
		"maxFileBytes":        maxIndexFileBytes,
		"maxBatchFiles":       maxIndexBatchFiles,
		"maxBatchBytes":       maxIndexBatchBytes,
		"estimatedChunkBytes": estimatedIndexChunkBytes,
		"maxEstimatedChunks":  maxIndexEstimatedChunks(),
		"maxManifestFiles":    maxIndexManifestFiles,
		"maxPathBytes":        maxIndexPathBytes,
	}
}

func codebaseIndexToolDefinition() (json.RawMessage, error) {
	manifestFile := map[string]interface{}{
		"type":                 "object",
		"additionalProperties": false,
		"required":             []string{"path", "hash", "size", "estimated_chunks"},
		"properties": map[string]interface{}{
			"path":             map[string]interface{}{"type": "string", "minLength": 1, "maxLength": maxIndexPathBytes},
			"hash":             map[string]interface{}{"type": "string", "pattern": "^[a-fA-F0-9]{64}$"},
			"size":             map[string]interface{}{"type": "integer", "minimum": 1, "maximum": maxIndexFileBytes},
			"estimated_chunks": map[string]interface{}{"type": "integer", "minimum": 1, "maximum": maxIndexEstimatedChunks()},
		},
	}
	uploadFile := map[string]interface{}{
		"type":                 "object",
		"additionalProperties": false,
		"required":             []string{"path", "hash", "content"},
		"properties": map[string]interface{}{
			"path":    map[string]interface{}{"type": "string", "minLength": 1, "maxLength": maxIndexPathBytes},
			"hash":    map[string]interface{}{"type": "string", "pattern": "^[a-fA-F0-9]{64}$"},
			"content": map[string]interface{}{"type": "string", "maxLength": maxIndexFileBytes},
		},
	}
	operation := func(name string) map[string]interface{} {
		return map[string]interface{}{"type": "string", "const": name}
	}
	jobOperation := func(name string) map[string]interface{} {
		return map[string]interface{}{
			"type":                 "object",
			"additionalProperties": false,
			"required":             []string{"operation", "job_id"},
			"properties": map[string]interface{}{
				"operation": operation(name),
				"job_id":    map[string]interface{}{"type": "string", "minLength": 1},
			},
		}
	}
	definition := map[string]interface{}{
		"name": codebaseIndexToolName,
		"_meta": map[string]interface{}{
			"com.anmezing.lce/index-limits": codebaseIndexLimitMetadata(),
		},
		"description": "Synchronize the current local workspace into the authenticated tenant index without requiring a host-specific plugin. " +
			"The Agent must read the workspace with its native file tools, exclude secrets/binaries/generated dependencies, and call operations in order: " +
			"start with the complete UTF-8 file manifest and stable root_id; upload only pending_files in bounded batches; " +
			"call upload once with an empty files array when start reports only deletions; complete after every pending file is accepted; " +
			"call fail if the workflow cannot finish. status renews both Relay and cloud staging leases. The server injects tenant identity and enforces " +
			"SHA-256 content matching, manifest and batch limits, byte quota, model fingerprint, root isolation, deletion handling, and graph finalization.",
		"inputSchema": map[string]interface{}{
			"type": "object",
			"oneOf": []interface{}{
				map[string]interface{}{
					"type":                 "object",
					"additionalProperties": false,
					"required":             []string{"operation", "root_id", "files"},
					"properties": map[string]interface{}{
						"operation":        operation("start"),
						"root_id":          map[string]interface{}{"type": "string", "minLength": 1, "maxLength": maxIndexRootIDLen},
						"workspace_name":   map[string]interface{}{"type": "string", "maxLength": 256},
						"branch":           map[string]interface{}{"type": "string", "maxLength": 512},
						"revision":         map[string]interface{}{"type": "string", "maxLength": 512},
						"client_version":   map[string]interface{}{"type": "string", "maxLength": 64},
						"files":            map[string]interface{}{"type": "array", "maxItems": maxIndexManifestFiles, "items": manifestFile},
						"unreadable_files": map[string]interface{}{"type": "array", "maxItems": maxIndexManifestFiles, "items": map[string]interface{}{"type": "string", "minLength": 1, "maxLength": maxIndexPathBytes}},
					},
				},
				map[string]interface{}{
					"type":                 "object",
					"additionalProperties": false,
					"required":             []string{"operation", "job_id", "root_id", "files"},
					"properties": map[string]interface{}{
						"operation": operation("upload"),
						"job_id":    map[string]interface{}{"type": "string", "minLength": 1},
						"root_id":   map[string]interface{}{"type": "string", "minLength": 1, "maxLength": maxIndexRootIDLen},
						"files":     map[string]interface{}{"type": "array", "maxItems": maxIndexBatchFiles, "items": uploadFile},
					},
				},
				jobOperation("status"),
				jobOperation("complete"),
				map[string]interface{}{
					"type":                 "object",
					"additionalProperties": false,
					// error 为契约必填（cloud-protocol.json requiredFields.fail）：
					// 客户端放弃流程时必须说明原因，便于排障与配额审计。
					"required": []string{"operation", "job_id", "error"},
					"properties": map[string]interface{}{
						"operation": operation("fail"),
						"job_id":    map[string]interface{}{"type": "string", "minLength": 1},
						"error":     map[string]interface{}{"type": "string", "maxLength": maxIndexFailureBytes},
					},
				},
			},
		},
	}
	encoded, err := json.Marshal(definition)
	return json.RawMessage(encoded), err
}

func appendCodebaseIndexTool(raw json.RawMessage) (json.RawMessage, error) {
	var tools []json.RawMessage
	if err := json.Unmarshal(raw, &tools); err != nil {
		return nil, fmt.Errorf("parse filtered MCP tools: %w", err)
	}
	for _, tool := range tools {
		var metadata struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(tool, &metadata); err != nil {
			return nil, fmt.Errorf("parse filtered MCP tool metadata: %w", err)
		}
		if strings.TrimSpace(metadata.Name) == codebaseIndexToolName {
			return nil, fmt.Errorf("duplicate public MCP tool %q", codebaseIndexToolName)
		}
	}
	definition, err := codebaseIndexToolDefinition()
	if err != nil {
		return nil, fmt.Errorf("encode %s definition: %w", codebaseIndexToolName, err)
	}
	tools = append(tools, definition)
	return json.Marshal(tools)
}

func decodeStrictIndexArgs(raw map[string]interface{}, target interface{}) error {
	encoded, err := json.Marshal(raw)
	if err != nil {
		return fmt.Errorf("encode index arguments: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("invalid codebase_index arguments: %w", err)
	}
	return nil
}

func requireIndexArgument(raw map[string]interface{}, name string) error {
	if value, ok := raw[name]; !ok || value == nil {
		return fmt.Errorf("missing required argument: %s", name)
	}
	return nil
}

func validateIndexSourcePath(value string) (string, error) {
	raw := strings.TrimSpace(strings.ReplaceAll(value, `\`, "/"))
	if len(raw) > maxIndexPathBytes {
		return "", fmt.Errorf("index path exceeds %d bytes", maxIndexPathBytes)
	}
	if raw == "" || strings.HasPrefix(raw, "/") || windowsDrivePrefix.MatchString(raw) {
		return "", fmt.Errorf("index path must be repository-relative: %s", value)
	}
	for _, segment := range strings.Split(raw, "/") {
		if segment == ".." {
			return "", fmt.Errorf("index path must not escape the repository: %s", value)
		}
	}
	normalized := normalizeIndexPath(raw)
	if normalized == "" {
		return "", fmt.Errorf("index path is required")
	}
	lower := strings.ToLower(normalized)
	segments := strings.Split(lower, "/")
	for _, segment := range segments {
		switch segment {
		case ".git", "node_modules", ".turbo", "dist", "build", ".next", "__pycache__":
			return "", fmt.Errorf("index path is excluded: %s", normalized)
		}
	}
	base := path.Base(lower)
	if base == ".env" || strings.HasPrefix(base, ".env.") ||
		base == "package-lock.json" || base == "yarn.lock" || base == "pnpm-lock.yaml" ||
		strings.HasSuffix(base, ".min.js") || strings.HasSuffix(base, ".min.css") {
		return "", fmt.Errorf("index path is excluded: %s", normalized)
	}
	switch path.Ext(base) {
	case ".pyc", ".class", ".o", ".so", ".dll", ".exe", ".wasm", ".map", ".pem", ".key", ".cert",
		".png", ".jpg", ".jpeg", ".gif", ".ico", ".svg", ".woff", ".woff2", ".ttf", ".eot", ".mp3",
		".mp4", ".zip", ".tar", ".gz", ".rar", ".pdf":
		return "", fmt.Errorf("index path is excluded: %s", normalized)
	}
	return normalized, nil
}

func validateMCPManifestFiles(files []mcpIndexManifestFile) ([]indexManifestFile, error) {
	result := make([]indexManifestFile, 0, len(files))
	for _, file := range files {
		filePath, err := validateIndexSourcePath(file.Path)
		if err != nil {
			return nil, err
		}
		if !sha256Pattern.MatchString(strings.TrimSpace(file.Hash)) {
			return nil, fmt.Errorf("manifest file hash must be SHA-256: %s", filePath)
		}
		if file.Size < 1 || file.Size > int64(maxIndexFileBytes) {
			return nil, fmt.Errorf("manifest file size is invalid: %s", filePath)
		}
		if file.EstimatedChunks < 1 || file.EstimatedChunks > maxIndexEstimatedChunks() {
			return nil, fmt.Errorf("estimated_chunks must be between 1 and %d: %s", maxIndexEstimatedChunks(), filePath)
		}
		result = append(result, indexManifestFile{
			Path:            filePath,
			Hash:            strings.ToLower(strings.TrimSpace(file.Hash)),
			Size:            file.Size,
			EstimatedChunks: file.EstimatedChunks,
		})
	}
	return result, nil
}

func validateMCPUploadFiles(files []mcpIndexUploadFile) ([]indexManifestFile, error) {
	result := make([]indexManifestFile, 0, len(files))
	for _, file := range files {
		filePath, err := validateIndexSourcePath(file.Path)
		if err != nil {
			return nil, err
		}
		if !sha256Pattern.MatchString(strings.TrimSpace(file.Hash)) {
			return nil, fmt.Errorf("upload file hash must be SHA-256: %s", filePath)
		}
		if file.Content == "" || strings.IndexByte(file.Content, 0) >= 0 {
			return nil, fmt.Errorf("upload file must contain non-empty UTF-8 text without NUL bytes: %s", filePath)
		}
		result = append(result, indexManifestFile{
			Path:    filePath,
			Hash:    strings.ToLower(strings.TrimSpace(file.Hash)),
			Content: file.Content,
		})
	}
	if _, err := validateIndexBatchSize(result); err != nil {
		return nil, err
	}
	return result, nil
}

// handleCodebaseIndex 的 userID 参数语义为租户（tenant_id := org_id ?? user_id），
// 由 handleMCPToolsCall 解析后传入；索引数据与 LCE 调用全部按租户归属。
func handleCodebaseIndex(ctx context.Context, userID string, raw map[string]interface{}) (interface{}, error) {
	operation, _ := raw["operation"].(string)
	operation = strings.TrimSpace(operation)
	switch operation {
	case "start":
		if err := requireIndexArgument(raw, "files"); err != nil {
			return nil, err
		}
		var input mcpIndexStartArgs
		if err := decodeStrictIndexArgs(raw, &input); err != nil {
			return nil, err
		}
		rootID := normalizeIndexRootID(input.RootID)
		if !indexRootIDPattern.MatchString(rootID) {
			return nil, fmt.Errorf("root_id must match %s", indexRootIDPattern.String())
		}
		if err := checkIndexClientVersion(input.ClientVersion); err != nil {
			return nil, err
		}
		files, err := validateMCPManifestFiles(input.Files)
		if err != nil {
			return nil, err
		}
		unreadable := make([]string, 0, len(input.UnreadableFiles))
		for _, filePath := range input.UnreadableFiles {
			normalized, err := validateIndexSourcePath(filePath)
			if err != nil {
				return nil, err
			}
			unreadable = append(unreadable, normalized)
		}
		workspaceName := strings.TrimSpace(input.WorkspaceName)
		if workspaceName == "" {
			workspaceName = rootID
		}
		if len(workspaceName) > 256 || len(input.Branch) > 512 || len(input.Revision) > 512 {
			return nil, fmt.Errorf("workspace_name, branch, or revision exceeds its size limit")
		}
		return createIndexJob(ctx, userID, indexStartRequest{
			ProtocolVersion: indexProtocolVersion,
			WorkspaceID:     rootID,
			WorkspaceName:   workspaceName,
			RootID:          rootID,
			Branch:          strings.TrimSpace(input.Branch),
			Revision:        strings.TrimSpace(input.Revision),
			Files:           files,
			UnreadableFiles: unreadable,
		})

	case "upload":
		if err := requireIndexArgument(raw, "files"); err != nil {
			return nil, err
		}
		var input mcpIndexUploadArgs
		if err := decodeStrictIndexArgs(raw, &input); err != nil {
			return nil, err
		}
		files, err := validateMCPUploadFiles(input.Files)
		if err != nil {
			return nil, err
		}
		jobID := strings.TrimSpace(input.JobID)
		rootID := normalizeIndexRootID(input.RootID)
		if uuid.Validate(jobID) != nil {
			return nil, fmt.Errorf("job_id must be a UUID")
		}
		if !indexRootIDPattern.MatchString(rootID) {
			return nil, fmt.Errorf("root_id must match %s", indexRootIDPattern.String())
		}
		return uploadIndexBatch(ctx, userID, indexUploadRequest{
			JobID:  jobID,
			RootID: rootID,
			Files:  files,
		})

	case "status":
		var input mcpIndexJobArgs
		if err := decodeStrictIndexArgs(raw, &input); err != nil {
			return nil, err
		}
		if uuid.Validate(strings.TrimSpace(input.JobID)) != nil {
			return nil, fmt.Errorf("job_id must be a UUID")
		}
		job, err := getIndexJob(ctx, userID, input.JobID)
		if err != nil {
			return nil, err
		}
		return map[string]interface{}{"job": job}, nil

	case "complete":
		var input mcpIndexJobArgs
		if err := decodeStrictIndexArgs(raw, &input); err != nil {
			return nil, err
		}
		if uuid.Validate(strings.TrimSpace(input.JobID)) != nil {
			return nil, fmt.Errorf("job_id must be a UUID")
		}
		job, err := completeIndexJob(ctx, userID, input.JobID)
		if err != nil {
			return nil, err
		}
		return map[string]interface{}{"job": job}, nil

	case "fail":
		var input mcpIndexFailArgs
		if err := decodeStrictIndexArgs(raw, &input); err != nil {
			return nil, err
		}
		// error 是契约必填字段（cloud-protocol.json requiredFields.fail）
		if err := requireIndexArgument(raw, "error"); err != nil {
			return nil, err
		}
		if uuid.Validate(strings.TrimSpace(input.JobID)) != nil {
			return nil, fmt.Errorf("job_id must be a UUID")
		}
		job, err := failIndexJob(ctx, userID, input.JobID, input.Error)
		if err != nil {
			return nil, err
		}
		return map[string]interface{}{"job": job}, nil

	default:
		return nil, fmt.Errorf("operation must be one of: start, upload, status, complete, fail")
	}
}
