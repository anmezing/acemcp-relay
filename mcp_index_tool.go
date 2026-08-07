package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"regexp"
	"strings"

	"github.com/google/uuid"
)

const (
	codebaseIndexToolName = "codebase_index"
	maxIndexPathBytes     = 4096
)

var (
	sha256Pattern      = regexp.MustCompile(`^[a-fA-F0-9]{64}$`)
	windowsDrivePrefix = regexp.MustCompile(`^[A-Za-z]:`)
	indexRootIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
)

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

func codebaseIndexToolDefinition() (json.RawMessage, error) {
	manifestFile := map[string]interface{}{
		"type":                 "object",
		"additionalProperties": false,
		"required":             []string{"path", "hash", "size", "estimated_chunks"},
		"properties": map[string]interface{}{
			"path":             map[string]interface{}{"type": "string", "minLength": 1, "maxLength": maxIndexPathBytes},
			"hash":             map[string]interface{}{"type": "string", "pattern": "^[a-fA-F0-9]{64}$"},
			"size":             map[string]interface{}{"type": "integer", "minimum": 1, "maximum": maxIndexFileBytes},
			"estimated_chunks": map[string]interface{}{"type": "integer", "minimum": 1},
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
		"description": "Synchronize the current local workspace into the authenticated tenant index without an IDE plugin. " +
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
					"required":             []string{"operation", "job_id"},
					"properties": map[string]interface{}{
						"operation": operation("fail"),
						"job_id":    map[string]interface{}{"type": "string", "minLength": 1},
						"error":     map[string]interface{}{"type": "string", "maxLength": 2000},
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
		if file.Size < 1 || file.Size > maxIndexFileBytes {
			return nil, fmt.Errorf("manifest file size is invalid: %s", filePath)
		}
		if file.EstimatedChunks < 1 || file.EstimatedChunks > int(maxIndexFileBytes) {
			return nil, fmt.Errorf("estimated_chunks must be between 1 and %d: %s", maxIndexFileBytes, filePath)
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
