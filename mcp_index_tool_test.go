package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestCodebaseIndexDefinitionKeepsTenantAndControlFieldsServerManaged(t *testing.T) {
	raw, err := codebaseIndexToolDefinition()
	if err != nil {
		t.Fatal(err)
	}
	var tool map[string]interface{}
	if err := json.Unmarshal(raw, &tool); err != nil {
		t.Fatal(err)
	}
	if tool["name"] != codebaseIndexToolName {
		t.Fatalf("unexpected tool name: %#v", tool["name"])
	}
	encoded := string(raw)
	for _, forbidden := range []string{"tenant_id", "model_config", "deleted_files", "protocol_version"} {
		if strings.Contains(encoded, forbidden) {
			t.Fatalf("caller must not control %s", forbidden)
		}
	}
	for _, operation := range []string{"start", "upload", "status", "complete", "fail"} {
		if !strings.Contains(encoded, `"const":"`+operation+`"`) {
			t.Fatalf("missing operation schema: %s", operation)
		}
	}
}

func TestIndexRootIDPatternAcceptsBranchViewEncoding(t *testing.T) {
	for _, allowed := range []string{
		"repo-a",
		"repo-a@feature-x",
		"repo.a_b:c@release-1.2",
		"repo@a@b", // 多个 '@'：/mcp/roots 按最后一个拆分
	} {
		if !indexRootIDPattern.MatchString(allowed) {
			t.Fatalf("root_id %q must be accepted", allowed)
		}
	}
	for _, denied := range []string{
		"@feature-x",       // 首字符必须是字母数字
		"repo-a@feature/x", // '/' 不在字符集内：客户端必须在编码分支时清洗
		"repo a@feature",   // 空格非法
		"a" + strings.Repeat("b", maxIndexRootIDLen), // 超长
	} {
		if indexRootIDPattern.MatchString(denied) {
			t.Fatalf("root_id %q must be rejected", denied)
		}
	}
}

func TestValidateIndexSourcePathEnforcesRemoteSafetyBoundary(t *testing.T) {
	for _, allowed := range []string{"src/main.go", "README.md", "web/app.tsx", "config/example.yaml"} {
		got, err := validateIndexSourcePath(allowed)
		if err != nil || got != allowed {
			t.Fatalf("expected %q to pass unchanged, got %q, %v", allowed, got, err)
		}
	}
	for _, denied := range []string{
		"../secret.txt",
		"/etc/passwd",
		`C:\repo\main.go`,
		".env.production",
		"src/private.pem",
		"node_modules/pkg/index.js",
		"dist/bundle.js",
		"pnpm-lock.yaml",
		"assets/logo.svg",
		strings.Repeat("a", maxIndexPathBytes+1),
	} {
		if _, err := validateIndexSourcePath(denied); err == nil {
			t.Fatalf("expected excluded path to fail: %s", denied)
		}
	}
}

func TestMCPIndexFileValidationRequiresRealSHAAndText(t *testing.T) {
	hash := strings.Repeat("a", 64)
	manifest, err := validateMCPManifestFiles([]mcpIndexManifestFile{{
		Path: "src/main.go", Hash: hash, Size: 12, EstimatedChunks: 1,
	}})
	if err != nil || len(manifest) != 1 || manifest[0].Hash != hash {
		t.Fatalf("valid manifest rejected: %#v, %v", manifest, err)
	}
	if _, err := validateMCPManifestFiles([]mcpIndexManifestFile{{
		Path: "src/main.go", Hash: "not-a-sha", Size: 12, EstimatedChunks: 1,
	}}); err == nil {
		t.Fatal("non-SHA-256 manifest hash must fail")
	}
	if _, err := validateMCPManifestFiles([]mcpIndexManifestFile{{
		Path: "src/main.go", Hash: hash, Size: 0, EstimatedChunks: 1,
	}}); err == nil {
		t.Fatal("empty files must stay outside the index manifest")
	}
	if _, err := validateMCPManifestFiles([]mcpIndexManifestFile{{
		Path: "src/main.go", Hash: hash, Size: maxIndexFileBytes, EstimatedChunks: maxIndexEstimatedChunks,
	}}); err != nil {
		t.Fatalf("maximum valid estimated chunk count was rejected: %v", err)
	}
	if _, err := validateMCPManifestFiles([]mcpIndexManifestFile{{
		Path: "src/main.go", Hash: hash, Size: maxIndexFileBytes, EstimatedChunks: maxIndexEstimatedChunks + 1,
	}}); err == nil || !strings.Contains(err.Error(), "between 1 and 128") {
		t.Fatalf("estimated chunk count above the protocol maximum must fail, got %v", err)
	}
	if _, err := validateMCPUploadFiles([]mcpIndexUploadFile{{
		Path: "src/main.go", Hash: hash, Content: "a\x00b",
	}}); err == nil {
		t.Fatal("NUL-containing content must be rejected as binary")
	}
}

func TestDecodeStrictIndexArgsRejectsFieldsFromAnotherOperation(t *testing.T) {
	raw := map[string]interface{}{
		"operation": "status",
		"job_id":    "b6ecef83-37f1-4664-930f-da2b2efb94f0",
		"root_id":   "caller-must-not-mix-operation-fields",
	}
	var input mcpIndexJobArgs
	if err := decodeStrictIndexArgs(raw, &input); err == nil {
		t.Fatal("status must reject upload/start-only fields")
	}
}

func TestHandleCodebaseIndexRejectsInvalidLifecycleInputBeforeDatabaseAccess(t *testing.T) {
	for _, raw := range []map[string]interface{}{
		{"operation": "unknown"},
		{"operation": "status", "job_id": "not-a-uuid"},
		{"operation": "start", "root_id": "root-ok"},
		{"operation": "upload", "job_id": "b6ecef83-37f1-4664-930f-da2b2efb94f0", "root_id": "root-ok"},
		{"operation": "upload", "job_id": "not-a-uuid", "root_id": "root-ok", "files": []interface{}{}},
	} {
		if _, err := handleCodebaseIndex(context.Background(), "tenant", raw); err == nil {
			t.Fatalf("invalid lifecycle input must fail: %#v", raw)
		}
	}
}

func TestCheckIndexStartRateLimitEnforcesPerRootInterval(t *testing.T) {
	base := time.Now()

	// full sync 首次调用必须放行
	if wait := checkIndexStartRateLimit("rate-user-a", "root-1", base); wait != 0 {
		t.Fatalf("first start must pass, got wait=%d", wait)
	}
	// 窗口内的重试要被挡下，并给出剩余等待秒数
	if wait := checkIndexStartRateLimit("rate-user-a", "root-1", base.Add(5*time.Second)); wait <= 0 || wait > 30 {
		t.Fatalf("retry within interval must be limited with 0 < wait <= 30, got %d", wait)
	}
	// 不同 root 互不影响
	if wait := checkIndexStartRateLimit("rate-user-a", "root-2", base.Add(5*time.Second)); wait != 0 {
		t.Fatalf("different root must pass, got wait=%d", wait)
	}
	// 不同用户互不影响
	if wait := checkIndexStartRateLimit("rate-user-b", "root-1", base.Add(6*time.Second)); wait != 0 {
		t.Fatalf("different user must pass, got wait=%d", wait)
	}
	// 间隔过后放行
	if wait := checkIndexStartRateLimit("rate-user-a", "root-1", base.Add(indexStartMinInterval+5*time.Second)); wait != 0 {
		t.Fatalf("start after interval must pass, got wait=%d", wait)
	}
}

func TestCheckIndexClientVersionGatesStartArgsLikeHeader(t *testing.T) {
	previous := currentMinClientVersion()
	t.Cleanup(func() { setMinClientVersion(previous) })

	setMinClientVersion("")
	if err := checkIndexClientVersion("0.0.1"); err != nil {
		t.Fatalf("no minimum configured must not gate, got %v", err)
	}

	setMinClientVersion("1.1.0")
	if err := checkIndexClientVersion(""); err == nil || !strings.Contains(err.Error(), "client_version is required") {
		t.Fatalf("clients not reporting client_version must be rejected when a minimum is configured, got %v", err)
	}
	if err := checkIndexClientVersion("1.1.0"); err != nil {
		t.Fatalf("matching version must pass, got %v", err)
	}
	if err := checkIndexClientVersion("1.0.9"); err == nil || !strings.Contains(err.Error(), "below minimum") {
		t.Fatalf("outdated version must be rejected with upgrade hint, got %v", err)
	}
	if err := checkIndexClientVersion("1.1.0-beta"); err == nil {
		t.Fatal("prerelease of the minimum version must be rejected")
	}
}

func TestHandleCodebaseIndexStartRejectsOutdatedClientVersionArg(t *testing.T) {
	previous := currentMinClientVersion()
	t.Cleanup(func() { setMinClientVersion(previous) })
	setMinClientVersion("1.1.0")

	// 版本门禁在文件校验与 DB 访问之前触发，返回工具错误信封所用的 error
	_, err := handleCodebaseIndex(context.Background(), "tenant-version-gate", map[string]interface{}{
		"operation":      "start",
		"root_id":        "root-version-gate",
		"client_version": "1.0.0",
		"files":          []interface{}{},
	})
	if err == nil || !strings.Contains(err.Error(), "below minimum") {
		t.Fatalf("start with outdated client_version must fail with upgrade hint, got %v", err)
	}
}

func TestHandleCodebaseIndexStartRejectsMissingClientVersionWhenMinimumConfigured(t *testing.T) {
	previous := currentMinClientVersion()
	t.Cleanup(func() { setMinClientVersion(previous) })
	setMinClientVersion("1.1.0")

	_, err := handleCodebaseIndex(context.Background(), "tenant-version-gate", map[string]interface{}{
		"operation": "start",
		"root_id":   "root-version-gate",
		"files":     []interface{}{},
	})
	if err == nil || !strings.Contains(err.Error(), "client_version is required") {
		t.Fatalf("start without client_version must fail when a minimum is configured, got %v", err)
	}
}

func TestAppendCodebaseIndexToolRejectsDuplicateDefinition(t *testing.T) {
	definition, err := codebaseIndexToolDefinition()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal([]json.RawMessage{definition})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := appendCodebaseIndexTool(raw); err == nil {
		t.Fatal("duplicate service-local tool name must fail closed")
	}
}

func TestAppendCodebaseIndexToolExposesExactlyFourPublicTools(t *testing.T) {
	upstream := json.RawMessage(`[
		{"name":"codebase-retrieval"},
		{"name":"codebase_symbol_graph"}
	]`)
	combined, err := appendCodebaseIndexTool(upstream)
	if err != nil {
		t.Fatal(err)
	}
	var tools []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(combined, &tools); err != nil {
		t.Fatal(err)
	}
	want := []string{"codebase-retrieval", "codebase_symbol_graph", codebaseIndexToolName}
	if len(tools) != len(want) {
		t.Fatalf("public tool count = %d, want %d", len(tools), len(want))
	}
	for i, tool := range tools {
		if tool.Name != want[i] {
			t.Fatalf("tool %d = %q, want %q", i, tool.Name, want[i])
		}
	}
}

func TestCodebaseIndexFailRequiresErrorField(t *testing.T) {
	// error 是契约必填字段（cloud-protocol.json requiredFields.fail）
	_, err := handleCodebaseIndex(context.Background(), "user-1", map[string]interface{}{
		"operation": "fail",
		"job_id":    "7e224a32-3423-4bb0-9213-3c55a5797c9d",
	})
	if err == nil || !strings.Contains(err.Error(), "missing required argument: error") {
		t.Fatalf("fail without error must be rejected before touching state, got: %v", err)
	}
}
