package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// ── 跨仓库契约钉住 ─────────────────────────────────────────────────────────
//
// contracts/cloud-protocol.json 是随 relay 提交的 LCE 云协议精确快照。
// 本测试把 relay 侧的契约面钉在这份文件上：
//   - codebase_index 工具 schema 的 operations 枚举与各 operation 的 required 集合
//   - 响应信封（codebaseIndexEnvelope）的 schemaVersion 与字段名
//   - manifest 条目字段（mcpIndexManifestFile 的 json tag）
//   - 客户端请求头（clientVersionHeader / Authorization）
//   - Relay 服务端公开工具面与契约 cloudToolSurface（排除本地/Relay 自有工具）精确一致
//
// 快照缺失、格式错误或实现漂移都必须让独立 CI 失败；同步快照时还会在
// 三仓联动验证中比较文件摘要，避免依赖 sibling checkout 和静默 skip。

type cloudProtocolContract struct {
	SchemaVersion    string   `json:"schemaVersion"`
	CloudToolSurface []string `json:"cloudToolSurface"`
	CodebaseIndex    struct {
		Operations         []string            `json:"operations"`
		RequiredFields     map[string][]string `json:"requiredFields"`
		OptionalFields     map[string][]string `json:"optionalFields"`
		FailureDiagnostics struct {
			Fields                []string `json:"fields"`
			PresenceRule          string   `json:"presenceRule"`
			InvalidValueBehavior  string   `json:"invalidValueBehavior"`
			MissingFieldsBehavior string   `json:"missingFieldsBehavior"`
			Codes                 map[string]struct {
				Origin   string `json:"origin"`
				Recovery string `json:"recovery"`
			} `json:"codes"`
		} `json:"failureDiagnostics"`
		StartOutcomes struct {
			Created struct {
				RequiredFields []string `json:"requiredFields"`
				OptionalFields []string `json:"optionalFields"`
				ProviderWork   string   `json:"providerWork"`
			} `json:"created"`
			Unchanged struct {
				RequiredFields []string `json:"requiredFields"`
				ProviderWork   string   `json:"providerWork"`
			} `json:"unchanged"`
			Busy struct {
				RequiredFields []string `json:"requiredFields"`
				OptionalFields []string `json:"optionalFields"`
				Reasons        []string `json:"reasons"`
				ProviderWork   string   `json:"providerWork"`
			} `json:"busy"`
			BusyRetryPolicy string `json:"busyRetryPolicy"`
		} `json:"startOutcomes"`
		LimitNegotiation struct {
			ToolMetadataField       string   `json:"toolMetadataField"`
			MetadataKey             string   `json:"metadataKey"`
			RequiredFields          []string `json:"requiredFields"`
			MissingMetadataBehavior string   `json:"missingMetadataBehavior"`
			InvalidMetadataBehavior string   `json:"invalidMetadataBehavior"`
		} `json:"limitNegotiation"`
		CompatibilityFallbackLimits struct {
			MaxFileBytes        int `json:"maxFileBytes"`
			MaxBatchFiles       int `json:"maxBatchFiles"`
			MaxBatchBytes       int `json:"maxBatchBytes"`
			EstimatedChunkBytes int `json:"estimatedChunkBytes"`
			MaxEstimatedChunks  int `json:"maxEstimatedChunks"`
			MaxManifestFiles    int `json:"maxManifestFiles"`
			MaxPathBytes        int `json:"maxPathBytes"`
		} `json:"compatibilityFallbackLimits"`
	} `json:"codebaseIndex"`
	ResponseEnvelope struct {
		SchemaVersion      string   `json:"schemaVersion"`
		Required           []string `json:"required"`
		OnSuccess          []string `json:"onSuccess"`
		OnError            []string `json:"onError"`
		ErrorShape         []string `json:"errorShape"`
		OptionalErrorShape []string `json:"optionalErrorShape"`
	} `json:"responseEnvelope"`
	ManifestEntryFields  []string `json:"manifestEntryFields"`
	ClientRequestHeaders []string `json:"clientRequestHeaders"`
	ClientCompatibility  struct {
		Package                                              string `json:"package"`
		LatestVersionSource                                  string `json:"latestVersionSource"`
		MinimumVersionSource                                 string `json:"minimumVersionSource"`
		MinimumVersionAdminConfigurable                      bool   `json:"minimumVersionAdminConfigurable"`
		IndexStartRequiresClientVersionWhenMinimumConfigured bool   `json:"indexStartRequiresClientVersionWhenMinimumConfigured"`
		UpgradeMechanism                                     string `json:"upgradeMechanism"`
		RestartRequiredAfterUpgrade                          bool   `json:"restartRequiredAfterUpgrade"`
	} `json:"clientCompatibility"`
	PromptEnhancement struct {
		ToolName string `json:"toolName"`
		Input    struct {
			RequiredFields      []string `json:"requiredFields"`
			OptionalFields      []string `json:"optionalFields"`
			RelayInjectedFields []string `json:"relayInjectedFields"`
		} `json:"input"`
		VerifiedReferenceFields []string `json:"verifiedReferenceFields"`
	} `json:"promptEnhancement"`
	DeepGraph struct {
		ToolName string `json:"toolName"`
		Input    struct {
			RequiredFields      []string `json:"requiredFields"`
			RelayRequiredFields []string `json:"relayRequiredFields"`
			OptionalFields      []string `json:"optionalFields"`
			RelayInjectedFields []string `json:"relayInjectedFields"`
		} `json:"input"`
	} `json:"deepGraph"`
	GraphAlgorithms struct {
		ToolName string `json:"toolName"`
		Input    struct {
			RequiredFields            []string            `json:"requiredFields"`
			ConditionalRequiredFields map[string][]string `json:"conditionalRequiredFields"`
			OptionalFields            []string            `json:"optionalFields"`
			RelayInjectedFields       []string            `json:"relayInjectedFields"`
		} `json:"input"`
	} `json:"graphAlgorithms"`
}

func loadCloudProtocolContract(t *testing.T) cloudProtocolContract {
	t.Helper()
	contractPath := filepath.Join("contracts", "cloud-protocol.json")
	raw, err := os.ReadFile(contractPath)
	if err != nil {
		t.Fatalf("read required cloud protocol snapshot %s: %v", contractPath, err)
	}
	var contract cloudProtocolContract
	if err := json.Unmarshal(raw, &contract); err != nil {
		t.Fatalf("parse %s: %v", contractPath, err)
	}
	return contract
}

func sortedCopy(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}

func diffStringSets(label string, got, want []string) string {
	g, w := sortedCopy(got), sortedCopy(want)
	if reflect.DeepEqual(g, w) {
		return ""
	}
	return fmt.Sprintf("%s mismatch:\n  relay:    %v\n  contract: %v", label, g, w)
}

// relayIndexOperations 从 codebaseIndexToolDefinition 的 JSON 中提取
// operation 枚举与各 operation 的 required 集合。
func relayIndexOperations(t *testing.T) map[string][]string {
	t.Helper()
	raw, err := codebaseIndexToolDefinition()
	if err != nil {
		t.Fatalf("build codebase_index tool definition: %v", err)
	}
	var definition struct {
		InputSchema struct {
			OneOf []struct {
				Required   []string `json:"required"`
				Properties struct {
					Operation struct {
						Const string `json:"const"`
					} `json:"operation"`
				} `json:"properties"`
			} `json:"oneOf"`
		} `json:"inputSchema"`
	}
	if err := json.Unmarshal(raw, &definition); err != nil {
		t.Fatalf("parse codebase_index tool definition: %v", err)
	}
	operations := make(map[string][]string)
	for _, branch := range definition.InputSchema.OneOf {
		op := branch.Properties.Operation.Const
		if op == "" {
			t.Fatalf("tool definition branch without operation const: %+v", branch)
		}
		if _, dup := operations[op]; dup {
			t.Fatalf("duplicate operation %q in tool definition", op)
		}
		operations[op] = branch.Required
	}
	return operations
}

func relayIndexOperationProperties(t *testing.T, operation string) []string {
	t.Helper()
	raw, err := codebaseIndexToolDefinition()
	if err != nil {
		t.Fatalf("build codebase_index tool definition: %v", err)
	}
	var definition struct {
		InputSchema struct {
			OneOf []struct {
				Properties map[string]json.RawMessage `json:"properties"`
			} `json:"oneOf"`
		} `json:"inputSchema"`
	}
	if err := json.Unmarshal(raw, &definition); err != nil {
		t.Fatalf("parse codebase_index tool definition: %v", err)
	}
	for _, branch := range definition.InputSchema.OneOf {
		operationSchema, ok := branch.Properties["operation"]
		if !ok {
			continue
		}
		var discriminator struct {
			Const string `json:"const"`
		}
		if err := json.Unmarshal(operationSchema, &discriminator); err != nil {
			t.Fatalf("parse operation discriminator: %v", err)
		}
		if discriminator.Const != operation {
			continue
		}
		fields := make([]string, 0, len(branch.Properties))
		for field := range branch.Properties {
			fields = append(fields, field)
		}
		return fields
	}
	t.Fatalf("tool definition missing operation %q", operation)
	return nil
}

func TestContractPinCodebaseIndexOperationsAndRequiredFields(t *testing.T) {
	contract := loadCloudProtocolContract(t)
	relayOps := relayIndexOperations(t)

	var relayNames []string
	for op := range relayOps {
		relayNames = append(relayNames, op)
	}
	if msg := diffStringSets("codebase_index operations", relayNames, contract.CodebaseIndex.Operations); msg != "" {
		t.Error(msg)
	}
	for op, wantRequired := range contract.CodebaseIndex.RequiredFields {
		gotRequired, ok := relayOps[op]
		if !ok {
			t.Errorf("contract requires operation %q, missing from relay tool schema", op)
			continue
		}
		if msg := diffStringSets(fmt.Sprintf("operation %q required fields", op), gotRequired, wantRequired); msg != "" {
			t.Error(msg)
		}
	}
}

func TestContractPinIndexFailureDiagnostics(t *testing.T) {
	contract := loadCloudProtocolContract(t)
	diagnostics := contract.CodebaseIndex.FailureDiagnostics
	wantFields := []string{"error_code", "error_origin", "recovery"}
	if msg := diffStringSets("failure diagnostic fields", diagnostics.Fields, wantFields); msg != "" {
		t.Fatal(msg)
	}
	if msg := diffStringSets("fail optional fields", contract.CodebaseIndex.OptionalFields["fail"], wantFields); msg != "" {
		t.Fatal(msg)
	}
	failProperties := relayIndexOperationProperties(t, "fail")
	failRequired := contract.CodebaseIndex.RequiredFields["fail"]
	requiredSet := make(map[string]bool, len(failRequired))
	for _, field := range failRequired {
		requiredSet[field] = true
	}
	var failOptional []string
	for _, field := range failProperties {
		if !requiredSet[field] {
			failOptional = append(failOptional, field)
		}
	}
	if msg := diffStringSets("fail tool schema optional fields", failOptional, wantFields); msg != "" {
		t.Error(msg)
	}
	if diagnostics.PresenceRule != "all_or_none" ||
		diagnostics.InvalidValueBehavior != "reject" ||
		diagnostics.MissingFieldsBehavior != "relay_classifies_error_text_and_persists_result" {
		t.Fatalf("failure diagnostic policy drifted: %+v", diagnostics)
	}
	if len(diagnostics.Codes) != len(indexFailureDiagnosticsByCode) {
		t.Fatalf("failure diagnostic code count: contract=%d relay=%d", len(diagnostics.Codes), len(indexFailureDiagnosticsByCode))
	}
	for code, expected := range indexFailureDiagnosticsByCode {
		got, ok := diagnostics.Codes[code]
		if !ok {
			t.Errorf("contract missing failure diagnostic code %q", code)
			continue
		}
		if got.Origin != expected.Origin || got.Recovery != expected.Recovery {
			t.Errorf("failure diagnostic %q = (%s, %s), want (%s, %s)",
				code, got.Origin, got.Recovery, expected.Origin, expected.Recovery)
		}
	}

	structType := reflect.TypeOf(mcpIndexFailArgs{})
	var relayOptionalFields []string
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		name, options, _ := strings.Cut(field.Tag.Get("json"), ",")
		if strings.Contains(options, "omitempty") {
			relayOptionalFields = append(relayOptionalFields, name)
		}
	}
	if msg := diffStringSets("relay fail optional fields", relayOptionalFields, wantFields); msg != "" {
		t.Error(msg)
	}
}

func TestContractPinIndexLimitNegotiation(t *testing.T) {
	contract := loadCloudProtocolContract(t)
	negotiation := contract.CodebaseIndex.LimitNegotiation
	if negotiation.ToolMetadataField != "_meta" ||
		negotiation.MetadataKey != codebaseIndexLimitsMetadataKey ||
		negotiation.MissingMetadataBehavior != "use_compatibility_fallback" ||
		negotiation.InvalidMetadataBehavior != "fail_before_manifest_scan" {
		t.Fatalf("index limit negotiation contract drifted: %+v", negotiation)
	}
	wantFields := []string{
		"maxFileBytes", "maxBatchFiles", "maxBatchBytes", "estimatedChunkBytes",
		"maxEstimatedChunks", "maxManifestFiles", "maxPathBytes",
	}
	if msg := diffStringSets("index limit metadata fields", negotiation.RequiredFields, wantFields); msg != "" {
		t.Fatal(msg)
	}
	metadata := codebaseIndexLimitMetadata()
	for _, field := range wantFields {
		if _, ok := metadata[field]; !ok {
			t.Fatalf("relay limit metadata missing %q", field)
		}
	}
	fallback := contract.CodebaseIndex.CompatibilityFallbackLimits
	if fallback.MaxFileBytes != maxIndexFileBytes ||
		fallback.MaxBatchFiles != maxIndexBatchFiles ||
		fallback.MaxBatchBytes != maxIndexBatchBytes ||
		fallback.EstimatedChunkBytes != estimatedIndexChunkBytes ||
		fallback.MaxEstimatedChunks != maxIndexEstimatedChunks ||
		fallback.MaxManifestFiles != maxIndexManifestFiles ||
		fallback.MaxPathBytes != maxIndexPathBytes {
		t.Fatalf("index compatibility fallback drifted: %+v", fallback)
	}
}

func TestContractPinIndexStartOutcomes(t *testing.T) {
	contract := loadCloudProtocolContract(t)
	if contract.SchemaVersion != "1.9" {
		t.Fatalf("cloud protocol schema version: got %q, want 1.9", contract.SchemaVersion)
	}
	outcomes := contract.CodebaseIndex.StartOutcomes
	if !reflect.DeepEqual(outcomes.Created.RequiredFields, []string{"job"}) ||
		!reflect.DeepEqual(outcomes.Created.OptionalFields, []string{"pending_files", "deleted_files"}) ||
		outcomes.Created.ProviderWork != "allowed" {
		t.Fatalf("created start outcome drifted: %+v", outcomes.Created)
	}
	if !reflect.DeepEqual(outcomes.Unchanged.RequiredFields, []string{"unchanged"}) || outcomes.Unchanged.ProviderWork != "forbidden" {
		t.Fatalf("unchanged start outcome drifted: %+v", outcomes.Unchanged)
	}
	if !reflect.DeepEqual(outcomes.Busy.RequiredFields, []string{"busy", "busy_reason", "retry_after_seconds"}) ||
		!reflect.DeepEqual(outcomes.Busy.OptionalFields, []string{"active_job"}) ||
		!reflect.DeepEqual(outcomes.Busy.Reasons, []string{indexStartBusyActiveJob, indexStartBusyRateLimited}) ||
		outcomes.Busy.ProviderWork != "forbidden" {
		t.Fatalf("busy start outcome drifted: %+v", outcomes.Busy)
	}
	if outcomes.BusyRetryPolicy != "client_waits_without_consuming_failure_retry_budget" {
		t.Fatalf("busy retry policy drifted: %q", outcomes.BusyRetryPolicy)
	}
}

func TestContractPinResponseEnvelope(t *testing.T) {
	contract := loadCloudProtocolContract(t)

	if indexEnvelopeSchemaVersion != contract.ResponseEnvelope.SchemaVersion {
		t.Errorf("envelope schemaVersion: relay %q, contract %q",
			indexEnvelopeSchemaVersion, contract.ResponseEnvelope.SchemaVersion)
	}

	success := codebaseIndexEnvelope(true, map[string]interface{}{"job_id": "j"}, "")
	failure := codebaseIndexEnvelope(false, nil, "boom", "provider_invalid_request")

	for _, field := range contract.ResponseEnvelope.Required {
		if _, ok := success[field]; !ok {
			t.Errorf("success envelope missing required field %q (has %v)", field, envelopeKeys(success))
		}
		if _, ok := failure[field]; !ok {
			t.Errorf("error envelope missing required field %q (has %v)", field, envelopeKeys(failure))
		}
	}
	for _, field := range contract.ResponseEnvelope.OnSuccess {
		if _, ok := success[field]; !ok {
			t.Errorf("success envelope missing field %q (has %v)", field, envelopeKeys(success))
		}
	}
	for _, field := range contract.ResponseEnvelope.OnError {
		if _, ok := failure[field]; !ok {
			t.Errorf("error envelope missing field %q (has %v)", field, envelopeKeys(failure))
		}
	}
	if ok, isBool := success["ok"].(bool); !isBool || !ok {
		t.Errorf("success envelope ok = %v, want true", success["ok"])
	}
	if ok, isBool := failure["ok"].(bool); !isBool || ok {
		t.Errorf("error envelope ok = %v, want false", failure["ok"])
	}
	errObj, isMap := failure["error"].(map[string]interface{})
	if !isMap {
		t.Fatalf("error envelope error field = %T, want object", failure["error"])
	}
	for _, field := range contract.ResponseEnvelope.ErrorShape {
		if _, ok := errObj[field]; !ok {
			t.Errorf("error object missing field %q (has %v)", field, envelopeKeys(errObj))
		}
	}
	if msg := diffStringSets(
		"optional error shape",
		contract.ResponseEnvelope.OptionalErrorShape,
		[]string{"code", "retry_after_seconds"},
	); msg != "" {
		t.Error(msg)
	}
	if errObj["code"] != "provider_invalid_request" {
		t.Errorf("error object code = %#v, want provider_invalid_request", errObj["code"])
	}
}

func envelopeKeys(envelope map[string]interface{}) []string {
	var keys []string
	for key := range envelope {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func TestContractPinManifestEntryFields(t *testing.T) {
	contract := loadCloudProtocolContract(t)

	structType := reflect.TypeOf(mcpIndexManifestFile{})
	var relayFields []string
	for i := 0; i < structType.NumField(); i++ {
		tag := structType.Field(i).Tag.Get("json")
		name, _, _ := strings.Cut(tag, ",")
		relayFields = append(relayFields, name)
	}
	if msg := diffStringSets("manifest entry fields", relayFields, contract.ManifestEntryFields); msg != "" {
		t.Error(msg)
	}
}

func TestContractPinClientRequestHeaders(t *testing.T) {
	contract := loadCloudProtocolContract(t)
	headers := make(map[string]bool)
	for _, header := range contract.ClientRequestHeaders {
		headers[header] = true
	}
	if !headers[clientVersionHeader] {
		t.Errorf("relay version-gate header %q not in contract clientRequestHeaders %v",
			clientVersionHeader, contract.ClientRequestHeaders)
	}
	// relay 认证读的是 Authorization Bearer 头（authenticateRequest）
	if !headers["Authorization"] {
		t.Errorf("contract clientRequestHeaders %v missing Authorization used by relay auth",
			contract.ClientRequestHeaders)
	}
}

func TestContractPinClientCompatibilityPolicy(t *testing.T) {
	contract := loadCloudProtocolContract(t)
	policy := contract.ClientCompatibility
	if policy.Package != cloudClientPackageName ||
		policy.LatestVersionSource != "npm_dist_tag_latest" ||
		policy.MinimumVersionSource != "relay_persistent_runtime_policy_with_env_bootstrap" ||
		!policy.MinimumVersionAdminConfigurable ||
		!policy.IndexStartRequiresClientVersionWhenMinimumConfigured ||
		policy.UpgradeMechanism != "runtime_or_client_package_manager" ||
		!policy.RestartRequiredAfterUpgrade {
		t.Fatalf("client compatibility policy drifted: %+v", policy)
	}
}

func TestContractPinRelayServerToolSurfaceMatchesContract(t *testing.T) {
	contract := loadCloudProtocolContract(t)
	// Relay owns codebase_index_status and the npm client owns the two local
	// worktree tools. Every other contract tool must be present in the Relay
	// allowlist, and Relay must not expose anything outside that set.
	notUpstreamChatTool := map[string]bool{
		codebaseIndexStatusToolName: true,
		"codebase_git_context":      true,
		"codebase_review_changes":   true,
	}
	want := make([]string, 0, len(contract.CloudToolSurface))
	for _, tool := range contract.CloudToolSurface {
		if !notUpstreamChatTool[tool] {
			want = append(want, tool)
		}
	}
	got := make([]string, 0, len(chatMCPToolPolicies))
	for tool := range chatMCPToolPolicies {
		got = append(got, tool)
	}
	if diff := diffStringSets("relay server tool surface", got, want); diff != "" {
		t.Error(diff)
	}
}

func TestContractPinDeepGraphAndAlgorithmPolicies(t *testing.T) {
	contract := loadCloudProtocolContract(t)
	for _, check := range []struct {
		label          string
		toolName       string
		contractFields []string
		relayRequired  []string
		optional       []string
		injected       []string
	}{
		{"deep graph", contract.DeepGraph.ToolName, contract.DeepGraph.Input.RequiredFields, contract.DeepGraph.Input.RelayRequiredFields, contract.DeepGraph.Input.OptionalFields, contract.DeepGraph.Input.RelayInjectedFields},
		{"graph algorithm", contract.GraphAlgorithms.ToolName, contract.GraphAlgorithms.Input.RequiredFields, contract.GraphAlgorithms.Input.RequiredFields, contract.GraphAlgorithms.Input.OptionalFields, contract.GraphAlgorithms.Input.RelayInjectedFields},
	} {
		policy, ok := chatMCPToolPolicies[check.toolName]
		if !ok {
			t.Fatalf("relay %s policy %q is missing", check.label, check.toolName)
		}
		wantArguments := append(append([]string(nil), check.contractFields...), check.optional...)
		if diff := diffStringSets(check.label+" arguments", sortedStringSetKeys(policy.arguments), wantArguments); diff != "" {
			t.Error(diff)
		}
		if diff := diffStringSets(check.label+" required arguments", sortedStringSetKeys(policy.required), check.relayRequired); diff != "" {
			t.Error(diff)
		}
		if !reflect.DeepEqual(check.injected, []string{"tenant_id"}) {
			t.Errorf("unexpected %s relay-injected fields: %v", check.label, check.injected)
		}
	}
	if !reflect.DeepEqual(contract.GraphAlgorithms.Input.ConditionalRequiredFields, map[string][]string{
		"submit": {"root_id", "algorithm"},
		"status": {"job_id"},
	}) {
		t.Errorf("graph algorithm conditional requirements drifted: %v", contract.GraphAlgorithms.Input.ConditionalRequiredFields)
	}
}

func TestContractPinPromptEnhancementPolicy(t *testing.T) {
	contract := loadCloudProtocolContract(t)
	policy, ok := chatMCPToolPolicies["codebase_enhance_prompt"]
	if !ok {
		t.Fatal("relay prompt enhancement policy is missing")
	}
	wantArguments := append(
		append([]string(nil), contract.PromptEnhancement.Input.RequiredFields...),
		contract.PromptEnhancement.Input.OptionalFields...,
	)
	if diff := diffStringSets("prompt enhancement arguments", sortedStringSetKeys(policy.arguments), wantArguments); diff != "" {
		t.Error(diff)
	}
	if diff := diffStringSets("prompt enhancement required arguments", sortedStringSetKeys(policy.required), contract.PromptEnhancement.Input.RequiredFields); diff != "" {
		t.Error(diff)
	}
	if contract.PromptEnhancement.ToolName != "codebase_enhance_prompt" {
		t.Errorf("unexpected prompt tool name: %q", contract.PromptEnhancement.ToolName)
	}
	if !reflect.DeepEqual(contract.PromptEnhancement.Input.RelayInjectedFields, []string{"tenant_id"}) {
		t.Errorf("unexpected relay-injected prompt fields: %v", contract.PromptEnhancement.Input.RelayInjectedFields)
	}
	if diff := diffStringSets(
		"prompt enhancement verified reference fields",
		contract.PromptEnhancement.VerifiedReferenceFields,
		[]string{"rootId", "path", "startLine", "endLine", "breadcrumb"},
	); diff != "" {
		t.Error(diff)
	}
}
