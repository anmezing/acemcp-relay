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
//   - 公开工具面是契约 cloudToolSurface 的子集
//
// 快照缺失、格式错误或实现漂移都必须让独立 CI 失败；同步快照时还会在
// 三仓联动验证中比较文件摘要，避免依赖 sibling checkout 和静默 skip。

type cloudProtocolContract struct {
	SchemaVersion    string   `json:"schemaVersion"`
	CloudToolSurface []string `json:"cloudToolSurface"`
	CodebaseIndex    struct {
		Operations     []string            `json:"operations"`
		RequiredFields map[string][]string `json:"requiredFields"`
		StartOutcomes  struct {
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
		SchemaVersion string   `json:"schemaVersion"`
		Required      []string `json:"required"`
		OnSuccess     []string `json:"onSuccess"`
		OnError       []string `json:"onError"`
		ErrorShape    []string `json:"errorShape"`
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
	if contract.SchemaVersion != "1.8" {
		t.Fatalf("cloud protocol schema version: got %q, want 1.8", contract.SchemaVersion)
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
	failure := codebaseIndexEnvelope(false, nil, "boom")

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

func TestContractPinPublicToolSurfaceIsContractSubset(t *testing.T) {
	contract := loadCloudProtocolContract(t)
	surface := make(map[string]bool)
	for _, tool := range contract.CloudToolSurface {
		surface[tool] = true
	}
	for tool := range chatMCPToolPolicies {
		if !surface[tool] {
			t.Errorf("relay exposes tool %q not present in contract cloudToolSurface %v",
				tool, contract.CloudToolSurface)
		}
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
