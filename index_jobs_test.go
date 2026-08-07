package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestNormalizeManifest(t *testing.T) {
	files, err := normalizeManifest([]indexManifestFile{
		{Path: `src\z.go`, Hash: "z", Size: 10, EstimatedChunks: 0, Content: "discard"},
		{Path: "./src/a.go", Hash: "a", Size: 20, EstimatedChunks: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := []string{files[0].Path, files[1].Path}; !reflect.DeepEqual(got, []string{"src/a.go", "src/z.go"}) {
		t.Fatalf("unexpected normalized paths: %#v", got)
	}
	if files[1].EstimatedChunks != 1 {
		t.Fatalf("expected minimum estimated chunk count, got %d", files[1].EstimatedChunks)
	}
	if files[1].Content != "" {
		t.Fatal("manifest content must not be retained")
	}
}

func TestWorkspaceRootBindingChanged(t *testing.T) {
	tests := []struct {
		name      string
		exists    bool
		stored    string
		requested string
		changed   bool
	}{
		{name: "new workspace", exists: false, requested: "repo-a", changed: false},
		{name: "same root", exists: true, stored: "repo-a", requested: "repo-a", changed: false},
		{name: "legacy default root", exists: true, stored: "", requested: "repo-a", changed: true},
		{name: "renamed root", exists: true, stored: "repo-a", requested: "repo-b", changed: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := workspaceRootBindingChanged(tt.exists, tt.stored, tt.requested); got != tt.changed {
				t.Fatalf("workspaceRootBindingChanged() = %v, want %v", got, tt.changed)
			}
		})
	}
}

func TestValidateIndexContentAgainstManifest(t *testing.T) {
	const content = "package main\n"
	const hash = "df1d036cbbf3df46e2045071e082245ece204c7f53ecf0a4e022bff9bb228f47"
	if err := validateIndexContent(hash, int64(len(content)), content); err != nil {
		t.Fatalf("matching content was rejected: %v", err)
	}
	if err := validateIndexContent(hash, int64(len(content)+1), content); err == nil || !strings.Contains(err.Error(), "size") {
		t.Fatalf("size mismatch was not rejected: %v", err)
	}
	if err := validateIndexContent(strings.Repeat("0", 64), int64(len(content)), content); err == nil || !strings.Contains(err.Error(), "SHA-256") {
		t.Fatalf("hash mismatch was not rejected: %v", err)
	}
}

func TestNormalizeManifestRejectsDuplicates(t *testing.T) {
	_, err := normalizeManifest([]indexManifestFile{
		{Path: "src/a.go", Hash: "one"},
		{Path: `src\a.go`, Hash: "two"},
	})
	if err == nil {
		t.Fatal("expected duplicate path error")
	}
}

func TestDiffManifest(t *testing.T) {
	previous := map[string]indexManifestFile{
		"same.go":    {Path: "same.go", Hash: "same", Size: 10},
		"changed.go": {Path: "changed.go", Hash: "old", Size: 10},
		"deleted.go": {Path: "deleted.go", Hash: "gone", Size: 10},
	}
	current := []indexManifestFile{
		{Path: "changed.go", Hash: "new", Size: 11, EstimatedChunks: 3},
		{Path: "new.go", Hash: "new", Size: 5, EstimatedChunks: 2},
		{Path: "same.go", Hash: "same", Size: 10, EstimatedChunks: 1},
	}
	pending, deleted, chunks := diffManifest(previous, current)
	if !reflect.DeepEqual(pending, []string{"changed.go", "new.go"}) {
		t.Fatalf("unexpected pending files: %#v", pending)
	}
	if !reflect.DeepEqual(deleted, []string{"deleted.go"}) {
		t.Fatalf("unexpected deleted files: %#v", deleted)
	}
	if chunks != 5 {
		t.Fatalf("unexpected estimated chunks: %d", chunks)
	}
}

func TestExtractChunkCount(t *testing.T) {
	cases := []struct {
		content string
		count   int64
		exact   bool
	}{
		{`{"chunk_count":12}`, 12, true},
		{`{"result":{"indexedChunks":7}}`, 7, true},
		{`{"chunks":[{},{}]}`, 2, true},
		{`indexed 19 chunks`, 19, true},
		{`index completed`, 0, false},
	}
	for _, tc := range cases {
		count, exact := extractChunkCount([]byte(tc.content))
		if count != tc.count || exact != tc.exact {
			t.Fatalf("%q: got (%d, %v), want (%d, %v)", tc.content, count, exact, tc.count, tc.exact)
		}
	}
}

func TestFilterChatMCPToolsHidesIndexManagementTools(t *testing.T) {
	raw := json.RawMessage(`[
		{"name":" codebase-retrieval ","description":"query","inputSchema":{"type":"object","properties":{"information_request":{"type":"string"},"technical_terms":{"type":"array"},"response_format":{"type":"string"}}}},
		{"name":"codebase_remote_index","description":"index"},
		{"name":"codebase_clear_index","description":"clear"},
		{"name":"codebase_git_context","description":"git"},
		{"name":"codebase_review_changes","description":"review"},
		{"name":"codebase_find_missing","description":"missing"},
		{"name":"codebase_symbol_graph","description":"graph","inputSchema":{"type":"object","properties":{"root_id":{"type":"string"},"symbol":{"type":"string"}}}},
		{"name":"codebase_tenant_stats","description":"stats","inputSchema":{"type":"object","properties":{"response_format":{"type":"string"}}}},
		{"name":"future_admin_tool","description":"must stay private"}
	]`)
	filtered, err := filterChatMCPTools(raw)
	if err != nil {
		t.Fatal(err)
	}
	var tools []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(filtered, &tools); err != nil {
		t.Fatal(err)
	}
	got := make([]string, len(tools))
	for i, tool := range tools {
		got[i] = tool.Name
	}
	if !reflect.DeepEqual(got, []string{"codebase-retrieval", "codebase_symbol_graph", "codebase_tenant_stats"}) {
		t.Fatalf("unexpected chat MCP tools: %#v", got)
	}
}

func TestFilterChatMCPToolsRequiresExactRemoteContract(t *testing.T) {
	validTool := func(name string) string {
		return fmt.Sprintf(`{"name":%q,"inputSchema":{"type":"object","properties":{}}}`, name)
	}
	missing := json.RawMessage("[" + validTool("codebase-retrieval") + "]")
	if _, err := filterChatMCPTools(missing); err == nil {
		t.Fatal("missing required remote tools must fail the Relay contract")
	}
	duplicate := json.RawMessage("[" + strings.Join([]string{
		validTool("codebase-retrieval"),
		validTool("codebase-retrieval"),
		validTool("codebase_symbol_graph"),
		validTool("codebase_tenant_stats"),
	}, ",") + "]")
	if _, err := filterChatMCPTools(duplicate); err == nil {
		t.Fatal("duplicate remote tools must fail the Relay contract")
	}
}

func TestChatMCPToolPolicyKeepsTenantToolsAndRejectsRawManagement(t *testing.T) {
	for _, allowed := range []string{"codebase-retrieval", "codebase_symbol_graph", "codebase_tenant_stats", codebaseIndexToolName} {
		if !isChatMCPToolAllowed(allowed) {
			t.Fatalf("%q must remain available through chat MCP", allowed)
		}
	}
	for _, denied := range []string{
		"codebase_remote_index",
		"codebase_clear_index",
		"codebase_git_context",
		"codebase_review_changes",
		"codebase_find_missing",
		"future_admin_tool",
	} {
		if isChatMCPToolAllowed(denied) {
			t.Fatalf("%q must not be model-callable through chat MCP", denied)
		}
	}
}

func TestAppendCodebaseIndexToolExposesExactlyFourTools(t *testing.T) {
	raw := json.RawMessage(`[
		{"name":"codebase-retrieval","inputSchema":{"type":"object","properties":{"information_request":{"type":"string"}}}},
		{"name":"codebase_symbol_graph","inputSchema":{"type":"object","properties":{"root_id":{"type":"string"},"symbol":{"type":"string"}}}},
		{"name":"codebase_tenant_stats","inputSchema":{"type":"object","properties":{}}}
	]`)
	filtered, err := filterChatMCPTools(raw)
	if err != nil {
		t.Fatal(err)
	}
	combined, err := appendCodebaseIndexTool(filtered)
	if err != nil {
		t.Fatal(err)
	}
	var tools []map[string]interface{}
	if err := json.Unmarshal(combined, &tools); err != nil {
		t.Fatal(err)
	}
	if len(tools) != 4 {
		t.Fatalf("remote MCP must expose exactly four tools, got %d", len(tools))
	}
	if tools[3]["name"] != codebaseIndexToolName {
		t.Fatalf("fourth tool must be %s, got %#v", codebaseIndexToolName, tools[3]["name"])
	}
	schema := tools[3]["inputSchema"].(map[string]interface{})
	operations := schema["oneOf"].([]interface{})
	if len(operations) != 5 {
		t.Fatalf("index tool must advertise five lifecycle operations, got %d", len(operations))
	}
}

func TestRewriteToolSchemaRemovesLocalOnlyFields(t *testing.T) {
	tool := json.RawMessage(`{
		"name": "codebase-retrieval",
		"description": "search",
		"inputSchema": {
			"type": "object",
			"properties": {
				"information_request": {"type": "string"},
				"repo_path": {"type": "string"},
				"workspace_config_path": {"type": "string"},
				"tenant_id": {"type": "string"},
				"include_worktree": {"type": "boolean"},
				"freshness_policy": {"type": "string"},
				"technical_terms": {"type": "array"},
				"response_format": {"type": "string"},
				"output_mode": {"type": "string", "enum": ["context_pack", "context_bundle"]},
				"workflow": {"type": "string"},
				"direct_context": {"type": "object"},
				"ide_signals": {"type": "object"},
				"lineage_context": {"type": "object"},
				"future_local_option": {"type": "boolean"}
			},
			"required": ["information_request"],
			"oneOf": [
				{"required": ["repo_path"]},
				{"required": ["tenant_id"]}
			]
		}
	}`)

	rewritten, err := rewriteToolSchema(tool)
	if err != nil {
		t.Fatal(err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(rewritten, &result); err != nil {
		t.Fatal(err)
	}
	schema := result["inputSchema"].(map[string]interface{})
	props := schema["properties"].(map[string]interface{})

	for _, kept := range []string{"information_request", "technical_terms", "response_format"} {
		if _, exists := props[kept]; !exists {
			t.Fatalf("property %q should have been kept", kept)
		}
	}
	if len(props) != 3 {
		t.Fatalf("remote retrieval must advertise only its three supported caller arguments, got %#v", props)
	}
	if description, _ := result["description"].(string); !strings.Contains(description, "server-side LCE index") {
		t.Fatalf("remote retrieval description was not specialized: %q", description)
	}
	if _, exists := schema["oneOf"]; exists {
		t.Fatal("oneOf constraint should have been removed")
	}
}

func TestRewriteToolSchemaSymbolGraph(t *testing.T) {
	tool := json.RawMessage(`{
		"name": "codebase_symbol_graph",
		"description": "graph",
		"inputSchema": {
			"type": "object",
			"properties": {
				"repo_path": {"type": "string"},
				"tenant_id": {"type": "string"},
				"root_id": {"type": "string"},
				"symbol": {"type": "string"},
				"query_type": {"type": "string"},
				"depth": {"type": "integer"}
			},
			"required": ["symbol"],
			"oneOf": [
				{"required": ["repo_path"], "not": {"required": ["tenant_id"]}},
				{"required": ["tenant_id"], "not": {"required": ["repo_path"]}}
			]
		}
	}`)

	rewritten, err := rewriteToolSchema(tool)
	if err != nil {
		t.Fatal(err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(rewritten, &result); err != nil {
		t.Fatal(err)
	}
	schema := result["inputSchema"].(map[string]interface{})
	props := schema["properties"].(map[string]interface{})

	for _, removed := range []string{"repo_path", "tenant_id"} {
		if _, exists := props[removed]; exists {
			t.Fatalf("property %q should have been removed", removed)
		}
	}
	for _, kept := range []string{"root_id", "symbol", "query_type", "depth"} {
		if _, exists := props[kept]; !exists {
			t.Fatalf("property %q should have been kept", kept)
		}
	}
	if _, exists := schema["oneOf"]; exists {
		t.Fatal("oneOf constraint should have been removed")
	}
	required := schema["required"].([]interface{})
	if !reflect.DeepEqual(required, []interface{}{"root_id", "symbol"}) {
		t.Fatalf("required should contain root_id and symbol, got %v", required)
	}
}

func TestRewriteToolSchemaPassesThroughUnknownTools(t *testing.T) {
	original := json.RawMessage(`{"name":"future_tool","inputSchema":{"type":"object","properties":{"foo":{"type":"string"}}}}`)
	rewritten, err := rewriteToolSchema(original)
	if err != nil {
		t.Fatal(err)
	}
	var a, b map[string]interface{}
	json.Unmarshal(original, &a)
	json.Unmarshal(rewritten, &b)
	if !reflect.DeepEqual(a, b) {
		t.Fatal("unknown tools must pass through unmodified")
	}
}

func TestRewriteToolSchemaFailsClosedForMalformedAllowedTool(t *testing.T) {
	for _, raw := range []json.RawMessage{
		json.RawMessage(`{"name":"codebase-retrieval"}`),
		json.RawMessage(`{"name":"codebase-retrieval","inputSchema":true}`),
		json.RawMessage(`{"name":"codebase-retrieval","inputSchema":{"type":"object"}}`),
	} {
		if _, err := rewriteToolSchema(raw); err == nil {
			t.Fatalf("malformed allowed schema must fail closed: %s", raw)
		}
	}
}

func TestValidateChatMCPToolArgsRejectsUnknownArguments(t *testing.T) {
	args := map[string]interface{}{
		"information_request": "find auth code",
		"technical_terms":     []string{"auth"},
		"tenant_id":           "caller-controlled",
	}
	if err := validateChatMCPToolArgs("codebase-retrieval", args); err == nil {
		t.Fatal("caller-controlled tenant_id must be rejected")
	}
	delete(args, "tenant_id")
	args["future_local_option"] = true
	if err := validateChatMCPToolArgs("codebase-retrieval", args); err == nil {
		t.Fatal("unknown future arguments must be rejected until explicitly allowed")
	}
	delete(args, "future_local_option")
	if err := validateChatMCPToolArgs("codebase-retrieval", args); err != nil {
		t.Fatalf("declared retrieval arguments should pass: %v", err)
	}
}

func TestValidateChatMCPToolArgsRequiresRemoteRoot(t *testing.T) {
	if err := validateChatMCPToolArgs("codebase_symbol_graph", map[string]interface{}{"symbol": "Handler"}); err == nil {
		t.Fatal("symbol graph calls without root_id must be rejected")
	}
	if err := validateChatMCPToolArgs("codebase_symbol_graph", map[string]interface{}{
		"root_id": "  ", "symbol": "Handler",
	}); err == nil {
		t.Fatal("symbol graph calls with a blank root_id must be rejected")
	}
	if err := validateChatMCPToolArgs("codebase_symbol_graph", map[string]interface{}{
		"root_id": "root-123", "symbol": "Handler",
	}); err != nil {
		t.Fatalf("complete symbol graph call should pass: %v", err)
	}
}

func TestOnlyRetrievalUsesTenantModelConfig(t *testing.T) {
	if !chatMCPToolUsesModelConfig("codebase-retrieval") {
		t.Fatal("retrieval must receive the tenant embedding/rerank configuration")
	}
	for _, toolName := range []string{"codebase_symbol_graph", "codebase_tenant_stats"} {
		if chatMCPToolUsesModelConfig(toolName) {
			t.Fatalf("%s must not depend on embedding/rerank configuration", toolName)
		}
	}
}

func TestNormalizeIndexRootID(t *testing.T) {
	if got := normalizeIndexRootID("  acemcp-relay  "); got != "acemcp-relay" {
		t.Fatalf("expected trimmed rootId, got %q", got)
	}
	if got := normalizeIndexRootID(""); got != "" {
		t.Fatalf("empty rootId must stay empty (falls back to LCE default root), got %q", got)
	}
	long := make([]byte, maxIndexRootIDLen+50)
	for i := range long {
		long[i] = 'a'
	}
	if got := normalizeIndexRootID(string(long)); len(got) != len(long) {
		t.Fatalf("rootId normalization must not silently truncate identity, got len %d", len(got))
	}
}

func TestLCEIndexRootIDMapsLegacyEmptyBindingToDefault(t *testing.T) {
	if got := lceIndexRootID(""); got != defaultLCEIndexRootID {
		t.Fatalf("legacy empty root must map to LCE default root, got %q", got)
	}
	if got := lceIndexRootID(" repo-a "); got != "repo-a" {
		t.Fatalf("explicit root should be preserved after trimming, got %q", got)
	}
}

func TestGraftUnreadablePathsKeepsPreviouslyIndexedFiles(t *testing.T) {
	previous := map[string]indexManifestFile{
		"locked.go":  {Path: "locked.go", Hash: "old-hash", Size: 10, EstimatedChunks: 2},
		"same.go":    {Path: "same.go", Hash: "same", Size: 5, EstimatedChunks: 1},
		"deleted.go": {Path: "deleted.go", Hash: "gone", Size: 5, EstimatedChunks: 1},
	}
	current := []indexManifestFile{
		{Path: "same.go", Hash: "same", Size: 5, EstimatedChunks: 1},
	}

	files, err := graftUnreadablePaths(previous, current, []string{`locked.go`, "never-indexed.go", "", "locked.go"})
	if err != nil {
		t.Fatal(err)
	}
	if got := []string{files[0].Path, files[1].Path}; !reflect.DeepEqual(got, []string{"locked.go", "same.go"}) {
		t.Fatalf("unexpected grafted manifest: %#v", got)
	}

	pending, deleted, _ := diffManifest(previous, files)
	if len(pending) != 0 {
		t.Fatalf("unreadable files must not be re-uploaded: %#v", pending)
	}
	if !reflect.DeepEqual(deleted, []string{"deleted.go"}) {
		t.Fatalf("only truly missing files may be deleted: %#v", deleted)
	}
}

func TestGraftUnreadablePathsPrefersCurrentManifestEntry(t *testing.T) {
	previous := map[string]indexManifestFile{
		"a.go": {Path: "a.go", Hash: "old", Size: 1, EstimatedChunks: 1},
	}
	current := []indexManifestFile{
		{Path: "a.go", Hash: "new", Size: 2, EstimatedChunks: 1},
	}
	files, err := graftUnreadablePaths(previous, current, []string{"a.go"})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Hash != "new" {
		t.Fatalf("current manifest entry must win over grafted snapshot entry: %#v", files)
	}
}
