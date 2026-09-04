package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCodebaseIndexStatusToolDefinition(t *testing.T) {
	raw, err := codebaseIndexStatusToolDefinition()
	if err != nil {
		t.Fatal(err)
	}
	var tool struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		InputSchema struct {
			Properties           map[string]interface{} `json:"properties"`
			AdditionalProperties bool                   `json:"additionalProperties"`
		} `json:"inputSchema"`
	}
	if err := json.Unmarshal(raw, &tool); err != nil {
		t.Fatal(err)
	}
	if tool.Name != codebaseIndexStatusToolName {
		t.Fatalf("unexpected tool name %q", tool.Name)
	}
	if _, ok := tool.InputSchema.Properties["root_id"]; !ok {
		t.Fatal("root_id property missing")
	}
	if tool.InputSchema.AdditionalProperties {
		t.Fatal("status arguments must reject unknown properties")
	}
	if !strings.Contains(strings.ToLower(tool.Description), "progress") {
		t.Fatalf("description must tell agents that progress is returned: %q", tool.Description)
	}
}

func TestValidateCodebaseIndexStatusArgs(t *testing.T) {
	for _, args := range []map[string]interface{}{
		nil,
		{},
		{"root_id": "repo@main"},
	} {
		if err := validateCodebaseIndexStatusArgs(args); err != nil {
			t.Fatalf("valid args %#v rejected: %v", args, err)
		}
	}
	for _, args := range []map[string]interface{}{
		{"root_id": ""},
		{"root_id": 1},
		{"repo_path": "/tmp/repo"},
	} {
		if err := validateCodebaseIndexStatusArgs(args); err == nil {
			t.Fatalf("invalid args %#v accepted", args)
		}
	}
}

func TestAppendCodebaseIndexStatusTool(t *testing.T) {
	combined, err := appendCodebaseIndexStatusTool(json.RawMessage(`[{"name":"codebase-retrieval"}]`))
	if err != nil {
		t.Fatal(err)
	}
	var tools []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(combined, &tools); err != nil {
		t.Fatal(err)
	}
	if len(tools) != 2 || tools[1].Name != codebaseIndexStatusToolName {
		t.Fatalf("status tool was not appended: %#v", tools)
	}
	if _, err := appendCodebaseIndexStatusTool(combined); err == nil {
		t.Fatal("duplicate status tool must be rejected")
	}
}

func TestRelayOwnsIndexStatusWhenUpstreamAlsoExposesIt(t *testing.T) {
	// The npm cloud client exposes a local status tool too. If an upstream LCE
	// server ever does the same, Relay must filter that copy and publish exactly
	// one tenant-backed status tool instead of breaking tools/list with a duplicate.
	upstream := json.RawMessage(`[
		{"name":"codebase-retrieval","inputSchema":{"type":"object","properties":{"information_request":{"type":"string"}}}},
		{"name":"codebase_symbol_graph","inputSchema":{"type":"object","properties":{"root_id":{"type":"string"},"symbol":{"type":"string"}}}},
		{"name":"codebase_deep_graph","inputSchema":{"type":"object","properties":{"root_id":{"type":"string"},"symbol":{"type":"string"}}}},
		{"name":"codebase_graph_algorithm","inputSchema":{"type":"object","properties":{"operation":{"type":"string"},"root_id":{"type":"string"},"job_id":{"type":"string"},"algorithm":{"type":"string"}}}},
		{"name":"codebase_enhance_prompt","inputSchema":{"type":"object","properties":{"prompt":{"type":"string"}}}},
		{"name":"codebase_index_status","inputSchema":{"type":"object","properties":{"repo_path":{"type":"string"}}}}
	]`)

	filtered, err := filterChatMCPTools(upstream)
	if err != nil {
		t.Fatal(err)
	}
	withIndex, err := appendCodebaseIndexTool(filtered)
	if err != nil {
		t.Fatal(err)
	}
	combined, err := appendCodebaseIndexStatusTool(withIndex)
	if err != nil {
		t.Fatal(err)
	}

	var tools []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(combined, &tools); err != nil {
		t.Fatal(err)
	}
	statusCount := 0
	for _, tool := range tools {
		if tool.Name == codebaseIndexStatusToolName {
			statusCount++
		}
	}
	if statusCount != 1 {
		t.Fatalf("expected exactly one Relay-owned status tool, got %d in %#v", statusCount, tools)
	}
}
