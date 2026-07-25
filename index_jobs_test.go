package main

import (
	"encoding/json"
	"reflect"
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
		{"name":"codebase-retrieval","description":"query"},
		{"name":"codebase_remote_index","description":"index"},
		{"name":"codebase_clear_index","description":"clear"},
		{"name":"graph_query","description":"graph"}
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
	got := []string{tools[0].Name, tools[1].Name}
	if !reflect.DeepEqual(got, []string{"codebase-retrieval", "graph_query"}) {
		t.Fatalf("unexpected chat MCP tools: %#v", got)
	}
}

func TestChatMCPToolPolicyKeepsQueriesAndRejectsManagement(t *testing.T) {
	if !isChatMCPToolAllowed("codebase-retrieval") {
		t.Fatal("query tool must remain available")
	}
	if isChatMCPToolAllowed("codebase_remote_index") {
		t.Fatal("remote indexing must not be model-callable")
	}
	if isChatMCPToolAllowed("codebase_clear_index") {
		t.Fatal("clear index must remain an explicit user action")
	}
}
