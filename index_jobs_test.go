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

func TestIndexControlPathsDoNotConsumeChatQuota(t *testing.T) {
	exempt := []string{
		"/relay/index-jobs",
		"/relay/index-jobs/job-id",
		"/relay/index-jobs/job-id/complete",
		"/relay/remote-index",
	}
	for _, requestPath := range exempt {
		if !isIndexControlPath(requestPath) {
			t.Fatalf("expected index control path to be exempt: %s", requestPath)
		}
	}

	charged := []string{
		"/relay/agents/codebase-retrieval",
		"/relay/index-jobs-extra",
		"/mcp",
	}
	for _, requestPath := range charged {
		if isIndexControlPath(requestPath) {
			t.Fatalf("unexpected quota exemption: %s", requestPath)
		}
	}
}

func TestIndexQuotaChargesOnlyJobCreation(t *testing.T) {
	exempt := []struct{ method, path string }{
		{"GET", "/relay/index-jobs/job-id"},
		{"POST", "/relay/index-jobs/job-id/complete"},
		{"POST", "/relay/index-jobs/job-id/fail"},
		{"POST", "/relay/remote-index"},
	}
	for _, request := range exempt {
		if !isIndexQuotaExempt(request.method, request.path) {
			t.Fatalf("expected quota exemption: %s %s", request.method, request.path)
		}
	}

	charged := []struct{ method, path string }{
		{"POST", "/relay/index-jobs"},
		{"POST", "/relay/agents/codebase-retrieval"},
		{"POST", "/mcp"},
	}
	for _, request := range charged {
		if isIndexQuotaExempt(request.method, request.path) {
			t.Fatalf("unexpected quota exemption: %s %s", request.method, request.path)
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
	if got := normalizeIndexRootID(string(long)); len(got) != maxIndexRootIDLen {
		t.Fatalf("expected rootId capped at %d, got len %d", maxIndexRootIDLen, len(got))
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
