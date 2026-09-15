package main

import (
	"os"
	"strings"
	"testing"
)

func TestCloudIndexPathPolicyPreservesRelayRestrictions(t *testing.T) {
	// Independent acceptance cases pin the pre-existing server boundary, including
	// every extension. Refactoring the list into a shared policy must not loosen it.
	denied := []string{".env", "nested/.ENV.production", "package-lock.json", "nested/PACKAGE-LOCK.JSON",
		"nested/yarn.lock", "nested/pnpm-lock.yaml", "nested/bundle.MIN.JS", "nested/style.min.css", "nested/.PEM"}
	for _, segment := range []string{".git", "node_modules", ".turbo", "dist", "build", ".next", "__pycache__"} {
		denied = append(denied, "nested/"+strings.ToUpper(segment)+"/index.ts")
	}
	for _, ext := range []string{".pyc", ".class", ".o", ".so", ".dll", ".exe", ".wasm", ".map", ".pem", ".key", ".cert",
		".png", ".jpg", ".jpeg", ".gif", ".ico", ".svg", ".woff", ".woff2", ".ttf", ".eot", ".mp3", ".mp4", ".zip", ".tar", ".gz", ".rar", ".pdf"} {
		denied = append(denied, "nested/file"+strings.ToUpper(ext))
	}
	for _, value := range denied {
		if _, err := validateIndexSourcePath(value); err == nil || !strings.Contains(err.Error(), "index path is excluded") {
			t.Errorf("expected excluded path %q, got %v", value, err)
		}
	}
	for _, value := range []string{"package.json", "src/build.ts", "rebuild/index.ts", "src/node_modules.ts", "env.example.ts", "src/main.go", "src/Program.cs", "src/View.swift", "README.md"} {
		if got, err := validateIndexSourcePath(value); err != nil || got != value {
			t.Errorf("expected source path %q, got %q, %v", value, got, err)
		}
	}
}

func TestDockerBuildIncludesEmbeddedIndexPathPolicy(t *testing.T) {
	data, err := os.ReadFile("Dockerfile")
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	copyAt := strings.Index(text, "COPY contracts/cloud-index-path-policy.json ./contracts/cloud-index-path-policy.json")
	buildAt := strings.Index(text, "go build")
	if copyAt < 0 || buildAt < copyAt {
		t.Fatal("Docker must copy the embedded upload policy before compiling Relay")
	}
}
