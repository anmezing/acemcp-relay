package main

import (
	"os"
	"strings"
	"testing"
)

func TestProductionNeo4jUsesSupportedAuthEnvironment(t *testing.T) {
	raw, err := os.ReadFile("deploy/docker-compose.yml")
	if err != nil {
		t.Fatalf("read production compose: %v", err)
	}

	compose := strings.ReplaceAll(string(raw), "\r\n", "\n")
	start := strings.Index(compose, "  neo4j:\n")
	end := strings.Index(compose, "\n  lce:\n")
	if start < 0 || end <= start {
		t.Fatal("could not isolate neo4j service in production compose")
	}
	neo4jService := compose[start:end]

	if strings.Contains(neo4jService, "\n      NEO4J_PASSWORD:") {
		t.Fatal("NEO4J_PASSWORD must remain a Compose input and must not be injected into the Neo4j container")
	}
	if !strings.Contains(neo4jService, "NEO4J_AUTH: neo4j/${NEO4J_PASSWORD:?") {
		t.Fatal("Neo4j authentication must be initialized through the supported NEO4J_AUTH variable")
	}
	if !strings.Contains(neo4jService, `--password "$${NEO4J_AUTH#*/}"`) {
		t.Fatal("Neo4j healthcheck must derive its password from NEO4J_AUTH")
	}
	if strings.Contains(neo4jService, `--password "$${NEO4J_PASSWORD}"`) {
		t.Fatal("Neo4j healthcheck must not depend on a standalone NEO4J_PASSWORD container variable")
	}
}
