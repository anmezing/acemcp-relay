package main

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

var composeServiceHeader = regexp.MustCompile(`(?m)^  [A-Za-z0-9][A-Za-z0-9_-]*:[ \t]*$`)

func readProductionCompose(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile("deploy/docker-compose.yml")
	if err != nil {
		t.Fatalf("read production compose: %v", err)
	}
	return strings.ReplaceAll(string(raw), "\r\n", "\n")
}

func productionComposeService(t *testing.T, compose, service string) string {
	t.Helper()
	marker := "  " + service + ":\n"
	start := strings.Index(compose, marker)
	if start < 0 {
		t.Fatalf("could not find %s service in production compose", service)
	}

	tail := compose[start+len(marker):]
	if next := composeServiceHeader.FindStringIndex(tail); next != nil {
		tail = tail[:next[0]]
	}
	return marker + tail
}

func TestProductionNeo4jUsesSupportedAuthEnvironment(t *testing.T) {
	compose := readProductionCompose(t)
	neo4jService := productionComposeService(t, compose, "neo4j")

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

func TestProductionHostDatabaseServicesMapDockerHostGateway(t *testing.T) {
	compose := readProductionCompose(t)
	if !strings.Contains(compose, "x-host-gateway: &host-gateway\n  - \"host.docker.internal:host-gateway\"") {
		t.Fatal("production compose must define one shared host.docker.internal host-gateway mapping")
	}

	for _, service := range []string{"lce", "neo4j-projector", "neo4j-algorithm-worker", "relay", "frontend"} {
		serviceBlock := productionComposeService(t, compose, service)
		if !strings.Contains(serviceBlock, "extra_hosts: *host-gateway") {
			t.Errorf("%s must map host.docker.internal so it can reach host-managed dependencies", service)
		}
	}
}

func TestProductionDeployDetectsImmediateContainerCrashLoops(t *testing.T) {
	raw, err := os.ReadFile("deploy/deploy.sh")
	if err != nil {
		t.Fatalf("read production deploy script: %v", err)
	}
	deploy := strings.ReplaceAll(string(raw), "\r\n", "\n")

	for _, required := range []string{
		"DEPLOY_STABILITY_WAIT_SECONDS:-15",
		"docker inspect --format '{{.RestartCount}}'",
		"service failed post-deploy stability check",
		`verify_compose_services_stable "${deployment_services[@]}"`,
	} {
		if !strings.Contains(deploy, required) {
			t.Errorf("production deploy script is missing crash-loop guard %q", required)
		}
	}

	up := strings.LastIndex(deploy, `docker compose "${compose_env_args[@]}" "${compose_profile_args[@]}" up`)
	verify := strings.LastIndex(deploy, `verify_compose_services_stable "${deployment_services[@]}"`)
	prune := strings.LastIndex(deploy, "prune_docker_resources")
	if up < 0 || verify <= up || prune <= verify {
		t.Fatal("post-deploy stability verification must run after Compose up and before resource cleanup")
	}
}
