package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestOperationalPolicyIsNotReintroducedOutsideRuntimePolicy(t *testing.T) {
	forbidden := []string{
		"LeaderboardUpdateInterval",
		"HealthCheckInterval",
		"INTERVAL '15 minutes'",
		"48*time.Hour",
		"clearIndexCooldownSeconds",
		"indexOperationLeaseDuration =",
		"modelConfigCacheTTL =",
		"quotaCacheTTL =",
		"deleteRootMinInterval =",
		"http://127.0.0.1:3000/mcp",
		"127.0.0.1:3009",
		"127.0.0.1:6060",
		"Add(24 * time.Hour)",
		"len(failure) > 2000",
		"getEnv(\"DB_HOST\", \"localhost\")",
		"getEnvInt(\"DB_PORT\", 5432)",
	}
	entries, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range entries {
		if path == "runtime_policy.go" || strings.HasSuffix(path, "_test.go") {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		text := string(data)
		for _, needle := range forbidden {
			if strings.Contains(text, needle) {
				t.Errorf("%s reintroduces operational hardcoding %q", path, needle)
			}
		}
	}
}

func TestUserDocumentationDoesNotBindRuntimePolicyToFixedHostOrIntervals(t *testing.T) {
	data, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, needle := range []string{"IDE Agent", "远端服务无法读取 IDE", "npx -y", "npm 客户端", "每 30 分钟更新", "每 2 分钟调用", "最多保留 30 秒"} {
		if strings.Contains(text, needle) {
			t.Errorf("README.md contains fixed user-facing guidance %q", needle)
		}
	}
}

func TestTenantAssertionTTLIsNamedSecurityInvariant(t *testing.T) {
	implementation, err := os.ReadFile("tenant_assertion.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(implementation)
	if !strings.Contains(text, "tenantAssertionTTL = 30 * time.Second") {
		t.Fatal("tenant assertion replay window must remain a named, reviewable security invariant")
	}
	if strings.Count(text, "30 * time.Second") != 1 {
		t.Fatal("tenant assertion replay window must not be duplicated")
	}

	documentation, err := os.ReadFile(filepath.Join("docs", "hardcoding-policy.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(documentation), "tenantAssertionTTL") {
		t.Fatal("hardcoding policy must classify the tenant assertion TTL as a security invariant")
	}
}

func TestDeploymentPolicyIsTemplateDrivenAndDiscoverable(t *testing.T) {
	nginxTemplate, err := os.ReadFile(filepath.Join("deploy", "nginx.conf.template"))
	if err != nil {
		t.Fatal(err)
	}
	nginxText := string(nginxTemplate)
	for _, forbidden := range []string{"lcebot.com", "/etc/ssl/cloudflare", "http://127.0.0.1:3009", "http://127.0.0.1:3001"} {
		if strings.Contains(nginxText, forbidden) {
			t.Errorf("nginx template contains deployment-specific value %q", forbidden)
		}
	}
	for _, required := range []string{"${PUBLIC_SERVER_NAMES}", "${TLS_CERTIFICATE_PATH}", "${NGINX_MCP_UPSTREAM}", "${NGINX_FRONTEND_UPSTREAM}"} {
		if !strings.Contains(nginxText, required) {
			t.Errorf("nginx template is missing %s", required)
		}
	}

	compose, err := os.ReadFile(filepath.Join("deploy", "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	composeText := string(compose)
	if strings.Contains(composeText, "https://lcebot.com") {
		t.Fatal("docker-compose must not fall back to a deployment-specific public origin")
	}
	if !strings.Contains(composeText, "NEXT_PUBLIC_SITE_URL:-${BETTER_AUTH_URL:?") {
		t.Fatal("public site URL must inherit the explicit Better Auth origin for old deployments")
	}
	if strings.Contains(composeText, "NEXT_PUBLIC_LCE_CLIENT_PACKAGE_RUNNER:?") {
		t.Fatal("an optional client launcher must not prevent the service from deploying")
	}

	envExample, err := os.ReadFile(filepath.Join("deploy", ".env.example"))
	if err != nil {
		t.Fatal(err)
	}
	envText := string(envExample)
	defined := map[string]bool{}
	for _, match := range regexp.MustCompile(`(?m)^([A-Z][A-Z0-9_]*)=`).FindAllStringSubmatch(envText, -1) {
		defined[match[1]] = true
	}
	for _, match := range regexp.MustCompile(`\$\{([A-Z][A-Z0-9_]*)`).FindAllStringSubmatch(composeText, -1) {
		if !defined[match[1]] {
			t.Errorf("deploy/.env.example does not document compose variable %s", match[1])
		}
	}

	deployScript, err := os.ReadFile(filepath.Join("deploy", "deploy.sh"))
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"DEPLOY_LCE_DIR", "DEPLOY_RELAY_DIR", "DEPLOY_FRONTEND_DIR", "docker compose config"} {
		if !strings.Contains(string(deployScript), required) {
			t.Errorf("deploy script does not expose or validate %s", required)
		}
	}

	tuneHost, err := os.ReadFile(filepath.Join("deploy", "tune-host.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(tuneHost), "conflicting server name") {
		t.Fatal("host tuning must reject duplicate active Nginx virtual hosts")
	}
}
