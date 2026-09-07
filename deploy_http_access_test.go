package main

import (
	"os"
	"strings"
	"testing"

	"github.com/goccy/go-yaml"
)

type httpAccessCompose struct {
	Services map[string]struct {
		Environment map[string]string `yaml:"environment"`
	} `yaml:"services"`
}

func TestProductionLCEHeaderAllowlistsRequireExplicitOverlay(t *testing.T) {
	raw, err := os.ReadFile("deploy/docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	var compose httpAccessCompose
	if err := yaml.Unmarshal(raw, &compose); err != nil {
		t.Fatal(err)
	}
	if _, exists := compose.Services["lce"]; !exists {
		t.Fatal("base Compose must still define LCE")
	}
	lceEnv := compose.Services["lce"].Environment
	keys := []string{"LCE_HTTP_ALLOWED_HOSTS", "LCE_HTTP_ALLOWED_ORIGINS"}
	for _, key := range keys {
		if _, exists := lceEnv[key]; exists {
			t.Errorf("default deployment must not inject %s", key)
		}
	}

	raw, err = os.ReadFile("deploy/docker-compose.http-access.yml")
	if err != nil {
		t.Fatal(err)
	}
	var overlay httpAccessCompose
	if err := yaml.Unmarshal(raw, &overlay); err != nil {
		t.Fatal(err)
	}
	if len(overlay.Services) != 1 || len(overlay.Services["lce"].Environment) != len(keys) {
		t.Fatal("HTTP overlay must only set the two LCE header allowlists")
	}
	for _, key := range keys {
		value := overlay.Services["lce"].Environment[key]
		if !strings.HasPrefix(value, "${"+key+":?") || !strings.HasSuffix(value, "}") {
			t.Errorf("optional overlay must require a verified, non-empty %s", key)
		}
	}

	raw, err = os.ReadFile("deploy/.env.example")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		for _, key := range keys {
			if strings.HasPrefix(strings.TrimSpace(line), key+"=") {
				t.Errorf("env example must not activate %s by default", key)
			}
		}
	}
}
