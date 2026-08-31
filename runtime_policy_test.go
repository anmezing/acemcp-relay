package main

import (
	"testing"
	"time"
)

func restoreRuntimePolicyAfterTest(t *testing.T) {
	t.Helper()
	t.Cleanup(loadRuntimePolicy)
}

func TestParsePositiveInt(t *testing.T) {
	if got, err := parsePositiveInt(" 42 ", 7); err != nil || got != 42 {
		t.Fatalf("parsePositiveInt valid = %d, %v", got, err)
	}
	if got, err := parsePositiveInt("0", 7); err == nil || got != 7 {
		t.Fatalf("parsePositiveInt invalid = %d, %v", got, err)
	}
	if got, err := parsePositiveInt("", 7); err != nil || got != 7 {
		t.Fatalf("parsePositiveInt fallback = %d, %v", got, err)
	}
}

func TestParseNonNegativeIntegers(t *testing.T) {
	if got, err := parseNonNegativeInt("0", 7); err != nil || got != 0 {
		t.Fatalf("parseNonNegativeInt zero = %d, %v", got, err)
	}
	if got, err := parseNonNegativeInt("-1", 7); err == nil || got != 7 {
		t.Fatalf("parseNonNegativeInt invalid = %d, %v", got, err)
	}
	if got, err := parseNonNegativeInt64("2147483648", 7); err != nil || got != 2147483648 {
		t.Fatalf("parseNonNegativeInt64 valid = %d, %v", got, err)
	}
}

func TestParsePositiveDuration(t *testing.T) {
	if got, err := parsePositiveDuration("2m30s", time.Second); err != nil || got != 150*time.Second {
		t.Fatalf("parsePositiveDuration valid = %s, %v", got, err)
	}
	if got, err := parsePositiveDuration("-1s", time.Second); err == nil || got != time.Second {
		t.Fatalf("parsePositiveDuration invalid = %s, %v", got, err)
	}
}

func TestLoadRuntimePolicyClampsFileLimitToBatchLimit(t *testing.T) {
	restoreRuntimePolicyAfterTest(t)
	t.Setenv("INDEX_MAX_FILE_BYTES", "2048")
	t.Setenv("INDEX_MAX_BATCH_BYTES", "1024")
	loadRuntimePolicy()
	if maxIndexFileBytes != 1024 || maxIndexBatchBytes != 1024 {
		t.Fatalf("limits = file %d, batch %d", maxIndexFileBytes, maxIndexBatchBytes)
	}
}

func TestLoadRuntimePolicyEnforcesRelationships(t *testing.T) {
	restoreRuntimePolicyAfterTest(t)
	t.Setenv("MCP_MAX_SESSIONS", "8")
	t.Setenv("MCP_MAX_SESSIONS_PER_USER", "12")
	t.Setenv("DB_MAX_OPEN_CONNS", "6")
	t.Setenv("DB_MAX_IDLE_CONNS", "9")
	t.Setenv("INDEX_OPERATION_LEASE_DURATION", "20s")
	t.Setenv("INDEX_OPERATION_RENEW_INTERVAL", "30s")

	loadRuntimePolicy()
	if mcpMaxSessionsPerUser != 8 {
		t.Fatalf("mcpMaxSessionsPerUser = %d, want 8", mcpMaxSessionsPerUser)
	}
	if dbMaxIdleConns != 6 {
		t.Fatalf("dbMaxIdleConns = %d, want 6", dbMaxIdleConns)
	}
	if indexOperationRenewInterval != 10*time.Second {
		t.Fatalf("indexOperationRenewInterval = %s, want 10s", indexOperationRenewInterval)
	}

}

func TestLoadRuntimePolicyOwnsServiceAddresses(t *testing.T) {
	restoreRuntimePolicyAfterTest(t)
	t.Setenv("SERVER_ADDR", " 0.0.0.0:4310 ")
	t.Setenv("LCE_MCP_URL", " https://lce.example.test/mcp/ ")
	t.Setenv("PPROF_ADDR", " 127.0.0.1:7411 ")
	t.Setenv("DB_HOST", " db.example.test ")
	t.Setenv("DB_PORT", "6432")
	t.Setenv("REDIS_HOST", " cache.example.test ")
	t.Setenv("REDIS_PORT", "6380")
	t.Setenv("DEFAULT_DAILY_REQUEST_LIMIT", "0")
	t.Setenv("DAILY_INDEX_BYTES_LIMIT", "4294967296")
	t.Setenv("DEBUG_CAPTURE_MAX_BYTES", "8192")
	t.Setenv("INDEX_MAX_FAILURE_BYTES", "4096")

	loadRuntimePolicy()
	if serverAddr != "0.0.0.0:4310" {
		t.Fatalf("serverAddr = %q", serverAddr)
	}
	if lceMCPURL != "https://lce.example.test/mcp" {
		t.Fatalf("lceMCPURL = %q", lceMCPURL)
	}
	if pprofAddr != "127.0.0.1:7411" {
		t.Fatalf("pprofAddr = %q", pprofAddr)
	}
	if dbHost != "db.example.test" || dbPort != 6432 {
		t.Fatalf("database endpoint = %s:%d", dbHost, dbPort)
	}
	if redisHost != "cache.example.test" || redisPort != 6380 {
		t.Fatalf("redis endpoint = %s:%d", redisHost, redisPort)
	}
	if defaultDailyRequestLimit != 0 || dailyIndexBytesLimit != 4294967296 {
		t.Fatalf("quota defaults = requests %d, bytes %d", defaultDailyRequestLimit, dailyIndexBytesLimit)
	}
	if debugCaptureMaxBytes != 8192 || maxIndexFailureBytes != 4096 {
		t.Fatalf("diagnostic limits = capture %d, failure %d", debugCaptureMaxBytes, maxIndexFailureBytes)
	}

}
