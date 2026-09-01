package main

import (
	"fmt"
	"strings"
)

type indexFailureDiagnostic struct {
	Code     string
	Origin   string
	Recovery string
}

var indexFailureDiagnosticsByCode = map[string]indexFailureDiagnostic{
	"client_disconnected":      {Code: "client_disconnected", Origin: "client", Recovery: "restart_client"},
	"heartbeat_timeout":        {Code: "heartbeat_timeout", Origin: "relay", Recovery: "restart_client"},
	"embedding_space_changed":  {Code: "embedding_space_changed", Origin: "remote_index", Recovery: "reset_root"},
	"upstream_bad_gateway":     {Code: "upstream_bad_gateway", Origin: "remote_index", Recovery: "retry_after_service_recovers"},
	"provider_billing":         {Code: "provider_billing", Origin: "provider", Recovery: "fix_provider_billing"},
	"provider_rate_limited":    {Code: "provider_rate_limited", Origin: "provider", Recovery: "retry_later"},
	"provider_invalid_request": {Code: "provider_invalid_request", Origin: "provider", Recovery: "contact_admin"},
	"repository_file_limit":    {Code: "repository_file_limit", Origin: "client", Recovery: "reduce_repository"},
	"repository_file_size_limit": {
		Code: "repository_file_size_limit", Origin: "client", Recovery: "reduce_repository",
	},
	"index_quota_exceeded":    {Code: "index_quota_exceeded", Origin: "relay", Recovery: "wait_for_quota_reset"},
	"provider_authentication": {Code: "provider_authentication", Origin: "provider", Recovery: "fix_credentials"},
	"network_unavailable":     {Code: "network_unavailable", Origin: "network", Recovery: "restart_client"},
	"index_failed":            {Code: "index_failed", Origin: "unknown", Recovery: "contact_admin"},
}

// validateReportedIndexFailureDiagnostic enforces the cloud-protocol all-or-none rule
// and accepts only the exact code/origin/recovery tuples shared by all three repositories.
func validateReportedIndexFailureDiagnostic(code, origin, recovery string) (indexFailureDiagnostic, bool, error) {
	code = strings.TrimSpace(code)
	origin = strings.TrimSpace(origin)
	recovery = strings.TrimSpace(recovery)
	provided := code != "" || origin != "" || recovery != ""
	if !provided {
		return indexFailureDiagnostic{}, false, nil
	}
	if code == "" || origin == "" || recovery == "" {
		return indexFailureDiagnostic{}, false, fmt.Errorf("error_code, error_origin, and recovery must be provided together")
	}
	expected, ok := indexFailureDiagnosticsByCode[code]
	if !ok {
		return indexFailureDiagnostic{}, false, fmt.Errorf("unsupported index failure error_code: %s", code)
	}
	if origin != expected.Origin || recovery != expected.Recovery {
		return indexFailureDiagnostic{}, false, fmt.Errorf(
			"index failure diagnostic %s must use error_origin=%s and recovery=%s",
			code, expected.Origin, expected.Recovery,
		)
	}
	return expected, true, nil
}

// resolveReportedIndexFailureDiagnostic preserves structured client diagnostics when
// present. Older clients omit them, so Relay classifies the text once and persists the
// result rather than making every reader repeatedly parse provider-specific wording.
func resolveReportedIndexFailureDiagnostic(status, detail, code, origin, recovery string) (indexFailureDiagnostic, error) {
	diagnostic, provided, err := validateReportedIndexFailureDiagnostic(code, origin, recovery)
	if err != nil {
		return indexFailureDiagnostic{}, err
	}
	if provided {
		return diagnostic, nil
	}
	return classifyIndexFailure(status, detail), nil
}

func persistedIndexFailureDiagnostic(code, origin, recovery string) (indexFailureDiagnostic, bool) {
	diagnostic, provided, err := validateReportedIndexFailureDiagnostic(code, origin, recovery)
	return diagnostic, provided && err == nil
}

// classifyIndexFailure is the compatibility path for old clients/jobs that only have
// raw error text. New clients should send the stable tuple defined by cloud-protocol.
func classifyIndexFailure(status, detail string) indexFailureDiagnostic {
	lower := strings.ToLower(strings.TrimSpace(detail))
	containsAny := func(values ...string) bool {
		for _, value := range values {
			if strings.Contains(lower, value) {
				return true
			}
		}
		return false
	}

	switch {
	case containsAny("client disconnected before first upload"):
		return indexFailureDiagnosticsByCode["client_disconnected"]
	case status == indexJobStatusTimedOut || containsAny("heartbeat timed out", "heartbeat timeout"):
		return indexFailureDiagnosticsByCode["heartbeat_timeout"]
	case containsAny("cloud embedding space changed", "embedding space changed", "clear the tenant root before starting a new index job"):
		return indexFailureDiagnosticsByCode["embedding_space_changed"]
	case containsAny("remote-index 502", "bad gateway", "cloudflare", "origin web server returned"):
		return indexFailureDiagnosticsByCode["upstream_bad_gateway"]
	case containsAny("payment required", "insufficient balance", "insufficient credit", "余额不足", "欠费", "billing", "remote-index 402"):
		return indexFailureDiagnosticsByCode["provider_billing"]
	case containsAny("too many requests", "rate limit", "rate-limit", "remote-index 429"):
		return indexFailureDiagnosticsByCode["provider_rate_limited"]
	case containsAny("the parameter is invalid", "[20015]", "valid utf-8", "special characters are properly escaped"):
		return indexFailureDiagnosticsByCode["provider_invalid_request"]
	case containsAny("manifest exceeds", "unreadable file list exceeds", "too many files", "file count limit", "maximum file count", "100,000 files", "100000 files", "文件数量", "文件数超过"):
		return indexFailureDiagnosticsByCode["repository_file_limit"]
	case containsAny("manifest file size is invalid", "file exceeds the", "byte limit", "file too large", "file size limit", "maximum file size", "512 kib", "524288", "文件大小超过", "单文件过大"):
		return indexFailureDiagnosticsByCode["repository_file_size_limit"]
	// Only Relay's platform index quota may use the wait-for-reset recovery.
	// Bare provider messages such as "embedding provider quota exceeded" must
	// not be mislabeled as Relay quota, otherwise clients wait for the wrong reset.
	case containsAny("daily index quota exceeded", "index quota exceeded", "index_quota_exceeded", "每日索引配额", "索引配额已用尽", "超出索引配额"):
		return indexFailureDiagnosticsByCode["index_quota_exceeded"]
	case containsAny("unauthorized", "invalid api key", "invalid token", "authentication failed", "remote-index 401", "remote-index 403"):
		return indexFailureDiagnosticsByCode["provider_authentication"]
	case containsAny("connection refused", "connection reset", "network is unreachable", "network down", "no such host", "i/o timeout", "context deadline exceeded", "unexpected eof", "socket hang up"):
		return indexFailureDiagnosticsByCode["network_unavailable"]
	default:
		return indexFailureDiagnosticsByCode["index_failed"]
	}
}
