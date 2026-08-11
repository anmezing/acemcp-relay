package main

import (
	"strings"
	"testing"
)

func TestFormatLogEvent(t *testing.T) {
	tests := []struct {
		name  string
		event string
		pairs []string
		want  string
	}{
		{
			name:  "plain values stay bare",
			event: "request_start",
			pairs: []string{"request_id", "abc-123", "user_id", "u1", "path", "/mcp", "method", "POST"},
			want:  `evt=request_start request_id=abc-123 user_id=u1 path=/mcp method=POST`,
		},
		{
			name:  "values with spaces or quotes are quoted",
			event: "lce_call_failed",
			pairs: []string{"tool", "codebase-retrieval", "error", `MCP tools/call returned 502: bad "gateway"`},
			want:  `evt=lce_call_failed tool=codebase-retrieval error="MCP tools/call returned 502: bad \"gateway\""`,
		},
		{
			name:  "empty value is quoted so the pair stays parseable",
			event: "panic_recovered",
			pairs: []string{"request_id", ""},
			want:  `evt=panic_recovered request_id=""`,
		},
		{
			name:  "value containing equals sign is quoted",
			event: "x",
			pairs: []string{"k", "a=b"},
			want:  `evt=x k="a=b"`,
		},
		{
			name:  "odd trailing key gets an empty value",
			event: "x",
			pairs: []string{"k"},
			want:  `evt=x k=""`,
		},
		{
			name:  "no pairs",
			event: "startup",
			want:  `evt=startup`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := formatLogEvent(tc.event, tc.pairs...); got != tc.want {
				t.Errorf("formatLogEvent = %q, want %q", got, tc.want)
			}
			if strings.Contains(tc.want, "\n") {
				t.Errorf("log lines must be single-line")
			}
		})
	}
}

func TestRequestIDContextRoundTrip(t *testing.T) {
	ctx := t.Context()
	if got := requestIDFromContext(ctx); got != "" {
		t.Errorf("empty context should yield empty request id, got %q", got)
	}
	ctx = withRequestID(ctx, "rid-42")
	if got := requestIDFromContext(ctx); got != "rid-42" {
		t.Errorf("requestIDFromContext = %q, want rid-42", got)
	}
	if got := requestIDFromContext(nil); got != "" {
		t.Errorf("nil context should yield empty request id, got %q", got)
	}
}
