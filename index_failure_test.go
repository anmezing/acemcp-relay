package main

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func TestValidateReportedIndexFailureDiagnosticRequiresExactAtomicTuple(t *testing.T) {
	valid, provided, err := validateReportedIndexFailureDiagnostic(
		"provider_invalid_request", "provider", "contact_admin",
	)
	if err != nil || !provided || valid != indexFailureDiagnosticsByCode["provider_invalid_request"] {
		t.Fatalf("valid provider diagnostic rejected: diagnostic=%+v provided=%v err=%v", valid, provided, err)
	}

	for _, tc := range []struct {
		name     string
		code     string
		origin   string
		recovery string
		contains string
	}{
		{"partial", "provider_invalid_request", "", "", "provided together"},
		{"unknown code", "future_code", "provider", "contact_admin", "unsupported"},
		{"mismatched owner", "provider_invalid_request", "network", "contact_admin", "must use"},
		{"mismatched recovery", "provider_invalid_request", "provider", "retry_later", "must use"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := validateReportedIndexFailureDiagnostic(tc.code, tc.origin, tc.recovery)
			if err == nil || !strings.Contains(err.Error(), tc.contains) {
				t.Fatalf("expected %q validation error, got %v", tc.contains, err)
			}
		})
	}
}

func TestResolveReportedIndexFailureDiagnosticClassifiesLegacy20015Once(t *testing.T) {
	diagnostic, err := resolveReportedIndexFailureDiagnostic(
		indexJobStatusFailed,
		"Embedding API 错误: The parameter is invalid. Please check again. [20015]",
		"", "", "",
	)
	if err != nil {
		t.Fatal(err)
	}
	if want := indexFailureDiagnosticsByCode["provider_invalid_request"]; diagnostic != want {
		t.Fatalf("legacy 20015 diagnostic = %+v, want %+v", diagnostic, want)
	}
}

func TestMarkIndexJobFailedPersistsStructuredDiagnostic(t *testing.T) {
	diagnostic := indexFailureDiagnosticsByCode["provider_invalid_request"]
	withMockTx(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectExec("UPDATE index_jobs").
			WithArgs(
				indexJobStatusFailed,
				"Embedding API invalid request [20015]",
				diagnostic.Code,
				diagnostic.Origin,
				diagnostic.Recovery,
				"job-1",
				"user-1",
				indexJobStatusRunning,
			).
			WillReturnResult(sqlmock.NewResult(0, 1))
	}, func(tx *sql.Tx) {
		updated, err := markIndexJobFailed(
			context.Background(), tx, "user-1", "job-1",
			"Embedding API invalid request [20015]", diagnostic,
		)
		if err != nil {
			t.Fatal(err)
		}
		if !updated {
			t.Fatal("running job must be marked failed")
		}
	})
}
