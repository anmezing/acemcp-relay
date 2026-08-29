package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func TestParsePlatformModelConfigPatch(t *testing.T) {
	tests := []struct {
		name        string
		section     string
		body        string
		wantErr     string
		wantSection string
	}{
		{name: "embeddings", section: "embeddings", body: `{"embeddings":{"provider":"voyage"}}`, wantSection: "embeddings"},
		{name: "rerank", section: "rerank", body: `{"rerank":{"provider":"voyage"}}`, wantSection: "rerank"},
		{name: "prompt enhancer", section: "promptEnhancer", body: `{"promptEnhancer":{"enabled":false}}`, wantSection: "promptEnhancer"},
		{name: "missing section", body: `{"embeddings":{}}`, wantErr: "section must be"},
		{name: "unknown section", section: "chat", body: `{"chat":{}}`, wantErr: "section must be"},
		{name: "multiple sections", section: "embeddings", body: `{"embeddings":{},"rerank":{}}`, wantErr: "exactly one"},
		{name: "mismatched section", section: "rerank", body: `{"embeddings":{}}`, wantErr: "must match"},
		{name: "null section", section: "rerank", body: `{"rerank":null}`, wantErr: "must be a JSON object"},
		{name: "array envelope", section: "rerank", body: `[]`, wantErr: "config must be a JSON object"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := parsePlatformModelConfigPatch(test.section, json.RawMessage(test.body))
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("parsePlatformModelConfigPatch() error = %v, want substring %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parsePlatformModelConfigPatch() error = %v", err)
			}
			if len(got) != 1 || got[test.wantSection] == nil {
				t.Fatalf("parsePlatformModelConfigPatch() = %#v, want only %q", got, test.wantSection)
			}
		})
	}
}

func TestClearAllRelayIndexStateCommitsAllTables(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	previousDB := db
	db = mockDB
	defer func() { db = previousDB }()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM index_operation_leases").
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec("DELETE FROM index_jobs").
		WillReturnResult(sqlmock.NewResult(0, 3))
	mock.ExpectExec("DELETE FROM indexed_files").
		WillReturnResult(sqlmock.NewResult(0, 5))
	mock.ExpectExec("DELETE FROM index_workspaces").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	got, err := clearAllRelayIndexState(context.Background())
	if err != nil {
		t.Fatalf("clearAllRelayIndexState() error = %v", err)
	}
	want := clearedRelayIndexes{Jobs: 3, Files: 5, Workspaces: 1, Leases: 2}
	if got != want {
		t.Fatalf("clearAllRelayIndexState() = %+v, want %+v", got, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestClearAllRelayIndexStateRollsBackOnDeleteFailure(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	previousDB := db
	db = mockDB
	defer func() { db = previousDB }()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM index_operation_leases").
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec("DELETE FROM index_jobs").
		WillReturnError(errors.New("jobs table unavailable"))
	mock.ExpectRollback()

	_, err = clearAllRelayIndexState(context.Background())
	if err == nil || err.Error() != "jobs table unavailable" {
		t.Fatalf("clearAllRelayIndexState() error = %v, want jobs table unavailable", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}
