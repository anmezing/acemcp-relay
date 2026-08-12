package main

import (
	"context"
	"errors"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

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
