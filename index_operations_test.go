package main

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func expectIndexOperationLock(mock sqlmock.Sqlmock, userID string) {
	mock.ExpectBegin()
	mock.ExpectExec("pg_advisory_xact_lock").WithArgs(userID).WillReturnResult(sqlmock.NewResult(0, 1))
}

func TestTryAcquireIndexOperationInsertsAvailableSharedLease(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()
	previousDB := db
	db = mockDB
	defer func() { db = previousDB }()

	expectIndexOperationLock(mock, "user-1")
	mock.ExpectQuery("WITH expired AS").
		WithArgs("lease-1", "user-1", "job:job-1", indexOperationShared, "upload-batch", indexOperationLeaseDuration.Milliseconds(), indexOperationExclusive).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectCommit()

	acquired, err := tryAcquireIndexOperation(
		context.Background(), "user-1", "job:job-1", "upload-batch", "lease-1", indexOperationShared,
	)
	if err != nil {
		t.Fatalf("tryAcquireIndexOperation returned error: %v", err)
	}
	if !acquired {
		t.Fatal("available shared lease should be acquired")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestTryAcquireIndexOperationDoesNotInsertOnConflict(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()
	previousDB := db
	db = mockDB
	defer func() { db = previousDB }()

	expectIndexOperationLock(mock, "user-1")
	mock.ExpectQuery("WITH expired AS").
		WithArgs("lease-2", "user-1", "job:job-1", indexOperationShared, "upload-batch", indexOperationLeaseDuration.Milliseconds(), indexOperationExclusive).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectCommit()

	acquired, err := tryAcquireIndexOperation(
		context.Background(), "user-1", "job:job-1", "upload-batch", "lease-2", indexOperationShared,
	)
	if err != nil {
		t.Fatalf("tryAcquireIndexOperation returned error: %v", err)
	}
	if acquired {
		t.Fatal("conflicting shared lease must not be acquired")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestTryAcquireExclusiveIndexOperationConflictsWithAnyActiveLease(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()
	previousDB := db
	db = mockDB
	defer func() { db = previousDB }()

	expectIndexOperationLock(mock, "user-1")
	mock.ExpectQuery("WITH expired AS").
		WithArgs("lease-3", "user-1", "*", indexOperationExclusive, "clear-index", indexOperationLeaseDuration.Milliseconds(), indexOperationExclusive).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectCommit()

	acquired, err := tryAcquireIndexOperation(
		context.Background(), "user-1", "*", "clear-index", "lease-3", indexOperationExclusive,
	)
	if err != nil {
		t.Fatalf("tryAcquireIndexOperation returned error: %v", err)
	}
	if acquired {
		t.Fatal("exclusive lease must wait for every active user operation")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
