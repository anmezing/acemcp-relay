package main

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestMigrateAccessControlTablesRemovesRetiredDeviceTables(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	oldDB := db
	db = mockDB
	defer func() { db = oldDB }()

	mock.ExpectExec("DROP TABLE IF EXISTS device_alerts").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS banned_users").
		WillReturnResult(sqlmock.NewResult(0, 0))

	if err := migrateAccessControlTables(); err != nil {
		t.Fatalf("migrateAccessControlTables: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}
