package main

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestMigrateQuotaTablesRunsOrgQuotaAlterSeparately(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	oldDB := db
	db = mockDB
	defer func() { db = oldDB }()

	mock.ExpectExec("CREATE TABLE IF NOT EXISTS user_quotas").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS org_quotas").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("ALTER TABLE org_quotas").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS org_member_quotas").
		WillReturnResult(sqlmock.NewResult(0, 0))

	if err := migrateQuotaTables(); err != nil {
		t.Fatalf("migrateQuotaTables: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}
