package main

import (
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestUpdateLeaderboardAggregatesSuccessfulMCPToolCalls(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	oldDB := db
	db = mockDB
	defer func() { db = oldDB }()

	mock.ExpectQuery(regexp.QuoteMeta(leaderboardAggregationQuery)).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), LeaderboardTopN).
		WillReturnRows(sqlmock.NewRows([]string{"user_id", "cnt"}).AddRow("user-1", int64(7)))
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO leaderboard").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), 1, "user-1", int64(7)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := updateLeaderboard(); err != nil {
		t.Fatalf("updateLeaderboard: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}
