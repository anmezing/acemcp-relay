package main

import (
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestLeaderboardAggregationQueryUsesOnlyConfiguredUsageTools(t *testing.T) {
	for _, path := range []string{LeaderboardRetrievalPath, LeaderboardEnhancePath} {
		if !strings.Contains(leaderboardAggregationQuery, path) {
			t.Fatalf("leaderboard query does not include %q", path)
		}
	}
	for _, path := range []string{
		"/mcp/tools/call/codebase_index",
		"/mcp/tools/call/codebase_index_status",
		"/mcp/tools/call/codebase_symbol_graph",
	} {
		if strings.Contains(leaderboardAggregationQuery, path) {
			t.Fatalf("leaderboard query unexpectedly includes %q", path)
		}
	}
}

func TestUpdateLeaderboardAggregatesConfiguredUsageTools(t *testing.T) {
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
	mock.ExpectExec("DELETE FROM leaderboard").
		WithArgs(sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
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

func TestUpdateLeaderboardClearsStaleSnapshotWhenNoUsageCallsExist(t *testing.T) {
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
		WillReturnRows(sqlmock.NewRows([]string{"user_id", "cnt"}))
	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM leaderboard").
		WithArgs(sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := updateLeaderboard(); err != nil {
		t.Fatalf("updateLeaderboard: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}
