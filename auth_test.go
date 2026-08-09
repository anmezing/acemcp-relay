package main

import (
	"crypto/md5"
	"encoding/hex"
	"net/http/httptest"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

func TestAuthenticateRequestRevalidatesRotatedKeysAgainstPostgres(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()
	previousDB := db
	db = mockDB
	defer func() { db = previousDB }()

	const token = "ace_rotated"
	hash := md5.Sum([]byte(token))
	keyID := hex.EncodeToString(hash[:])
	mock.ExpectQuery("SELECT user_id FROM api_keys").WithArgs(keyID).
		WillReturnRows(sqlmock.NewRows([]string{"user_id"}).AddRow("user-1"))
	mock.ExpectQuery("SELECT user_id FROM api_keys").WithArgs(keyID).
		WillReturnRows(sqlmock.NewRows([]string{"user_id"}))

	request := httptest.NewRequest("GET", "/mcp", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	context, _ := gin.CreateTestContext(httptest.NewRecorder())
	context.Request = request

	if userID, ok := authenticateRequest(context); !ok || userID != "user-1" {
		t.Fatalf("expected current key to authenticate, got user=%q ok=%v", userID, ok)
	}
	if userID, ok := authenticateRequest(context); ok || userID != "" {
		t.Fatalf("rotated key must stop authenticating immediately, got user=%q ok=%v", userID, ok)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCompareVersionsOrdersNumericSegmentsAndPrereleases(t *testing.T) {
	tests := []struct {
		a, b string
		want int
	}{
		{"1.2.3", "1.2.3", 0},
		{"1.2.3", "1.2.4", -1},
		{"1.2.4", "1.2.3", 1},
		{"1.10.0", "1.9.0", 1},
		{"2.0.0", "1.99.99", 1},
		{"1.2", "1.2.0", 0},
		// semver：预发布 < 同数字正式版
		{"1.2.3-beta", "1.2.3", -1},
		{"1.2.3", "1.2.3-beta", 1},
		// 数字段不同，预发布后缀不改变数字段的排序
		{"1.2.3", "1.2.4-rc.1", -1},
		{"1.2.4-rc.1", "1.2.3", 1},
		{"1.2.3-beta", "1.2.4", -1},
		// 双方都带预发布后缀：数字段相同视为相等（门禁只需要档位语义）
		{"1.2.3-alpha", "1.2.3-beta", 0},
	}
	for _, test := range tests {
		if got := compareVersions(test.a, test.b); got != test.want {
			t.Errorf("compareVersions(%q, %q) = %d, want %d", test.a, test.b, got, test.want)
		}
	}
}
