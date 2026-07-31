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
