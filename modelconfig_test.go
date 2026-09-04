package main

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestLoadModelConfigFailsClosedOnDatabaseError(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()
	redisServer := miniredis.RunT(t)

	previousDB := db
	previousRedis := redisClient
	previousKey := modelConfigKey
	db = mockDB
	redisClient = redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	modelConfigKey = make([]byte, 32)
	defer func() {
		db = previousDB
		redisClient = previousRedis
		modelConfigKey = previousKey
	}()

	mock.ExpectQuery("SELECT config_enc").
		WithArgs("user-1").
		WillReturnError(errors.New("database unavailable"))

	_, err = loadRerankModelConfigArg(context.Background(), "user-1")
	if err == nil {
		t.Fatal("database failure must not be treated as platform-default model configuration")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestGetUserModelConfigIgnoresStaleRedisValueAfterReset(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()
	redisServer := miniredis.RunT(t)

	previousDB := db
	previousRedis := redisClient
	db = mockDB
	redisClient = redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	defer func() {
		db = previousDB
		redisClient = previousRedis
	}()

	if err := redisClient.Set(context.Background(), "modelcfg:user-1", `{"enc":"stale-secret"}`, 0).Err(); err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery("SELECT config_enc").
		WithArgs("user-1").
		WillReturnRows(sqlmock.NewRows([]string{"config_enc"}))

	row, err := getUserModelConfigRow(context.Background(), "user-1")
	if err != nil {
		t.Fatal(err)
	}
	if row != nil {
		t.Fatalf("deleted database row must override stale Redis data, got %+v", row)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestGetUserModelConfigIgnoresStaleNoRowCacheAfterSave(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()
	redisServer := miniredis.RunT(t)

	previousDB := db
	previousRedis := redisClient
	db = mockDB
	redisClient = redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	defer func() {
		db = previousDB
		redisClient = previousRedis
	}()

	if err := redisClient.Set(context.Background(), "modelcfg:user-1", "0", 0).Err(); err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery("SELECT config_enc").
		WithArgs("user-1").
		WillReturnRows(sqlmock.NewRows([]string{"config_enc"}).AddRow("fresh-secret"))

	row, err := getUserModelConfigRow(context.Background(), "user-1")
	if err != nil {
		t.Fatal(err)
	}
	if row == nil || row.Enc != "fresh-secret" {
		t.Fatalf("database row must override stale no-row cache, got %+v", row)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
