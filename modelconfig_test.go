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

	mock.ExpectQuery("SELECT config_enc, fingerprint, applied_fingerprint").
		WithArgs("user-1").
		WillReturnError(errors.New("database unavailable"))

	_, _, err = loadModelConfigArg(context.Background(), "user-1")
	if err == nil {
		t.Fatal("database failure must not be treated as platform-default model configuration")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
