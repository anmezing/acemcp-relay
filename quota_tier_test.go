package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// ── tier 分层配额 ─────────────────────────────────────────────────────────
//
// 契约（与前端并行开发，已定死）：api_keys.tier ∈ {'free','pro'}，未知值按
// free 处理（fail-safe），free 用 DEFAULT_*，pro 用 PRO_*（0 = 不限）。

// withTierQuotaEnv 临时设置分层限额全局变量并接管 redis/db。
func withTierQuotaEnv(t *testing.T, freeReq, proReq int, freeBytes, proBytes int64) (sqlmock.Sqlmock, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	previousRedis := redisClient
	redisClient = redis.NewClient(&redis.Options{Addr: server.Addr()})

	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	previousDB := db
	db = mockDB

	prevFreeReq, prevProReq := defaultDailyRequestLimit, proDailyRequestLimit
	prevFreeBytes, prevProBytes := dailyIndexBytesLimit, proDailyIndexBytesLimit
	defaultDailyRequestLimit, proDailyRequestLimit = freeReq, proReq
	dailyIndexBytesLimit, proDailyIndexBytesLimit = freeBytes, proBytes

	t.Cleanup(func() {
		defaultDailyRequestLimit, proDailyRequestLimit = prevFreeReq, prevProReq
		dailyIndexBytesLimit, proDailyIndexBytesLimit = prevFreeBytes, prevProBytes
		_ = redisClient.Close()
		redisClient = previousRedis
		db = previousDB
		mockDB.Close()
	})
	return mock, server
}

func expectNoQuotaOverride(mock sqlmock.Sqlmock, userID string) {
	mock.ExpectQuery("SELECT daily_limit FROM user_quotas").WithArgs(userID).
		WillReturnRows(sqlmock.NewRows([]string{"daily_limit"}))
	expectNoActivePlan(mock, userID)
}

func expectNoIndexBytesOverride(mock sqlmock.Sqlmock, userID string) {
	mock.ExpectQuery("SELECT daily_index_bytes_limit FROM user_quotas").WithArgs(userID).
		WillReturnRows(sqlmock.NewRows([]string{"daily_index_bytes_limit"}))
	expectNoActivePlan(mock, userID)
}

func expectNoActivePlan(mock sqlmock.Sqlmock, userID string) {
	mock.ExpectQuery("SELECT daily_request_limit, daily_index_bytes_limit, expires_at").
		WithArgs(userID).
		WillReturnRows(sqlmock.NewRows([]string{"daily_request_limit", "daily_index_bytes_limit", "expires_at"}))
}

func TestNormalizeTierUnknownValuesFallBackToFree(t *testing.T) {
	cases := map[string]string{
		"free": tierFree, "pro": tierPro, "": tierFree,
		"enterprise": tierFree, "PRO": tierFree, "  pro  ": tierPro,
	}
	for input, want := range cases {
		if got := normalizeTier(input); got != want {
			t.Errorf("normalizeTier(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestUserTierContextRoundTrip(t *testing.T) {
	if got := userTierFromContext(nil); got != tierFree {
		t.Fatalf("nil context tier = %q, want free", got)
	}
	if got := userTierFromContext(context.Background()); got != tierFree {
		t.Fatalf("empty context tier = %q, want free", got)
	}
	ctx := withUserTier(context.Background(), "pro")
	if got := userTierFromContext(ctx); got != tierPro {
		t.Fatalf("round-trip tier = %q, want pro", got)
	}
	// 未知 tier 进 context 也被归一为 free
	if got := userTierFromContext(withUserTier(context.Background(), "vip")); got != tierFree {
		t.Fatalf("unknown tier from context = %q, want free", got)
	}
}

func TestCheckRequestQuotaFreeAndProUseTheirOwnLimits(t *testing.T) {
	mock, _ := withTierQuotaEnv(t, 2, 5, 0, 0)
	expectNoQuotaOverride(mock, "free-user")
	expectNoQuotaOverride(mock, "pro-user")

	for i := 0; i < 2; i++ {
		if ok, limit := checkRequestQuota("free-user", "", "free"); !ok || limit != 2 {
			t.Fatalf("free request %d: ok=%v limit=%d, want ok limit=2", i, ok, limit)
		}
	}
	if ok, _ := checkRequestQuota("free-user", "", "free"); ok {
		t.Fatal("free user request 3 should exceed the free limit of 2")
	}

	for i := 0; i < 5; i++ {
		if ok, limit := checkRequestQuota("pro-user", "", "pro"); !ok || limit != 5 {
			t.Fatalf("pro request %d: ok=%v limit=%d, want ok limit=5", i, ok, limit)
		}
	}
	if ok, _ := checkRequestQuota("pro-user", "", "pro"); ok {
		t.Fatal("pro user request 6 should exceed the pro limit of 5")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCheckRequestQuotaUnknownTierUsesFreeLimit(t *testing.T) {
	mock, _ := withTierQuotaEnv(t, 1, 100, 0, 0)
	expectNoQuotaOverride(mock, "weird-user")

	if ok, limit := checkRequestQuota("weird-user", "", "platinum"); !ok || limit != 1 {
		t.Fatalf("first request: ok=%v limit=%d, want ok limit=1 (free)", ok, limit)
	}
	if ok, _ := checkRequestQuota("weird-user", "", "platinum"); ok {
		t.Fatal("unknown tier must be limited like free, not pro")
	}
}

func TestCheckRequestQuotaProZeroMeansUnlimited(t *testing.T) {
	mock, _ := withTierQuotaEnv(t, 1, 0, 0, 0)
	expectNoQuotaOverride(mock, "pro-user")

	for i := 0; i < 10; i++ {
		if ok, limit := checkRequestQuota("pro-user", "", "pro"); !ok || limit > 0 {
			t.Fatalf("request %d: ok=%v limit=%d, want unlimited pass", i, ok, limit)
		}
	}
}

func TestGetUserDailyLimitPerUserOverrideBeatsTierDefault(t *testing.T) {
	mock, _ := withTierQuotaEnv(t, 10, 1000, 0, 0)
	mock.ExpectQuery("SELECT daily_limit FROM user_quotas").WithArgs("override-user").
		WillReturnRows(sqlmock.NewRows([]string{"daily_limit"}).AddRow(7))

	if limit := getUserDailyLimit("override-user", "pro"); limit != 7 {
		t.Fatalf("limit = %d, want per-user override 7 over pro default 1000", limit)
	}
	// 第二次读走 Redis 缓存，不再查 DB
	if limit := getUserDailyLimit("override-user", "pro"); limit != 7 {
		t.Fatalf("cached limit = %d, want 7", limit)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("second lookup must hit the cache: %v", err)
	}
}

func TestChargeIndexBytesFreeAndProUseTheirOwnLimits(t *testing.T) {
	mock, _ := withTierQuotaEnv(t, 0, 0, 100, 300)
	expectNoIndexBytesOverride(mock, "free-user")
	expectNoIndexBytesOverride(mock, "pro-user")

	if decision := chargeIndexBytes("free-user", "", "free", 100); !decision.Allowed || decision.Limit != 100 {
		t.Fatalf("free charge at limit: allowed=%v limit=%d", decision.Allowed, decision.Limit)
	}
	if decision := chargeIndexBytes("free-user", "", "free", 1); decision.Allowed {
		t.Fatal("free user over 100 bytes must be rejected")
	}

	if decision := chargeIndexBytes("pro-user", "", "pro", 300); !decision.Allowed || decision.Limit != 300 {
		t.Fatalf("pro charge at limit: allowed=%v limit=%d", decision.Allowed, decision.Limit)
	}
	if decision := chargeIndexBytes("pro-user", "", "pro", 1); decision.Allowed {
		t.Fatal("pro user over 300 bytes must be rejected")
	}
}

func TestChargeIndexBytesUnknownTierUsesFreeLimit(t *testing.T) {
	mock, _ := withTierQuotaEnv(t, 0, 0, 100, 0)
	expectNoIndexBytesOverride(mock, "weird-user")

	if decision := chargeIndexBytes("weird-user", "", "platinum", 101); decision.Allowed {
		t.Fatal("unknown tier must use the free byte limit, not pro unlimited")
	}
}

func TestChargeIndexBytesProZeroMeansUnlimited(t *testing.T) {
	mock, _ := withTierQuotaEnv(t, 0, 0, 100, 0)
	expectNoIndexBytesOverride(mock, "pro-user")

	if decision := chargeIndexBytes("pro-user", "", "pro", 1<<30); !decision.Allowed || decision.Limit != 0 {
		t.Fatalf("pro with limit 0 must pass any size, allowed=%v limit=%d", decision.Allowed, decision.Limit)
	}
}

func TestUserIndexBytesOverrideBeatsTierDefault(t *testing.T) {
	mock, _ := withTierQuotaEnv(t, 0, 0, 100, 300)
	mock.ExpectQuery("SELECT daily_index_bytes_limit FROM user_quotas").
		WithArgs("override-user").
		WillReturnRows(sqlmock.NewRows([]string{"daily_index_bytes_limit"}).AddRow(125))

	if decision := chargeIndexBytes("override-user", "", "pro", 125); !decision.Allowed || decision.Limit != 125 {
		t.Fatalf("charge at override: allowed=%v limit=%d, want allowed limit=125", decision.Allowed, decision.Limit)
	}
	if decision := chargeIndexBytes("override-user", "", "pro", 1); decision.Allowed || decision.Limit != 125 {
		t.Fatalf("charge over override: allowed=%v limit=%d, want rejected limit=125", decision.Allowed, decision.Limit)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestNullUserIndexBytesOverrideInheritsTier(t *testing.T) {
	mock, _ := withTierQuotaEnv(t, 0, 0, 100, 300)
	mock.ExpectQuery("SELECT daily_index_bytes_limit FROM user_quotas").
		WithArgs("inherit-user").
		WillReturnRows(sqlmock.NewRows([]string{"daily_index_bytes_limit"}).AddRow(nil))
	expectNoActivePlan(mock, "inherit-user")

	if decision := chargeIndexBytes("inherit-user", "", "pro", 300); !decision.Allowed || decision.Limit != 300 {
		t.Fatalf("null override: allowed=%v limit=%d, want pro default 300", decision.Allowed, decision.Limit)
	}
}

func TestActivePlanProvidesBothPersonalQuotaDimensions(t *testing.T) {
	mock, _ := withTierQuotaEnv(t, 10, 20, 100, 200)
	mock.ExpectQuery("SELECT daily_limit FROM user_quotas").WithArgs("paid-user").
		WillReturnRows(sqlmock.NewRows([]string{"daily_limit"}))
	expiresAt := time.Now().Add(time.Hour)
	mock.ExpectQuery("SELECT daily_request_limit, daily_index_bytes_limit, expires_at").
		WithArgs("paid-user").
		WillReturnRows(sqlmock.NewRows([]string{"daily_request_limit", "daily_index_bytes_limit", "expires_at"}).
			AddRow(77, 777, expiresAt))

	if got := getUserDailyLimit("paid-user", "free"); got != 77 {
		t.Fatalf("request limit = %d, want paid plan 77", got)
	}

	mock.ExpectQuery("SELECT daily_index_bytes_limit FROM user_quotas").WithArgs("paid-user").
		WillReturnRows(sqlmock.NewRows([]string{"daily_index_bytes_limit"}).AddRow(nil))
	mock.ExpectQuery("SELECT daily_request_limit, daily_index_bytes_limit, expires_at").
		WithArgs("paid-user").
		WillReturnRows(sqlmock.NewRows([]string{"daily_request_limit", "daily_index_bytes_limit", "expires_at"}).
			AddRow(77, 777, expiresAt))
	if got := getUserIndexBytesLimit("paid-user", "free"); got != 777 {
		t.Fatalf("index byte limit = %d, want paid plan 777", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestActivePlanQuotaCacheExpiresBeforeSubscription(t *testing.T) {
	mock, server := withTierQuotaEnv(t, 10, 20, 100, 200)
	mock.ExpectQuery("SELECT daily_limit FROM user_quotas").WithArgs("paid-user").
		WillReturnRows(sqlmock.NewRows([]string{"daily_limit"}))
	expiresAt := time.Now().Add(2 * time.Minute)
	mock.ExpectQuery("SELECT daily_request_limit, daily_index_bytes_limit, expires_at").
		WithArgs("paid-user").
		WillReturnRows(sqlmock.NewRows([]string{"daily_request_limit", "daily_index_bytes_limit", "expires_at"}).
			AddRow(77, 777, expiresAt))

	if got := getUserDailyLimit("paid-user", "free"); got != 77 {
		t.Fatalf("request limit = %d, want paid plan 77", got)
	}
	ttl := server.TTL("quota:limit:paid-user")
	if ttl <= 0 || ttl >= quotaCacheTTL {
		t.Fatalf("cache TTL = %v, want positive and shorter than %v", ttl, quotaCacheTTL)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestQuotaDatabaseErrorsAreNotCached(t *testing.T) {
	mock, server := withTierQuotaEnv(t, 10, 20, 100, 200)
	mock.ExpectQuery("SELECT daily_limit FROM user_quotas").WithArgs("db-error-user").
		WillReturnError(errors.New("database unavailable"))

	if got := getUserDailyLimit("db-error-user", "pro"); got != 20 {
		t.Fatalf("request limit = %d, want fail-open pro default 20", got)
	}
	if server.Exists("quota:limit:db-error-user") {
		t.Fatal("database errors must not populate the request quota cache")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestActivePlanLookupErrorsAreNotCached(t *testing.T) {
	mock, server := withTierQuotaEnv(t, 10, 20, 100, 200)
	mock.ExpectQuery("SELECT daily_index_bytes_limit FROM user_quotas").
		WithArgs("plan-error-user").
		WillReturnRows(sqlmock.NewRows([]string{"daily_index_bytes_limit"}))
	mock.ExpectQuery("SELECT daily_request_limit, daily_index_bytes_limit, expires_at").
		WithArgs("plan-error-user").
		WillReturnError(errors.New("database unavailable"))

	if got := getUserIndexBytesLimit("plan-error-user", "pro"); got != 200 {
		t.Fatalf("index byte limit = %d, want fail-open pro default 200", got)
	}
	if server.Exists("quota:limit:indexbytes:plan-error-user") {
		t.Fatal("active-plan lookup errors must not populate the quota cache")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
