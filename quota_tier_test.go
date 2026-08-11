package main

import (
	"context"
	"testing"

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
	withTierQuotaEnv(t, 0, 0, 100, 300)

	if allowed, _, limit := chargeIndexBytes("free-user", "", "free", 100); !allowed || limit != 100 {
		t.Fatalf("free charge at limit: allowed=%v limit=%d", allowed, limit)
	}
	if allowed, _, _ := chargeIndexBytes("free-user", "", "free", 1); allowed {
		t.Fatal("free user over 100 bytes must be rejected")
	}

	if allowed, _, limit := chargeIndexBytes("pro-user", "", "pro", 300); !allowed || limit != 300 {
		t.Fatalf("pro charge at limit: allowed=%v limit=%d", allowed, limit)
	}
	if allowed, _, _ := chargeIndexBytes("pro-user", "", "pro", 1); allowed {
		t.Fatal("pro user over 300 bytes must be rejected")
	}
}

func TestChargeIndexBytesUnknownTierUsesFreeLimit(t *testing.T) {
	withTierQuotaEnv(t, 0, 0, 100, 0)

	if allowed, _, _ := chargeIndexBytes("weird-user", "", "platinum", 101); allowed {
		t.Fatal("unknown tier must use the free byte limit, not pro unlimited")
	}
}

func TestChargeIndexBytesProZeroMeansUnlimited(t *testing.T) {
	withTierQuotaEnv(t, 0, 0, 100, 0)

	if allowed, _, limit := chargeIndexBytes("pro-user", "", "pro", 1<<30); !allowed || limit != 0 {
		t.Fatalf("pro with limit 0 must pass any size, allowed=%v limit=%d", allowed, limit)
	}
}
