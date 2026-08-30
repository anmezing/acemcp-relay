package main

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func resetAuthCache() {
	authCacheMu.Lock()
	authCache = make(map[string]authCacheEntry)
	authCacheMu.Unlock()
}

// backdateAuthCacheEntry 把 token 对应的缓存条目改成已过期，模拟 TTL 到期。
func backdateAuthCacheEntry(t *testing.T, token string) {
	t.Helper()
	sum := sha256.Sum256([]byte(token))
	key := hex.EncodeToString(sum[:])
	authCacheMu.Lock()
	defer authCacheMu.Unlock()
	entry, found := authCache[key]
	if !found {
		t.Fatal("expected cache entry to exist")
	}
	entry.expiresAt = time.Now().Add(-time.Second)
	authCache[key] = entry
}

func newAuthTestContext(t *testing.T, token string) (*gin.Context, sqlmock.Sqlmock, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	previousDB := db
	db = mockDB
	resetAuthCache()

	request := httptest.NewRequest("GET", "/mcp", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	context, _ := gin.CreateTestContext(httptest.NewRecorder())
	context.Request = request
	return context, mock, func() {
		db = previousDB
		mockDB.Close()
		resetAuthCache()
	}
}

func tokenHashes(token string) (string, string) {
	md5Sum := md5.Sum([]byte(token))
	sha256Sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(md5Sum[:]), hex.EncodeToString(sha256Sum[:])
}

func TestAuthenticateRequestMD5KeyStillAuthenticates(t *testing.T) {
	const token = "lce_legacy_md5"
	context, mock, cleanup := newAuthTestContext(t, token)
	defer cleanup()

	md5Hex, sha256Hex := tokenHashes(token)
	mock.ExpectQuery("SELECT keys.user_id, COALESCE").WithArgs(md5Hex, sha256Hex).
		WillReturnRows(sqlmock.NewRows([]string{"user_id", "tier", "org_id"}).AddRow("user-md5", "free", ""))

	if id, ok := authenticateRequest(context); !ok || id.UserID != "user-md5" {
		t.Fatalf("expected MD5 key to authenticate, got user=%q ok=%v", id.UserID, ok)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAuthenticateRequestSHA256KeyAuthenticates(t *testing.T) {
	const token = "lce_new_sha256"
	context, mock, cleanup := newAuthTestContext(t, token)
	defer cleanup()

	// 双读：无论 id 存的是 MD5 还是 SHA-256，同一条 IN 查询都能命中
	md5Hex, sha256Hex := tokenHashes(token)
	mock.ExpectQuery("SELECT keys.user_id, COALESCE").WithArgs(md5Hex, sha256Hex).
		WillReturnRows(sqlmock.NewRows([]string{"user_id", "tier", "org_id"}).AddRow("user-sha", "free", ""))

	if id, ok := authenticateRequest(context); !ok || id.UserID != "user-sha" {
		t.Fatalf("expected SHA-256 key to authenticate, got user=%q ok=%v", id.UserID, ok)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAuthenticateRequestCacheHitSkipsDatabase(t *testing.T) {
	const token = "lce_cached"
	context, mock, cleanup := newAuthTestContext(t, token)
	defer cleanup()

	md5Hex, sha256Hex := tokenHashes(token)
	// 只允许一次查询；第二次认证必须命中缓存
	mock.ExpectQuery("SELECT keys.user_id, COALESCE").WithArgs(md5Hex, sha256Hex).
		WillReturnRows(sqlmock.NewRows([]string{"user_id", "tier", "org_id"}).AddRow("user-1", "free", ""))

	for i := 0; i < 3; i++ {
		if id, ok := authenticateRequest(context); !ok || id.UserID != "user-1" {
			t.Fatalf("attempt %d: expected cache-backed auth, got user=%q ok=%v", i, id.UserID, ok)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("cache hit must not query database: %v", err)
	}
}

func TestAuthenticateRequestTTLExpiryRequeriesAndRevokesRotatedKey(t *testing.T) {
	const token = "lce_rotated"
	context, mock, cleanup := newAuthTestContext(t, token)
	defer cleanup()

	md5Hex, sha256Hex := tokenHashes(token)
	mock.ExpectQuery("SELECT keys.user_id, COALESCE").WithArgs(md5Hex, sha256Hex).
		WillReturnRows(sqlmock.NewRows([]string{"user_id", "tier", "org_id"}).AddRow("user-1", "free", ""))
	mock.ExpectQuery("SELECT keys.user_id, COALESCE").WithArgs(md5Hex, sha256Hex).
		WillReturnRows(sqlmock.NewRows([]string{"user_id", "tier", "org_id"}))

	if id, ok := authenticateRequest(context); !ok || id.UserID != "user-1" {
		t.Fatalf("expected current key to authenticate, got user=%q ok=%v", id.UserID, ok)
	}
	// TTL 内旧 key 仍可通过缓存认证（撤销延迟 ≤30s，已定案语义）
	if _, ok := authenticateRequest(context); !ok {
		t.Fatal("within TTL the cached key should still authenticate")
	}
	backdateAuthCacheEntry(t, token)
	if id, ok := authenticateRequest(context); ok || id.UserID != "" {
		t.Fatalf("after TTL expiry rotated key must stop authenticating, got user=%q ok=%v", id.UserID, ok)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAuthenticateRequestNegativeCacheSuppressesRepeatedMisses(t *testing.T) {
	const token = "lce_unknown"
	context, mock, cleanup := newAuthTestContext(t, token)
	defer cleanup()

	md5Hex, sha256Hex := tokenHashes(token)
	// 只允许一次未命中查询；后续同 token 的爆破请求走负缓存
	mock.ExpectQuery("SELECT keys.user_id, COALESCE").WithArgs(md5Hex, sha256Hex).
		WillReturnRows(sqlmock.NewRows([]string{"user_id", "tier", "org_id"}))

	for i := 0; i < 3; i++ {
		if _, ok := authenticateRequest(context); ok {
			t.Fatalf("attempt %d: unknown key must not authenticate", i)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("negative cache must suppress repeated DB queries: %v", err)
	}

	// 负缓存过期后重查 DB，此时 key 已被写入则应认证成功
	mock.ExpectQuery("SELECT keys.user_id, COALESCE").WithArgs(md5Hex, sha256Hex).
		WillReturnRows(sqlmock.NewRows([]string{"user_id", "tier", "org_id"}).AddRow("user-2", "free", ""))
	backdateAuthCacheEntry(t, token)
	if id, ok := authenticateRequest(context); !ok || id.UserID != "user-2" {
		t.Fatalf("after negative TTL expiry expected re-query to authenticate, got user=%q ok=%v", id.UserID, ok)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAuthenticateRequestDatabaseErrorFailsClosedWithoutCaching(t *testing.T) {
	const token = "lce_db_down"
	context, mock, cleanup := newAuthTestContext(t, token)
	defer cleanup()

	md5Hex, sha256Hex := tokenHashes(token)
	mock.ExpectQuery("SELECT keys.user_id, COALESCE").WithArgs(md5Hex, sha256Hex).
		WillReturnError(errors.New("connection refused"))
	// 故障不缓存：下一次请求应重新查 DB 并在恢复后放行
	mock.ExpectQuery("SELECT keys.user_id, COALESCE").WithArgs(md5Hex, sha256Hex).
		WillReturnRows(sqlmock.NewRows([]string{"user_id", "tier", "org_id"}).AddRow("user-3", "free", ""))

	if _, ok := authenticateRequest(context); ok {
		t.Fatal("DB error must fail closed")
	}
	if id, ok := authenticateRequest(context); !ok || id.UserID != "user-3" {
		t.Fatalf("after DB recovery expected auth to succeed, got user=%q ok=%v", id.UserID, ok)
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

// TestVersionGateMetrics 挂在 compareVersions 测试旁：验证门禁判定与
// relay_client_version_gate_total 计数联动。
func TestVersionGateMetrics(t *testing.T) {
	previousMin := currentMinClientVersion()
	defer setMinClientVersion(previousMin)

	setMinClientVersion("")
	if got := evaluateVersionGate("0.0.1"); got != "" {
		t.Errorf("gate disabled: got %q, want empty", got)
	}

	setMinClientVersion("1.2.0")
	if got := evaluateVersionGate(""); got != "" {
		t.Errorf("no client version reported: got %q, want empty", got)
	}
	if got := evaluateVersionGate("1.1.9"); got != "reject_426" {
		t.Errorf("evaluateVersionGate(1.1.9) = %q, want reject_426", got)
	}
	if got := evaluateVersionGate("1.2.0"); got != "pass" {
		t.Errorf("evaluateVersionGate(1.2.0) = %q, want pass", got)
	}
	if got := evaluateVersionGate("2.0.0-beta"); got != "pass" {
		t.Errorf("evaluateVersionGate(2.0.0-beta) = %q, want pass", got)
	}

	passBefore := testutil.ToFloat64(metricVersionGate.WithLabelValues("pass"))
	rejectBefore := testutil.ToFloat64(metricVersionGate.WithLabelValues("reject_426"))

	recordVersionGate(evaluateVersionGate("1.3.0")) // pass
	recordVersionGate(evaluateVersionGate("1.0.0")) // reject_426
	recordVersionGate(evaluateVersionGate(""))      // 不计数

	if got := testutil.ToFloat64(metricVersionGate.WithLabelValues("pass")) - passBefore; got != 1 {
		t.Errorf("pass counter delta = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metricVersionGate.WithLabelValues("reject_426")) - rejectBefore; got != 1 {
		t.Errorf("reject_426 counter delta = %v, want 1", got)
	}
}

// ── tier 读取与缓存 ────────────────────────────────────────────────────────

func TestAuthenticateRequestReadsTierAndCachesIt(t *testing.T) {
	const token = "lce_pro_key"
	context, mock, cleanup := newAuthTestContext(t, token)
	defer cleanup()

	md5Hex, sha256Hex := tokenHashes(token)
	// 只允许一次查询：第二次认证的 tier 必须来自缓存
	mock.ExpectQuery("SELECT keys.user_id, COALESCE").WithArgs(md5Hex, sha256Hex).
		WillReturnRows(sqlmock.NewRows([]string{"user_id", "tier", "org_id"}).AddRow("user-pro", "pro", ""))

	proIdentity, ok := authenticateRequest(context)
	if !ok || proIdentity.UserID != "user-pro" || proIdentity.Tier != tierPro {
		t.Fatalf("expected pro tier auth, got user=%q tier=%q ok=%v", proIdentity.UserID, proIdentity.Tier, ok)
	}
	if cached, ok := authenticateRequest(context); !ok || cached.Tier != tierPro {
		t.Fatalf("cached auth must keep the tier, got tier=%q ok=%v", cached.Tier, ok)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("second auth must not query the database: %v", err)
	}
}

func TestAuthenticateRequestUnknownTierFallsBackToFree(t *testing.T) {
	const token = "lce_weird_tier"
	context, mock, cleanup := newAuthTestContext(t, token)
	defer cleanup()

	md5Hex, sha256Hex := tokenHashes(token)
	mock.ExpectQuery("SELECT keys.user_id, COALESCE").WithArgs(md5Hex, sha256Hex).
		WillReturnRows(sqlmock.NewRows([]string{"user_id", "tier", "org_id"}).AddRow("user-w", "platinum", ""))

	if weird, ok := authenticateRequest(context); !ok || weird.Tier != tierFree {
		t.Fatalf("unknown tier must normalize to free, got tier=%q ok=%v", weird.Tier, ok)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// ── 组织身份读取与缓存 ─────────────────────────────────────────────────────

func TestAuthenticateRequestReadsOrgFieldsAndCachesThem(t *testing.T) {
	const token = "lce_org_member_key"
	context, mock, cleanup := newAuthTestContext(t, token)
	defer cleanup()

	md5Hex, sha256Hex := tokenHashes(token)
	// 首次认证查 key + 权威成员关系；第二次必须完全来自缓存。
	mock.ExpectQuery("SELECT keys.user_id, COALESCE").WithArgs(md5Hex, sha256Hex).
		WillReturnRows(sqlmock.NewRows([]string{"user_id", "tier", "org_id"}).
			AddRow("user-m", "free", "org-1"))
	mock.ExpectQuery("SELECT COALESCE").WithArgs("org-1", "user-m").
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("member"))

	identity, ok := authenticateRequest(context)
	if !ok || identity.OrgID != "org-1" || identity.OrgRole != "member" {
		t.Fatalf("expected org identity, got %+v ok=%v", identity, ok)
	}
	if identity.TenantID() != "org-1" {
		t.Fatalf("tenant must resolve to org_id, got %q", identity.TenantID())
	}
	if cached, ok := authenticateRequest(context); !ok || cached.OrgID != "org-1" || cached.OrgRole != "member" {
		t.Fatalf("cached auth must keep org fields, got %+v ok=%v", cached, ok)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("second auth must not query the database: %v", err)
	}
}

func TestAuthenticateRequestRejectsOrgKeyWithoutCurrentMembership(t *testing.T) {
	const token = "lce_removed_org_member"
	context, mock, cleanup := newAuthTestContext(t, token)
	defer cleanup()

	md5Hex, sha256Hex := tokenHashes(token)
	mock.ExpectQuery("SELECT keys.user_id, COALESCE").WithArgs(md5Hex, sha256Hex).
		WillReturnRows(sqlmock.NewRows([]string{"user_id", "tier", "org_id"}).
			AddRow("removed-user", "free", "org-1"))
	mock.ExpectQuery("SELECT COALESCE").WithArgs("org-1", "removed-user").
		WillReturnRows(sqlmock.NewRows([]string{"role"}))

	if identity, ok := authenticateRequest(context); ok || identity.UserID != "" {
		t.Fatalf("removed organization member must not authenticate, got %+v ok=%v", identity, ok)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestNormalizeOrgRoleUsesCurrentCompoundMemberRole(t *testing.T) {
	if got := normalizeOrgRole("admin, owner", "org-1"); got != orgRoleOwner {
		t.Fatalf("compound owner role = %q, want owner", got)
	}
	if got := normalizeOrgRole("admin", "org-1"); got != "member" {
		t.Fatalf("non-owner organization role = %q, want member", got)
	}
	if got := normalizeOrgRole("owner", ""); got != "" {
		t.Fatalf("personal key role = %q, want empty", got)
	}
}

func TestAuthIdentityTenantResolution(t *testing.T) {
	personal := authIdentity{UserID: "user-1"}
	if personal.TenantID() != "user-1" {
		t.Fatalf("personal tenant must be user_id, got %q", personal.TenantID())
	}
	org := authIdentity{UserID: "user-1", OrgID: "org-9", OrgRole: "owner"}
	if org.TenantID() != "org-9" {
		t.Fatalf("org tenant must be org_id, got %q", org.TenantID())
	}
}
