package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

// ── 组织租户归并：配额、权限、LCE 租户参数 ─────────────────────────────────
//
// 核心公式 tenant_id := org_id ?? user_id。个人用户（org_id 空）必须与归并前
// 行为完全一致（quota_tier_test.go 里的存量用例即回归保障）；本文件覆盖组织路径。

func expectMemberQuotaRow(mock sqlmock.Sqlmock, orgID, userID string, limit int) {
	mock.ExpectQuery("SELECT daily_limit FROM org_member_quotas").WithArgs(orgID, userID).
		WillReturnRows(sqlmock.NewRows([]string{"daily_limit"}).AddRow(limit))
}

func expectNoMemberQuotaRow(mock sqlmock.Sqlmock, orgID, userID string) {
	mock.ExpectQuery("SELECT daily_limit FROM org_member_quotas").WithArgs(orgID, userID).
		WillReturnRows(sqlmock.NewRows([]string{"daily_limit"}))
}

func expectOrgQuotaRow(mock sqlmock.Sqlmock, orgID string, reqLimit, bytesLimit interface{}) {
	mock.ExpectQuery("SELECT daily_request_limit, daily_index_bytes_limit FROM org_quotas").WithArgs(orgID).
		WillReturnRows(sqlmock.NewRows([]string{"daily_request_limit", "daily_index_bytes_limit"}).
			AddRow(reqLimit, bytesLimit))
}

func TestGetOrgQuotaLimitsAllowsIndependentInheritedDimensions(t *testing.T) {
	mock, server := withTierQuotaEnv(t, 7, 0, 100, 0)
	expectOrgQuotaRow(mock, "org-null-request", nil, int64(50))
	expectOrgOwnerEntitlement(mock, "org-null-request", nil, nil, nil, "free")

	limits := getOrgQuotaLimits("org-null-request")
	if !limits.RequestSet || limits.Request != 7 {
		t.Fatalf("NULL request limit must inherit owner tier, got %+v", limits)
	}
	if !limits.IndexBytesSet || limits.IndexBytes != 50 {
		t.Fatalf("configured byte limit lost when request is NULL: %+v", limits)
	}
	if cached, err := server.Get("quota:limit:orgq:org-null-request"); err != nil || cached != "v2,7,50" {
		t.Fatalf("resolved org quota cache = %q (%v), want v2,7,50", cached, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestParseOrgQuotaCacheSupportsOnlyFinalV2Format(t *testing.T) {
	if _, ok := parseOrgQuotaCache("10,20"); ok {
		t.Fatal("unversioned old cache data must be rejected")
	}
	if _, ok := parseOrgQuotaCache("v1,25,n"); ok {
		t.Fatal("v1 nullable cache data is obsolete and must be rejected")
	}
	final, ok := parseOrgQuotaCache("v2,25,250")
	if !ok || !final.RequestSet || final.Request != 25 ||
		!final.IndexBytesSet || final.IndexBytes != 250 {
		t.Fatalf("final cache parse = %+v ok=%v", final, ok)
	}
	if _, ok := parseOrgQuotaCache("v2,broken,250"); ok {
		t.Fatal("invalid cache value must be rejected")
	}
}

func expectNoOrgQuotaRow(mock sqlmock.Sqlmock, orgID string) {
	mock.ExpectQuery("SELECT daily_request_limit, daily_index_bytes_limit FROM org_quotas").WithArgs(orgID).
		WillReturnRows(sqlmock.NewRows([]string{"daily_request_limit", "daily_index_bytes_limit"}))
	expectOrgOwnerEntitlement(mock, orgID, nil, nil, nil, "free")
}

func expectOrgOwnerEntitlement(
	mock sqlmock.Sqlmock,
	orgID string,
	requestLimit, indexBytesLimit, expiresAt interface{},
	ownerTier string,
) {
	mock.ExpectQuery("SELECT subscriptions.daily_request_limit").
		WithArgs(orgID).
		WillReturnRows(sqlmock.NewRows([]string{
			"daily_request_limit",
			"daily_index_bytes_limit",
			"expires_at",
			"owner_tier",
		}).AddRow(requestLimit, indexBytesLimit, expiresAt, ownerTier))
}

func TestMemberQuotaLimitCacheKeyContract(t *testing.T) {
	const want = "quota:limit:member:4620fd3c76d86783329c9d16a2f45531b11dd545776ca278e57ecc43888bf922"
	if got := memberQuotaLimitCacheKey("org-1", "user-1"); got != want {
		t.Fatalf("member quota cache key = %q, want %q", got, want)
	}
}

func TestCheckRequestQuotaOrgMemberPersonalLimitRejectsBeforePool(t *testing.T) {
	mock, _ := withTierQuotaEnv(t, 0, 0, 0, 0)
	expectMemberQuotaRow(mock, "org-1", "member-1", 2)
	expectOrgQuotaRow(mock, "org-1", 100, 0)

	for i := 0; i < 2; i++ {
		if ok, _ := checkRequestQuota("member-1", "org-1", "free"); !ok {
			t.Fatalf("request %d within member limit must pass", i)
		}
	}
	// 第 3 次撞成员个人上限，返回的 limit 是成员上限而不是组织池上限
	if ok, limit := checkRequestQuota("member-1", "org-1", "free"); ok || limit != 2 {
		t.Fatalf("request 3: ok=%v limit=%d, want member-limit rejection (2)", ok, limit)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCheckRequestQuotaOrgPoolSharedAcrossMembers(t *testing.T) {
	mock, _ := withTierQuotaEnv(t, 0, 0, 0, 0)
	expectNoMemberQuotaRow(mock, "org-1", "member-a")
	expectOrgQuotaRow(mock, "org-1", 3, 0)
	expectNoMemberQuotaRow(mock, "org-1", "member-b")

	if ok, _ := checkRequestQuota("member-a", "org-1", "free"); !ok {
		t.Fatal("member-a request 1 must pass")
	}
	if ok, _ := checkRequestQuota("member-a", "org-1", "free"); !ok {
		t.Fatal("member-a request 2 must pass")
	}
	if ok, _ := checkRequestQuota("member-b", "org-1", "free"); !ok {
		t.Fatal("member-b request 3 must pass (pool has 3)")
	}
	// 第 4 次由另一名成员发起也要被组织池拒绝：池按 org 共享
	if ok, limit := checkRequestQuota("member-b", "org-1", "free"); ok || limit != 3 {
		t.Fatalf("pool must be shared across members, ok=%v limit=%d", ok, limit)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCheckRequestQuotaFullOrgPoolDoesNotChargeMember(t *testing.T) {
	_, server := withTierQuotaEnv(t, 0, 0, 0, 0)
	orgID := "full-org"
	userID := "member-with-room"
	if err := server.Set(memberQuotaLimitCacheKey(orgID, userID), "10"); err != nil {
		t.Fatal(err)
	}
	if err := server.Set("quota:limit:orgq:"+orgID, "v2,1,0"); err != nil {
		t.Fatal(err)
	}
	day := time.Now().In(quotaLocation()).Format("20060102")
	if err := server.Set("quota:used:"+orgID+":"+day, "1"); err != nil {
		t.Fatal(err)
	}

	decision := checkRequestQuotaDetailed(userID, orgID, tierFree)
	if decision.Allowed || decision.Scope != "organization" || decision.Used != 1 || decision.Limit != 1 {
		t.Fatalf("unexpected organization rejection: %+v", decision)
	}
	memberKey := "quota:used:org:" + orgID + ":" + userID + ":" + day
	if server.Exists(memberKey) {
		t.Fatalf("full organization pool must not charge member key %q", memberKey)
	}
}

// 一人多密钥：同一用户同时持有个人密钥与组织密钥时，个人租户计数
// （quota:used:{user}）与该用户在组织内的计数（quota:used:org:{org}:{user}）
// 以及组织池计数互不影响。
func TestCheckRequestQuotaPersonalAndOrgKeysCountIndependently(t *testing.T) {
	mock, server := withTierQuotaEnv(t, 2, 0, 0, 0)
	// 个人路径：user_quotas 无覆盖 → free 默认 2
	expectNoQuotaOverride(mock, "dual-user")
	// 组织路径：成员上限 5，组织池 100
	expectMemberQuotaRow(mock, "org-1", "dual-user", 5)
	expectOrgQuotaRow(mock, "org-1", 100, 0)

	// 个人密钥打满 2 次后被拒
	for i := 0; i < 2; i++ {
		if ok, _ := checkRequestQuota("dual-user", "", "free"); !ok {
			t.Fatalf("personal request %d must pass", i)
		}
	}
	if ok, _ := checkRequestQuota("dual-user", "", "free"); ok {
		t.Fatal("personal key must hit its own limit of 2")
	}

	// 组织密钥不受个人用量影响：成员上限 5 内全部放行
	for i := 0; i < 5; i++ {
		if ok, _ := checkRequestQuota("dual-user", "org-1", "free"); !ok {
			t.Fatalf("org request %d must pass despite exhausted personal quota", i)
		}
	}
	if ok, limit := checkRequestQuota("dual-user", "org-1", "free"); ok || limit != 5 {
		t.Fatalf("org member limit must trigger at 5, ok=%v limit=%d", ok, limit)
	}

	day := time.Now().In(quotaLocation()).Format("20060102")
	if v, err := server.Get("quota:used:dual-user:" + day); err != nil || v != "2" {
		t.Fatalf("personal counter = %q (%v), want 2 (rejected requests are not charged)", v, err)
	}
	if v, err := server.Get("quota:used:org:org-1:dual-user:" + day); err != nil || v != "5" {
		t.Fatalf("member counter = %q (%v), want 5 (rejected requests are not charged)", v, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCheckRequestQuotaOrgWithoutQuotaRowUsesOwnerTierNotCallerTier(t *testing.T) {
	mock, _ := withTierQuotaEnv(t, 2, 100, 0, 0)
	expectNoMemberQuotaRow(mock, "org-2", "member-1")
	expectNoOrgQuotaRow(mock, "org-2")

	for i := 0; i < 2; i++ {
		if ok, limit := checkRequestQuota("member-1", "org-2", "pro"); !ok || limit != 2 {
			t.Fatalf("request %d: ok=%v limit=%d, want owner free limit 2", i, ok, limit)
		}
	}
	if ok, _ := checkRequestQuota("member-1", "org-2", "pro"); ok {
		t.Fatal("a pro member must not raise a free owner's organization limit")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCheckRequestQuotaMemberLimitsAreIsolatedPerOrg(t *testing.T) {
	mock, _ := withTierQuotaEnv(t, 0, 0, 0, 0)
	expectMemberQuotaRow(mock, "org-a", "shared-user", 1)
	expectOrgQuotaRow(mock, "org-a", 100, 0)
	expectMemberQuotaRow(mock, "org-b", "shared-user", 3)
	expectOrgQuotaRow(mock, "org-b", 100, 0)

	if ok, _ := checkRequestQuota("shared-user", "org-a", "free"); !ok {
		t.Fatal("org-a first request must pass")
	}
	if ok, limit := checkRequestQuota("shared-user", "org-a", "free"); ok || limit != 1 {
		t.Fatalf("org-a second request must hit its own limit, ok=%v limit=%d", ok, limit)
	}
	for i := 0; i < 3; i++ {
		if ok, limit := checkRequestQuota("shared-user", "org-b", "free"); !ok || limit != 100 {
			t.Fatalf("org-b request %d must not inherit org-a limit, ok=%v limit=%d", i, ok, limit)
		}
	}
	if ok, limit := checkRequestQuota("shared-user", "org-b", "free"); ok || limit != 3 {
		t.Fatalf("org-b fourth request must hit limit 3, ok=%v limit=%d", ok, limit)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestChargeIndexBytesOrgUsesOrgQuotaAndSharesTenantPool(t *testing.T) {
	mock, _ := withTierQuotaEnv(t, 0, 0, 1000, 0)
	expectOrgQuotaRow(mock, "org-1", 0, 50)

	// 组织有 org_quotas：字节上限用 50 而不是 tier 默认 1000；计数按租户（org）共享
	if allowed, _, limit := chargeIndexBytes("org-1", "org-1", "free", 50); !allowed || limit != 50 {
		t.Fatalf("org byte charge: allowed=%v limit=%d, want 50", allowed, limit)
	}
	if allowed, _, _ := chargeIndexBytes("org-1", "org-1", "free", 1); allowed {
		t.Fatal("org byte pool is shared: second charge must be rejected")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestChargeIndexBytesOrgWithoutQuotaRowUsesOwnerTierNotCallerTier(t *testing.T) {
	mock, _ := withTierQuotaEnv(t, 0, 0, 100, 0)
	expectNoOrgQuotaRow(mock, "org-3")

	if allowed, _, limit := chargeIndexBytes("org-3", "org-3", "pro", 100); !allowed || limit != 100 {
		t.Fatalf("org fallback byte charge: allowed=%v limit=%d, want owner free limit 100", allowed, limit)
	}
	if allowed, _, _ := chargeIndexBytes("org-3", "org-3", "pro", 1); allowed {
		t.Fatal("a pro member must not make a free owner's org byte pool unlimited")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestOrgWithoutSubscriptionInheritsOwnerBaseProTier(t *testing.T) {
	mock, _ := withTierQuotaEnv(t, 2, 9, 100, 900)
	expectNoMemberQuotaRow(mock, "org-owner-pro", "member-free")
	mock.ExpectQuery("SELECT daily_request_limit, daily_index_bytes_limit FROM org_quotas").
		WithArgs("org-owner-pro").
		WillReturnRows(sqlmock.NewRows([]string{"daily_request_limit", "daily_index_bytes_limit"}))
	expectOrgOwnerEntitlement(mock, "org-owner-pro", nil, nil, nil, "pro")

	if ok, limit := checkRequestQuota("member-free", "org-owner-pro", "free"); !ok || limit != 9 {
		t.Fatalf("owner base tier request limit: ok=%v limit=%d, want 9", ok, limit)
	}
	if allowed, _, limit := chargeIndexBytes("org-owner-pro", "org-owner-pro", "free", 900); !allowed || limit != 900 {
		t.Fatalf("owner base tier byte limit: allowed=%v limit=%d, want 900", allowed, limit)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestOrgWithoutAdminOverrideInheritsOwnerPlan(t *testing.T) {
	mock, _ := withTierQuotaEnv(t, 2, 100, 100, 1000)
	expectNoMemberQuotaRow(mock, "org-paid", "member-1")
	mock.ExpectQuery("SELECT daily_request_limit, daily_index_bytes_limit FROM org_quotas").
		WithArgs("org-paid").
		WillReturnRows(sqlmock.NewRows([]string{"daily_request_limit", "daily_index_bytes_limit"}))
	expectOrgOwnerEntitlement(mock, "org-paid", 9, 900, time.Now().Add(time.Hour), "free")

	if ok, limit := checkRequestQuota("member-1", "org-paid", "free"); !ok || limit != 9 {
		t.Fatalf("organization request plan: ok=%v limit=%d, want 9", ok, limit)
	}

	// owner 套餐两个维度已作为一份最终权益缓存，字节请求不再重复查库。
	if allowed, _, limit := chargeIndexBytes("org-paid", "org-paid", "free", 900); !allowed || limit != 900 {
		t.Fatalf("organization byte plan: allowed=%v limit=%d, want 900", allowed, limit)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// ── 删除权限：仅组织 owner ─────────────────────────────────────────────────

func newOrgContext(t *testing.T, userID, orgID, orgRole, method, body string) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	c, recorder := newRootAdminContext(t, userID, method, body)
	c.Set(ContextKeyTenantID, orgID)
	c.Set(ContextKeyOrgID, orgID)
	c.Set(ContextKeyOrgRole, orgRole)
	return c, recorder
}

func TestHandleDeleteRootOrgMemberForbidden(t *testing.T) {
	resetDeleteRootRateLimit()
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		// 不预期任何查询：403 必须发生在运行中任务检查与 LCE 调用之前
		c, recorder := newOrgContext(t, "member-1", "org-1", "member", "POST", `{"root_id":"repo-a"}`)
		handleDeleteRoot(c)
		if recorder.Code != 403 {
			t.Fatalf("status = %d, want 403, body = %s", recorder.Code, recorder.Body.String())
		}
		if !strings.Contains(recorder.Body.String(), "仅组织所有者可删除索引") {
			t.Fatalf("403 body must carry the contract error message: %s", recorder.Body.String())
		}
	})
}

func TestHandleDeleteRootOrgUnknownRoleFailsClosed(t *testing.T) {
	resetDeleteRootRateLimit()
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		c, recorder := newOrgContext(t, "member-1", "org-1", "", "POST", `{"root_id":"repo-a"}`)
		handleDeleteRoot(c)
		if recorder.Code != 403 {
			t.Fatalf("missing org_role must fail closed, status = %d", recorder.Code)
		}
	})
}

func TestHandleDeleteRootOrgOwnerDeletesWithOrgTenant(t *testing.T) {
	resetDeleteRootRateLimit()
	var lceTenant, lceRoot string
	stubLCEClearIndexRoot(t, func(ctx context.Context, userID, rootID string) (*mcpToolResult, error) {
		lceTenant, lceRoot = userID, rootID
		return &mcpToolResult{Content: []byte(`{"ok":true}`)}, nil
	})
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		// 所有数据操作都按租户 org-1 而非真实用户 owner-1
		mock.ExpectQuery("SELECT EXISTS").
			WithArgs("org-1", "repo-a").
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
		expectDeleteRootTx(mock, "org-1", "repo-a", 5)

		c, recorder := newOrgContext(t, "owner-1", "org-1", "owner", "POST", `{"root_id":"repo-a"}`)
		handleDeleteRoot(c)
		if recorder.Code != 200 {
			t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
		}
		var response struct {
			Deleted bool `json:"deleted"`
		}
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil || !response.Deleted {
			t.Fatalf("unexpected response: %s", recorder.Body.String())
		}
	})
	if lceTenant != "org-1" || lceRoot != "repo-a" {
		t.Fatalf("LCE clear must use the org tenant, got tenant=%q root=%q", lceTenant, lceRoot)
	}
}

func TestHandleClearIndexOrgMemberForbidden(t *testing.T) {
	c, recorder := newOrgContext(t, "member-1", "org-1", "member", "POST", `{}`)
	handleClearIndex(c)
	if recorder.Code != 403 {
		t.Fatalf("status = %d, want 403, body = %s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "仅组织所有者可删除索引") {
		t.Fatalf("403 body must carry the contract error message: %s", recorder.Body.String())
	}
}

// 个人用户（org_id 空）不受 owner 门禁影响：走到后续逻辑（这里止步于运行中
// 任务检查的 mock），而不是 403。
func TestHandleDeleteRootPersonalUserNotAffectedByOwnerGate(t *testing.T) {
	resetDeleteRootRateLimit()
	withMockDB(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("SELECT EXISTS").
			WithArgs("user-1", "repo-a").
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
		c, recorder := newRootAdminContext(t, "user-1", "POST", `{"root_id":"repo-a"}`)
		handleDeleteRoot(c)
		if recorder.Code == 403 {
			t.Fatalf("personal user must never hit the owner gate: %s", recorder.Body.String())
		}
		if recorder.Code != 409 {
			t.Fatalf("status = %d, want 409 from the running-job check", recorder.Code)
		}
	})
}
