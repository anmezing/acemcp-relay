package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestSwiftPartReservationConcurrentAndAcrossMidnight(t *testing.T) {
	server := withTestIndexQuota(t, 100)
	pool := indexQuotaPool{PoolID: "owner", Limit: 100}
	now := time.Date(2026, 9, 16, 23, 59, 59, 0, quotaLocation())
	var total atomic.Int64
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, conflict := chargeSwiftPart(context.Background(), pool, "org", "root", "job", 0, []byte("abc"), now)
			if conflict || !d.Allowed {
				t.Errorf("duplicate rejected: %+v conflict=%v", d, conflict)
			}
			total.Add(d.Charged)
		}()
	}
	wg.Wait()
	if total.Load() != 3 {
		t.Fatalf("charged=%d", total.Load())
	}
	d, conflict := chargeSwiftPart(context.Background(), pool, "org", "root", "job", 0, []byte("abc"), now.Add(2*time.Second))
	if !d.Allowed || conflict || d.Charged != 0 || server.Exists(indexBytesKeyAt(pool.PoolID, now.Add(2*time.Second))) {
		t.Fatalf("midnight replay charged: %+v", d)
	}
	d, conflict = chargeSwiftPart(context.Background(), pool, "org", "root", "job", 0, []byte("def"), now)
	if !conflict || d.Allowed || d.Charged != 0 {
		t.Fatalf("mutable identity accepted: %+v", d)
	}
}

func TestSwiftPartSharedPoolAndRejectedReservation(t *testing.T) {
	withTestIndexQuota(t, 5)
	pool := indexQuotaPool{PoolID: "owner", Limit: 5}
	now := time.Now()
	charge := func(tenant string, part int64, data string) indexQuotaDecision {
		d, conflict := chargeSwiftPart(context.Background(), pool, tenant, "root", "job", part, []byte(data), now)
		if conflict {
			t.Fatal("unexpected conflict")
		}
		return d
	}
	if d := charge("org-a", 0, "abc"); !d.Allowed || d.Charged != 3 {
		t.Fatal(d)
	}
	if d := charge("org-b", 0, "abc"); d.Allowed || d.Used != 3 {
		t.Fatal(d)
	}
	if d := charge("org-b", 0, "ab"); !d.Allowed || d.Charged != 2 {
		t.Fatal(d)
	}
	pool.Limit = 0
	if d := charge("org-b", 1, "abc"); !d.Allowed || d.Charged != 3 {
		t.Fatal(d)
	}
	if d := charge("org-b", 1, "abc"); !d.Allowed || d.Charged != 0 {
		t.Fatal(d)
	}
}

func TestSwiftUploadChargesDecodedBytesAndValidatesParts(t *testing.T) {
	server := withTestIndexQuota(t, 100)
	server.Set("quota:limit:indexbytes:tenant", "100")
	args := map[string]interface{}{"operation": "upload", "root_id": "root", "job_id": "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa", "part": float64(0), "data": base64.StdEncoding.EncodeToString([]byte("abcde"))}
	if err := reserveSwiftUpload(context.Background(), "tenant", "", tierFree, args); err != nil {
		t.Fatal(err)
	}
	if value, _ := server.Get(indexBytesKey("tenant")); value != "5" {
		t.Fatalf("decoded charge=%s", value)
	}
	for _, data := range []string{"", "YWJj\n", "YR==", strings.Repeat("a", base64.StdEncoding.EncodedLen(swiftSyncPartBytes)+4)} {
		args["data"] = data
		if err := reserveSwiftUpload(context.Background(), "tenant", "", tierFree, args); err == nil || err.Code != "SWIFT_SYNC_INVALID" {
			t.Fatalf("invalid data accepted: %v", err)
		}
	}
	args["data"] = "YQ=="
	for _, part := range []float64{-1, 0.5, 512} {
		args["part"] = part
		if err := reserveSwiftUpload(context.Background(), "tenant", "", tierFree, args); err == nil || err.Code != "SWIFT_SYNC_INVALID" {
			t.Fatalf("invalid part accepted: %v", err)
		}
	}
	args["part"] = float64(1)
	server.Close()
	if err := reserveSwiftUpload(context.Background(), "tenant", "", tierFree, args); err == nil || err.Code != "QUOTA_ACCOUNTING_UNAVAILABLE" {
		t.Fatalf("outage not closed: %v", err)
	}
}

func TestSwiftSynchronizationRateIsSeparateFromRequestQuota(t *testing.T) {
	server := withTestIndexQuota(t, 100)
	server.Set("quota:limit:tenant", "1")
	rpc := jsonRPCRequest{Method: "tools/call", Params: json.RawMessage(`{"name":"codebase_swift_sync","arguments":{"operation":"status","root_id":"root"}}`)}
	admit := func(rpc jsonRPCRequest) (bool, int) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("POST", "/mcp", nil)
		c.Set(ContextKeyUserID, "tenant")
		c.Set(ContextKeyUserTier, tierFree)
		c.Set(contextRequestQuotaDeferred, true)
		ok := admitDeferredMCPQuota(c, rpc)
		if ok && !admitDeferredMCPQuota(c, rpc) {
			t.Fatal("double admission charged again")
		}
		return ok, w.Code
	}
	for range swiftSyncRequestsPerMinute {
		if ok, status := admit(rpc); !ok {
			t.Fatalf("sync quota rejected early: %d", status)
		}
	}
	if ok, status := admit(rpc); ok || status != 429 {
		t.Fatalf("rate limit missing: %v %d", ok, status)
	}
	if ok, _ := admit(jsonRPCRequest{}); !ok {
		t.Fatal("sync consumed daily quota")
	}
	if ok, status := admit(jsonRPCRequest{}); ok || status != 429 {
		t.Fatalf("ordinary quota missing: %v %d", ok, status)
	}
	server.FastForward(time.Minute)
	if ok, _ := admit(rpc); !ok {
		t.Fatal("rate window did not recover")
	}
}

func TestSwiftQuotaClassification(t *testing.T) {
	for _, body := range []string{
		`{"name":"codebase_index","arguments":{"operation":"status","root_id":"root"}}`,
		`{"name":"codebase_swift_sync","arguments":{"operation":"invented","root_id":"root"}}`,
		`{"name":"codebase_swift_sync","arguments":{"operation":"status","root_id":123}}`,
		`{"name":"codebase_swift_sync","arguments":{"operation":"status","root_id":"root","tenant_id":"other"}}`,
	} {
		if isSwiftSynchronizationRequest(jsonRPCRequest{Method: "tools/call", Params: json.RawMessage(body)}) {
			t.Fatal(body)
		}
	}
}
