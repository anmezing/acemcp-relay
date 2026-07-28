package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

const testSecret = "test-secret-that-is-long-enough-for-hmac"

func TestSignerRejectsWeakSecret(t *testing.T) {
	if _, err := newTenantAssertionSigner("short"); err == nil {
		t.Fatal("expected a short secret to be rejected rather than silently accepted")
	}
}

func TestSignerDisabledWithoutSecret(t *testing.T) {
	signer, err := newTenantAssertionSigner("")
	if err != nil {
		t.Fatalf("empty secret should not be an error: %v", err)
	}
	if signer != nil {
		t.Fatal("expected no signer when the secret is unset")
	}
	header, err := signer.authorizationHeader("tenant-a")
	if err != nil || header != "" {
		t.Fatalf("expected no header and no error without a secret, got %q %v", header, err)
	}
}

// 断言必须与 LCE 的校验实现同格式：v1.<base64url(payload)>.<base64url(hmac)>，
// HMAC 覆盖编码后的 payload。这里独立重算一遍签名，而不是复用 sign 的内部结果。
func TestAssertionWireFormatMatchesVerifier(t *testing.T) {
	signer, err := newTenantAssertionSigner(testSecret)
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	now := time.Unix(1_700_000_000, 0)
	assertion, err := signer.sign("tenant-a", now)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}

	parts := strings.Split(assertion, ".")
	if len(parts) != 3 {
		t.Fatalf("expected 3 dot-separated parts, got %d", len(parts))
	}
	if parts[0] != tenantAssertionVersion {
		t.Fatalf("version = %q, want %q", parts[0], tenantAssertionVersion)
	}

	mac := hmac.New(sha256.New, []byte(testSecret))
	mac.Write([]byte(parts[1]))
	want := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if parts[2] != want {
		t.Fatal("signature does not cover the encoded payload exactly as the verifier expects")
	}

	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("payload is not base64url: %v", err)
	}
	var payload tenantAssertionPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("payload is not JSON: %v", err)
	}
	if payload.Tenant != "tenant-a" {
		t.Fatalf("tenant = %q", payload.Tenant)
	}
	if payload.Expiry != now.Add(tenantAssertionTTL).Unix() {
		t.Fatalf("expiry = %d, want %d", payload.Expiry, now.Add(tenantAssertionTTL).Unix())
	}
	if payload.Nonce == "" {
		t.Fatal("nonce must be present so a verifier can add replay rejection without a format change")
	}
}

func TestAssertionNonceDiffersPerCall(t *testing.T) {
	signer, _ := newTenantAssertionSigner(testSecret)
	now := time.Unix(1_700_000_000, 0)
	first, _ := signer.sign("tenant-a", now)
	second, _ := signer.sign("tenant-a", now)
	if first == second {
		t.Fatal("two assertions issued at the same instant must still differ")
	}
}

func TestSignRejectsEmptyTenant(t *testing.T) {
	signer, _ := newTenantAssertionSigner(testSecret)
	if _, err := signer.sign("   ", time.Now()); err == nil {
		t.Fatal("expected an empty tenant to be rejected")
	}
}

// 断言从 args 派生，保证签发的租户与请求的租户一致；调用点无法各自传错。
func TestTenantIDFromArgs(t *testing.T) {
	cases := []struct {
		name string
		args map[string]interface{}
		want string
	}{
		{"nil args", nil, ""},
		{"absent", map[string]interface{}{"repo_path": "/x"}, ""},
		{"present", map[string]interface{}{"tenant_id": "tenant-a"}, "tenant-a"},
		{"trimmed", map[string]interface{}{"tenant_id": "  tenant-a  "}, "tenant-a"},
		{"wrong type", map[string]interface{}{"tenant_id": 42}, ""},
		{"empty string", map[string]interface{}{"tenant_id": ""}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tenantIDFromArgs(tc.args); got != tc.want {
				t.Fatalf("tenantIDFromArgs = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestAuthorizationHeaderCarriesBearerScheme(t *testing.T) {
	signer, _ := newTenantAssertionSigner(testSecret)
	header, err := signer.authorizationHeader("tenant-a")
	if err != nil {
		t.Fatalf("authorizationHeader: %v", err)
	}
	if !strings.HasPrefix(header, "Bearer "+tenantAssertionVersion+".") {
		t.Fatalf("header = %q, want a Bearer-scheme assertion", header)
	}
}
