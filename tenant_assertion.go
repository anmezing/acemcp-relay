package main

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// 租户断言：向 LCE 证明"这次调用是为哪个租户发起的"。
//
// tenant_id 在 MCP 里只是一个普通工具入参，LCE 无法区分授权调用与伪造调用：任何能连到
// LCE 端口的人都可以读、写、甚至用 codebase_clear_index 清空任意租户的索引。授权本来
// 完全落在 relay 这一侧，LCE 只能靠"端口别暴露"来兜底。
//
// relay 已经完成了用户认证，因此由它用共享密钥签发一个短时效断言，LCE 校验通过后才认
// 这个 tenant_id。持有密钥即证明调用方是 relay，载荷说明这次调用属于哪个租户。
//
// 与 LCE 侧 src/security/tenantAssertion.ts 的格式必须一致：
//
//	v1.<base64url(payload)>.<base64url(hmac-sha256(payload))>
//	payload = {"t":<租户>,"e":<过期秒级时间戳>,"n":<nonce>}
//
// HMAC 覆盖的是编码后的载荷，所以租户与过期时间被一起签名，换租户必然导致签名失配。
const (
	tenantAssertionVersion = "v1"
	// 断言可被捕获者在有效期内重放（仅限该租户），因此有效期要短，链路仍应是私网或 TLS。
	tenantAssertionTTL = 30 * time.Second
	// 与 LCE 侧一致：过短的密钥不构成认证，直接拒绝而不是假装能用。
	minTenantAssertionSecretLength = 32
)

var errTenantAssertionSecretMissing = errors.New("tenant assertion secret is not configured")

type tenantAssertionPayload struct {
	Tenant string `json:"t"`
	Expiry int64  `json:"e"`
	Nonce  string `json:"n"`
}

// tenantAssertionSigner 在密钥缺省时为 nil：此时不附带断言，供 LCE 未开启校验的
// 环境（本地/loopback）继续工作。
type tenantAssertionSigner struct {
	secret []byte
}

func newTenantAssertionSigner(secret string) (*tenantAssertionSigner, error) {
	trimmed := strings.TrimSpace(secret)
	if trimmed == "" {
		return nil, nil
	}
	if len(trimmed) < minTenantAssertionSecretLength {
		return nil, fmt.Errorf("LCE_TENANT_ASSERTION_SECRET must be at least %d characters", minTenantAssertionSecretLength)
	}
	return &tenantAssertionSigner{secret: []byte(trimmed)}, nil
}

func (s *tenantAssertionSigner) sign(tenantID string, now time.Time) (string, error) {
	if s == nil {
		return "", errTenantAssertionSecretMissing
	}
	trimmed := strings.TrimSpace(tenantID)
	if trimmed == "" {
		return "", errors.New("tenant assertion requires a non-empty tenant id")
	}

	nonce := make([]byte, 9)
	if _, err := rand.Read(nonce); err != nil {
		return "", fmt.Errorf("tenant assertion nonce: %w", err)
	}

	payload := tenantAssertionPayload{
		Tenant: trimmed,
		Expiry: now.Add(tenantAssertionTTL).Unix(),
		Nonce:  base64.RawURLEncoding.EncodeToString(nonce),
	}
	encodedPayload, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("tenant assertion payload: %w", err)
	}
	encoded := base64.RawURLEncoding.EncodeToString(encodedPayload)

	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(encoded))
	signature := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	return fmt.Sprintf("%s.%s.%s", tenantAssertionVersion, encoded, signature), nil
}

// authorizationHeader 返回该租户对应的 Authorization 头值。
// 未配置密钥时返回空串，调用方据此不设置该头。
func (s *tenantAssertionSigner) authorizationHeader(tenantID string) (string, error) {
	if s == nil {
		return "", nil
	}
	assertion, err := s.sign(tenantID, time.Now())
	if err != nil {
		return "", err
	}
	return "Bearer " + assertion, nil
}

// tenantIDFromArgs 从工具入参里取出本次调用真正作用的租户。
// 断言由此派生而不是由调用点各自传入，保证签发的租户与请求的租户永远一致。
func tenantIDFromArgs(args map[string]interface{}) string {
	if args == nil {
		return ""
	}
	if value, ok := args["tenant_id"].(string); ok {
		return strings.TrimSpace(value)
	}
	return ""
}
