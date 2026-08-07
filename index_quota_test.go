package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// 索引通道同时受请求数和上传字节约束。请求数不能代表 embedding 成本：
//
// 下面三条边界共同限制单次和每日成本：单文件上限、单批上限、每日字节配额。

func TestIndexSizeCapsAreOrderedAndBounded(t *testing.T) {
	// 单文件可以独占一批，但不能绕过批次总量上限。
	if maxIndexFileBytes > maxIndexBatchBytes {
		t.Errorf("单文件上限 (%d) 不应超过单批上限 (%d)", maxIndexFileBytes, maxIndexBatchBytes)
	}
	// 每日配额必须显著大于单批上限，否则一批就撞墙，正常扫描无法完成
	if defaultDailyIndexBytes <= maxIndexBatchBytes {
		t.Errorf("每日配额 (%d) 应远大于单批上限 (%d)", defaultDailyIndexBytes, maxIndexBatchBytes)
	}
	// 单批上限乘以批文件数上限不应超出 nginx 的 100MB 请求体限制
	const nginxBodyLimit = 100 << 20
	if maxIndexBatchBytes > nginxBodyLimit {
		t.Errorf("单批上限 (%d) 超过了 nginx 的请求体上限 (%d)", maxIndexBatchBytes, nginxBodyLimit)
	}
}

func TestWorstCaseJSONEscapingFitsDownstreamBodyLimit(t *testing.T) {
	content := strings.Repeat("\x00", maxIndexBatchBytes)
	files := []indexManifestFile{{Path: "src/control-bytes.txt", Content: content}}
	if _, err := validateIndexBatchSize(files); err != nil {
		t.Fatalf("raw batch at the negotiated limit should pass: %v", err)
	}
	args := map[string]interface{}{
		"tenant_id": "tenant-1",
		"job_id":    "7e224a32-3423-4bb0-9213-3c55a5797c9d",
		"operation": "stage",
		"root_id":   "root-1",
		"files": []map[string]interface{}{{
			"path": "src/control-bytes.txt", "content": content,
		}},
	}
	encoded, err := validateMCPToolCallBody("codebase_remote_index", args)
	if err != nil {
		t.Fatalf("worst-case escaped batch must fit the LCE body contract: %v", err)
	}
	if encoded <= maxIndexBatchBytes {
		t.Fatalf("fixture did not exercise JSON expansion: encoded=%d raw=%d", encoded, maxIndexBatchBytes)
	}
}

func TestEncodedMCPEnvelopeCannotExceedDownstreamBodyLimit(t *testing.T) {
	_, err := validateMCPToolCallBody("codebase_remote_index", map[string]interface{}{
		"tenant_id": strings.Repeat("x", maxLCEMCPRequestBodyBytes),
	})
	if err == nil {
		t.Fatal("oversized encoded MCP envelope should be rejected before calling LCE")
	}
	if !strings.Contains(err.Error(), "encoded LCE request") {
		t.Fatalf("unexpected body-limit error: %v", err)
	}
}

// 攻击者的核心手法是用极少的请求配额推送极大的内容量。这里换算出加固后
// 一次请求配额最多能撬动多少字节，确认它不再是"无限"。
func TestSingleRequestCannotDriveUnboundedEmbedding(t *testing.T) {
	// 加固前：1 次配额（创建 job）之后的批次不受任何体积约束
	// 加固后：每批最多 maxIndexBatchBytes，且当日总量受 defaultDailyIndexBytes 封顶
	maxPerDay := int64(defaultDailyIndexBytes)

	// 上百 GB 是修复前的量级，修复后必须远低于它
	const preFixOrderOfMagnitude = int64(100) << 30 // 100 GiB
	if maxPerDay >= preFixOrderOfMagnitude {
		t.Fatalf("每日索引上限 %d 字节仍在修复前的量级 (%d)", maxPerDay, preFixOrderOfMagnitude)
	}

	// 同时也不能收得过紧：一个中型仓库首次全量索引通常在百 MB 量级
	const typicalFullIndex = int64(500) << 20 // 500 MiB
	if maxPerDay < typicalFullIndex {
		t.Errorf("每日索引上限 %d 字节过紧，正常仓库首次全量索引需要约 %d", maxPerDay, typicalFullIndex)
	}
}

func TestOversizedFileIsRejected(t *testing.T) {
	_, err := validateIndexBatchSize([]indexManifestFile{
		{Path: "src/small.go", Content: "package main"},
		{Path: "generated/bundle.js", Content: strings.Repeat("x", maxIndexFileBytes+1)},
	})
	if err == nil {
		t.Fatal("超过单文件上限的文件应当被拒绝")
	}
	// 错误要指出是哪个文件，否则客户端无从定位
	if !strings.Contains(err.Error(), "generated/bundle.js") {
		t.Errorf("错误消息应包含超限的文件名，实际: %v", err)
	}
}

func TestBatchOfIndividuallyLegalFilesStillHitsBatchCap(t *testing.T) {
	// 每个文件都不超单文件上限，但凑满一批就超总量 —— 这是绕过单文件限制的直接手法
	files := make([]indexManifestFile, maxIndexBatchFiles)
	for i := range files {
		files[i] = indexManifestFile{
			Path:    fmt.Sprintf("src/f%d.go", i),
			Content: strings.Repeat("x", maxIndexFileBytes),
		}
	}

	_, err := validateIndexBatchSize(files)
	if err == nil {
		t.Fatalf("%d 个各 %d 字节的文件应当撞上单批上限 %d",
			maxIndexBatchFiles, maxIndexFileBytes, maxIndexBatchBytes)
	}
	if !strings.Contains(err.Error(), "batch") {
		t.Errorf("错误消息应指明是批次超限，实际: %v", err)
	}
}

func TestNormalBatchPassesAndReportsItsSize(t *testing.T) {
	// 正常增量批次必须放行，且返回的字节数要等于内容总量——那是计入配额的依据，
	// 报少了等于漏计费，报多了会误伤正常用户。
	files := []indexManifestFile{
		{Path: "a.go", Content: strings.Repeat("x", 1000)},
		{Path: "b.go", Content: strings.Repeat("y", 2000)},
		{Path: "c.go", Content: ""},
	}
	total, err := validateIndexBatchSize(files)
	if err != nil {
		t.Fatalf("正常批次不应被拒绝: %v", err)
	}
	if total != 3000 {
		t.Errorf("返回字节数 = %d, 期望 3000", total)
	}
}

func withTestIndexQuota(t *testing.T, limit int64) *miniredis.Miniredis {
	t.Helper()
	server := miniredis.RunT(t)
	previousClient := redisClient
	previousLimit := dailyIndexBytesLimit
	redisClient = redis.NewClient(&redis.Options{Addr: server.Addr()})
	dailyIndexBytesLimit = limit
	t.Cleanup(func() {
		_ = redisClient.Close()
		redisClient = previousClient
		dailyIndexBytesLimit = previousLimit
	})
	return server
}

func TestChargeIndexBytesRejectsWithoutIncreasingUsage(t *testing.T) {
	server := withTestIndexQuota(t, 100)
	userID := "quota-user"

	allowed, used, _ := chargeIndexBytes(userID, 60)
	if !allowed || used != 60 {
		t.Fatalf("first charge = allowed %v, used %d; want true, 60", allowed, used)
	}
	allowed, used, _ = chargeIndexBytes(userID, 50)
	if allowed || used != 60 {
		t.Fatalf("over-limit charge = allowed %v, used %d; want false, unchanged 60", allowed, used)
	}
	stored, err := server.Get(indexBytesKey(userID))
	if err != nil {
		t.Fatal(err)
	}
	if stored != "60" {
		t.Fatalf("rejected charge changed Redis usage to %q", stored)
	}
	if ttl := server.TTL(indexBytesKey(userID)); ttl <= 0 {
		t.Fatalf("quota key must expire, ttl=%s", ttl)
	}
}

func TestChargeIndexBytesIsAtomicUnderConcurrency(t *testing.T) {
	server := withTestIndexQuota(t, 100)
	userID := "concurrent-quota-user"
	var wg sync.WaitGroup
	var mu sync.Mutex
	allowedCount := 0
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			allowed, _, _ := chargeIndexBytes(userID, 10)
			if allowed {
				mu.Lock()
				allowedCount++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if allowedCount != 10 {
		t.Fatalf("allowed %d concurrent charges, want exactly 10", allowedCount)
	}
	stored, err := server.Get(indexBytesKey(userID))
	if err != nil {
		t.Fatal(err)
	}
	if stored != strconv.Itoa(100) {
		t.Fatalf("stored usage = %q, want 100", stored)
	}
}

func TestQuotaRetryAfterUsesShanghaiDayBoundary(t *testing.T) {
	loc := quotaLocation()
	now := time.Date(2026, 7, 31, 23, 59, 59, 500_000_000, loc)
	if got := quotaRetryAfterHeader(now); got != "1" {
		t.Fatalf("Retry-After = %s, want 1 second", got)
	}
}
