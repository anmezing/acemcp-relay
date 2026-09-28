package main

import (
	"context"
	"fmt"
	"testing"
)

func TestIndexOperationLeaseConflictsPostgres(t *testing.T) {
	withIsolatedRelayPostgres(t)
	ctx := context.Background()
	acquire := func(token, resource, kind string, mode indexOperationMode) bool {
		t.Helper()
		acquired, err := tryAcquireIndexOperation(ctx, "tenant", resource, kind, token, mode)
		if err != nil {
			t.Fatal(err)
		}
		return acquired
	}
	release := func(token string) {
		t.Helper()
		if _, err := db.Exec(`DELETE FROM index_operation_leases WHERE lease_token = $1`, token); err != nil {
			t.Fatal(err)
		}
	}
	token := func(n int) string { return fmt.Sprintf("00000000-0000-0000-0000-%012d", n) }

	for i := 1; i <= maxConcurrentIndexUploadsPerJob; i++ {
		if !acquire(token(i), "job:a", indexUploadBatchKind, indexOperationShared) {
			t.Fatalf("upload batch %d of one job should overlap with the others", i)
		}
	}
	if acquire(token(10), "job:a", indexUploadBatchKind, indexOperationShared) {
		t.Fatal("upload batches of one job must stay bounded")
	}
	for _, kind := range []string{"complete-job", "fail-job", "reconcile-publication"} {
		if acquire(token(11), "job:a", kind, indexOperationShared) {
			t.Fatalf("%s must wait for in-flight upload batches of its job", kind)
		}
	}
	if !acquire(token(12), "job:b", indexUploadBatchKind, indexOperationShared) {
		t.Fatal("another job's upload batches are bounded independently")
	}
	if acquire(token(13), "*", "create-job", indexOperationExclusive) {
		t.Fatal("an exclusive operation must wait for every shared lease")
	}

	for i := 1; i <= maxConcurrentIndexUploadsPerJob; i++ {
		release(token(i))
	}
	if !acquire(token(20), "job:a", "complete-job", indexOperationShared) {
		t.Fatal("complete-job should acquire once the job's uploads finished")
	}
	if acquire(token(21), "job:a", indexUploadBatchKind, indexOperationShared) {
		t.Fatal("an upload batch must not overlap the job's completion")
	}
	if acquire(token(22), "job:a", "fail-job", indexOperationShared) {
		t.Fatal("non-upload operations on one job stay mutually exclusive")
	}
}
