package main

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

func TestRootDeletionPostgres(t *testing.T) {
	dsn := os.Getenv("LCE_RELAY_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("LCE_RELAY_TEST_DATABASE_URL is required for isolated PostgreSQL tests")
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := "root_deletion_test_" + uuid.New().String()
	if _, err := admin.Exec("CREATE SCHEMA " + pq.QuoteIdentifier(schema)); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := admin.Exec("DROP SCHEMA " + pq.QuoteIdentifier(schema) + " CASCADE"); err != nil {
			t.Error(err)
		}
	}()
	query := u.Query()
	query.Set("search_path", schema)
	u.RawQuery = query.Encode()
	isolated, err := sql.Open("postgres", u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer isolated.Close()
	previous := db
	db = isolated
	defer func() { db = previous }()
	if err := migrateIndexingTables(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	resetDeleteRootRateLimit()
	seed := func(tenant, root string) {
		t.Helper()
		if _, err := db.Exec(`INSERT INTO index_workspaces(user_id, workspace_id, root_id) VALUES ($1, $2, $2)`, tenant, root); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO indexed_files(user_id, workspace_id, path, hash, size) VALUES ($1, $2, 'test.go', 'hash', 10)`, tenant, root); err != nil {
			t.Fatal(err)
		}
	}
	files := func(tenant string) int {
		t.Helper()
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM indexed_files WHERE user_id = $1`, tenant).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}

	seed("alice", "repo-a")
	seed("bob", "repo-a")
	first, err := enqueueRootDeletion(ctx, "alice", "alice", "repo-a")
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for i := 0; i < 6; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			repeated, err := enqueueRootDeletion(ctx, "alice", "alice", "repo-a")
			if err != nil || repeated.ID != first.ID {
				t.Errorf("duplicate submission: %+v %v", repeated, err)
			}
		}()
	}
	group.Wait()
	if _, err := enqueueRootDeletion(ctx, "alice", "alice", "other-root"); !errors.Is(err, errIndexOperationBusy) {
		t.Fatalf("another deletion should conflict, got %v", err)
	}
	if _, err := acquireExclusiveIndexOperation(ctx, "alice", "create-job"); !errors.Is(err, errIndexOperationBusy) {
		t.Fatalf("new indexing should fail fast behind the durable marker: %v", err)
	}
	available, err := tryAcquireIndexOperation(ctx, "bob", "*", "create-job", uuid.NewString(), indexOperationExclusive)
	if err != nil || !available {
		t.Fatalf("deletion must not block another tenant: %v", err)
	}
	oldAttempt, err := claimRootDeletion(ctx)
	if err != nil || oldAttempt.ID != first.ID {
		t.Fatalf("claim: %+v %v", oldAttempt, err)
	}
	if _, err := db.Exec(`UPDATE root_deletion_jobs SET claim_until = NOW() - INTERVAL '1 second' WHERE id = $1`, first.ID); err != nil {
		t.Fatal(err)
	}
	newAttempt, err := claimRootDeletion(ctx)
	if err != nil || newAttempt.AttemptID == oldAttempt.AttemptID {
		t.Fatalf("reclaim must fence the stale worker: %+v %v", newAttempt, err)
	}
	calls := 0
	stubLCEClearIndexRoot(t, func(_ context.Context, tenant, root string) (*mcpToolResult, error) {
		calls++
		if tenant != "alice" || root != "repo-a" {
			t.Fatalf("wrong cloud deletion identity: %s/%s", tenant, root)
		}
		return &mcpToolResult{Content: []byte(`{"deleted_files":7}`)}, nil
	})
	processRootDeletion(ctx, oldAttempt)
	if calls != 0 || files("alice") != 1 {
		t.Fatal("stale attempt must not call Cloud or delete Relay rows")
	}
	processRootDeletion(ctx, newAttempt)
	jobs, err := loadRootDeletions(ctx, "alice")
	if err != nil || len(jobs) != 1 || jobs[0].Status != "succeeded" || jobs[0].DeletedFiles != 7 {
		t.Fatalf("completion snapshot: %+v %v", jobs, err)
	}
	if calls != 1 || files("alice") != 0 || files("bob") != 1 {
		t.Fatal("wrong deletion count or cross-tenant cleanup")
	}
	seed("alice", "repo-a")
	processRootDeletion(ctx, newAttempt)
	if calls != 1 || files("alice") != 1 {
		t.Fatal("completed attempt replay deleted a rebuilt index")
	}

	seed("uncertain", "repo-b")
	if _, err := enqueueRootDeletion(ctx, "uncertain", "owner", "repo-b"); err != nil {
		t.Fatal(err)
	}
	uncertain, err := claimRootDeletion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	lceClearIndexRoot = func(context.Context, string, string) (*mcpToolResult, error) {
		return nil, errors.New("response lost")
	}
	processRootDeletion(ctx, uncertain)
	jobs, err = loadRootDeletions(ctx, "uncertain")
	if err != nil || len(jobs) != 1 || jobs[0].Status != "running" || jobs[0].Error == "" {
		t.Fatalf("unknown result must retain the marker: %+v %v", jobs, err)
	}
	if _, err := acquireExclusiveIndexOperation(ctx, "uncertain", "create-job"); !errors.Is(err, errIndexOperationBusy) {
		t.Fatalf("uncertain deletion permitted new indexing: %v", err)
	}
	if files("uncertain") != 1 {
		t.Fatal("Cloud communication failure removed Relay state")
	}

	// A cleanup failure after Cloud success must roll back both the file deletes
	// and the terminal status; the durable marker remains for a later retry.
	lceClearIndexRoot = func(context.Context, string, string) (*mcpToolResult, error) {
		return &mcpToolResult{Content: []byte(`{"deleted_files":1}`)}, nil
	}
	if _, err := db.Exec(`CREATE FUNCTION reject_deletion_success() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN IF NEW.status = 'succeeded' THEN RAISE EXCEPTION 'test rollback'; END IF; RETURN NEW; END $$;
		CREATE TRIGGER reject_deletion_success BEFORE UPDATE ON root_deletion_jobs
		FOR EACH ROW EXECUTE FUNCTION reject_deletion_success()`); err != nil {
		t.Fatal(err)
	}
	processRootDeletion(ctx, uncertain)
	if files("uncertain") != 1 {
		t.Fatal("Relay rows escaped a failed completion transaction")
	}
	if _, err := db.Exec(`DROP TRIGGER reject_deletion_success ON root_deletion_jobs`); err != nil {
		t.Fatal(err)
	}
	processRootDeletion(ctx, uncertain)
	if files("uncertain") != 0 {
		t.Fatal("retry did not complete after rollback")
	}
	if rootDeletionClaimDuration <= rootDeletionRunTimeout+time.Minute {
		t.Fatal("recovery grace must outlive the worker deadline")
	}
}
