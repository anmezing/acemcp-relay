package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestPlatformConfigPostgres(t *testing.T) {
	withIsolatedRelayPostgres(t)
	ctx := context.Background()
	seed := func() {
		t.Helper()
		if _, err := db.Exec(`INSERT INTO index_workspaces(user_id,workspace_id,root_id) VALUES ('user','repo','root');
   INSERT INTO indexed_files(user_id,workspace_id,path,hash,size) VALUES ('user','repo','a.ts','hash',10)`); err != nil {
			t.Fatal(err)
		}
	}
	count := func() int {
		t.Helper()
		var n int
		if err := db.QueryRow("SELECT COUNT(*) FROM indexed_files").Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	seed()
	lease, err := tryExclusiveIndexOperation(ctx, "user", "test")
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.NewString()
	if _, err := enqueuePlatformConfigJob(ctx, id, "embeddings", true); !errors.Is(err, errIndexOperationBusy) {
		t.Fatalf("admitted reset while lease active: %v", err)
	}
	lease.Release()
	accepted, err := enqueuePlatformConfigJob(ctx, id, "embeddings", true)
	if err != nil || accepted.Status != "pending" {
		t.Fatalf("admission: %+v %v", accepted, err)
	}
	if _, err := tryExclusiveIndexOperation(ctx, "another-user", "create-job"); !errors.Is(err, errIndexOperationBusy) {
		t.Fatalf("persistent fence did not reject new operations: %v", err)
	}
	if _, err := enqueueRootDeletion(ctx, "another-user", "owner", "root"); !errors.Is(err, errIndexOperationBusy) {
		t.Fatalf("persistent fence admitted deletion: %v", err)
	}
	claimed, err := claimPlatformConfigJob(ctx)
	if err != nil || claimed.ID != id {
		t.Fatalf("claim: %+v %v", claimed, err)
	}
	receipt := cloudPlatformConfigOperation{OperationID: id, State: "completed", EmbeddingChanged: true, Result: json.RawMessage(`{"deletedRoots":1,"deletedFiles":1,"deletedCachedEmbeddings":0}`)}
	if _, err := db.Exec(`CREATE FUNCTION reject_config_success() RETURNS trigger LANGUAGE plpgsql AS $$
  BEGIN IF NEW.status='succeeded' THEN RAISE EXCEPTION 'receipt failure'; END IF; RETURN NEW; END $$;
  CREATE TRIGGER reject_config_success BEFORE UPDATE ON platform_config_jobs FOR EACH ROW EXECUTE FUNCTION reject_config_success()`); err != nil {
		t.Fatal(err)
	}
	if err := finishPlatformConfigJob(ctx, claimed, receipt); err == nil {
		t.Fatal("expected injected failure")
	}
	if count() != 1 {
		t.Fatal("cleanup escaped failed receipt transaction")
	}
	if _, err := db.Exec("DROP TRIGGER reject_config_success ON platform_config_jobs"); err != nil {
		t.Fatal(err)
	}
	if err := finishPlatformConfigJob(ctx, claimed, receipt); err != nil {
		t.Fatal(err)
	}
	if count() != 0 {
		t.Fatal("confirmed cloud commit did not reconcile local state")
	}
	seed()
	if err := finishPlatformConfigJob(ctx, claimed, receipt); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("completed replay was not fenced: %v", err)
	}
	if count() != 1 {
		t.Fatal("duplicate cleanup deleted a rebuilt index")
	}
	repeated, err := recoverPlatformConfigJob(ctx, id)
	if err != nil || repeated.Status != "succeeded" {
		t.Fatalf("recovery replay: %+v %v", repeated, err)
	}
	if _, err := db.Exec(`INSERT INTO platform_config_jobs(id,section,embedding_changed,status,attempt_count,claim_until)
  VALUES ($1,'embeddings',TRUE,'running',$2,NOW()-INTERVAL '1 second')`, uuid.NewString(), platformConfigJobMaxAttempts); err != nil {
		t.Fatal(err)
	}
	if _, err := claimPlatformConfigJob(ctx); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("exhausted job reclaimed: %v", err)
	}
	paused, err := loadPlatformConfigJob(ctx, "latest")
	if err != nil || !paused.RecoveryRequired {
		t.Fatalf("crash recovery missing: %+v %v", paused, err)
	}
	if _, err := tryExclusiveIndexOperation(ctx, "user", "create-job"); !errors.Is(err, errIndexOperationBusy) {
		t.Fatalf("paused config lost fence: %v", err)
	}
	resumed, err := recoverPlatformConfigJob(ctx, paused.ID)
	if err != nil || resumed.RecoveryRequired || resumed.AttemptCount != 0 {
		t.Fatalf("recovery: %+v %v", resumed, err)
	}
}
