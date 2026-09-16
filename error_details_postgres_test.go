package main

import "testing"

func TestErrorDetailsPostgres(t *testing.T) {
	withIsolatedRelayPostgres(t)
	if _, err := db.Exec(`CREATE TABLE request_logs(id UUID PRIMARY KEY);
		CREATE TABLE error_details(id SERIAL PRIMARY KEY, request_id UUID REFERENCES request_logs(id), error TEXT);
		INSERT INTO request_logs VALUES ('aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa');
		INSERT INTO error_details(request_id,error) VALUES ('aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa','x')`); err != nil {
		t.Fatal(err)
	}
	// A real failed concurrent build leaves the catalog entry marked invalid.
	if _, err := db.Exec(`CREATE INDEX CONCURRENTLY idx_error_details_request_id ON error_details ((1/(length(error)-1)))`); err == nil {
		t.Fatal("expected failed index build")
	}
	for range 2 {
		if err := migrateErrorDetailsTable(); err != nil {
			t.Fatal(err)
		}
	}
	var valid, cascading bool
	if err := db.QueryRow(`SELECT indisvalid FROM pg_index WHERE indexrelid='idx_error_details_request_id'::regclass`).Scan(&valid); err != nil || !valid {
		t.Fatalf("index validity: %v %v", valid, err)
	}
	if err := db.QueryRow(`SELECT convalidated AND confdeltype='c' FROM pg_constraint WHERE conrelid='error_details'::regclass AND contype='f'`).Scan(&cascading); err != nil || !cascading {
		t.Fatalf("constraint: %v %v", cascading, err)
	}
	if _, err := db.Exec(`DELETE FROM request_logs`); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM error_details`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("cascade count=%d error=%v", count, err)
	}
}
