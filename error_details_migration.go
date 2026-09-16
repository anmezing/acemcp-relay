package main

import (
	"context"
	"database/sql/driver"
	"fmt"
	"time"
)

func ensureErrorDetailsRequestIndex() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	// Concurrent builds cannot run inside a transaction. Hold a session lock on
	// this dedicated connection so two starting replicas cannot drop each other's build.
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock(1163149510)`); err != nil {
		return err
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if _, err := conn.ExecContext(cleanup, `SELECT pg_advisory_unlock(1163149510)`); err != nil {
			_ = conn.Raw(func(interface{}) error { return driver.ErrBadConn })
		}
	}()
	var invalid bool
	if err := conn.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_index
		WHERE indexrelid=to_regclass('idx_error_details_request_id') AND NOT indisvalid)`).Scan(&invalid); err != nil {
		return err
	}
	if invalid {
		if _, err := conn.ExecContext(ctx, `DROP INDEX CONCURRENTLY IF EXISTS idx_error_details_request_id`); err != nil {
			return fmt.Errorf("remove interrupted error-details index: %w", err)
		}
	}
	if _, err := conn.ExecContext(ctx, `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_error_details_request_id ON error_details(request_id)`); err != nil {
		return fmt.Errorf("index error details: %w", err)
	}
	return nil
}
