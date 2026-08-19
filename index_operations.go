package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

type indexOperationMode string

const (
	indexOperationShared    indexOperationMode = "shared"
	indexOperationExclusive indexOperationMode = "exclusive"

	indexOperationLeaseDuration = 2 * time.Minute
	indexOperationRenewInterval = 30 * time.Second
	indexOperationAcquirePoll   = 100 * time.Millisecond
)

type indexOperationLease struct {
	userID  string
	token   string
	ctx     context.Context
	cancel  context.CancelFunc
	stop    chan struct{}
	done    chan struct{}
	release sync.Once
}

func (l *indexOperationLease) Context() context.Context {
	return l.ctx
}

func (l *indexOperationLease) Release() {
	l.release.Do(func() {
		close(l.stop)
		<-l.done
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = db.ExecContext(ctx,
			`DELETE FROM index_operation_leases WHERE user_id = $1 AND lease_token = $2`,
			l.userID, l.token,
		)
		l.cancel()
	})
}

func acquireSharedIndexOperation(ctx context.Context, userID, resource, kind string) (*indexOperationLease, error) {
	// "shared" means shared at the user scope: different resources may run in
	// parallel, while the SQL conflict predicate still serializes one resource.
	return acquireIndexOperation(ctx, userID, resource, kind, indexOperationShared)
}

func acquireExclusiveIndexOperation(ctx context.Context, userID, kind string) (*indexOperationLease, error) {
	return acquireIndexOperation(ctx, userID, "*", kind, indexOperationExclusive)
}

func acquireIndexOperation(
	ctx context.Context,
	userID, resource, kind string,
	mode indexOperationMode,
) (*indexOperationLease, error) {
	if userID == "" || resource == "" || kind == "" {
		return nil, fmt.Errorf("invalid index operation lease identity")
	}
	token := uuid.NewString()
	for {
		acquired, err := tryAcquireIndexOperation(ctx, userID, resource, kind, token, mode)
		if err != nil {
			return nil, err
		}
		if acquired {
			leaseCtx, cancel := context.WithCancel(ctx)
			lease := &indexOperationLease{
				userID: userID,
				token:  token,
				ctx:    leaseCtx,
				cancel: cancel,
				stop:   make(chan struct{}),
				done:   make(chan struct{}),
			}
			go lease.renew()
			return lease, nil
		}

		timer := time.NewTimer(indexOperationAcquirePoll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func tryAcquireIndexOperation(
	ctx context.Context,
	userID, resource, kind, token string,
	mode indexOperationMode,
) (bool, error) {
	tx, err := beginLockedIndexUserTx(ctx, userID)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	var acquired bool
	if err := tx.QueryRowContext(ctx, `
		WITH expired AS (
			DELETE FROM index_operation_leases
			WHERE user_id = $2 AND lease_expires_at <= NOW()
		), inserted AS (
			INSERT INTO index_operation_leases (
				lease_token, user_id, resource, mode, kind, lease_expires_at
			)
			SELECT $1, $2, $3, $4::text, $5, NOW() + ($6 * INTERVAL '1 millisecond')
			WHERE NOT EXISTS (
				SELECT 1 FROM index_operation_leases
				WHERE user_id = $2
				  AND lease_expires_at > NOW()
				  AND (mode = $7::text OR $4::text = $7::text OR resource = $3)
			)
			RETURNING 1
		)
		SELECT EXISTS(SELECT 1 FROM inserted)
	`, token, userID, resource, mode, kind, indexOperationLeaseDuration.Milliseconds(), indexOperationExclusive).Scan(&acquired); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return acquired, nil
}

func (l *indexOperationLease) renew() {
	defer close(l.done)
	deadline := time.Now().Add(indexOperationLeaseDuration)
	ticker := time.NewTicker(indexOperationRenewInterval)
	defer ticker.Stop()

	for {
		select {
		case <-l.stop:
			return
		case <-l.ctx.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			result, err := db.ExecContext(ctx, `
				UPDATE index_operation_leases
				SET lease_expires_at = NOW() + ($3 * INTERVAL '1 millisecond')
				WHERE user_id = $1 AND lease_token = $2
			`, l.userID, l.token, indexOperationLeaseDuration.Milliseconds())
			cancel()
			if err == nil {
				rows, rowsErr := result.RowsAffected()
				if rowsErr == nil && rows == 1 {
					deadline = time.Now().Add(indexOperationLeaseDuration)
					continue
				}
			}
			if time.Now().Add(indexOperationRenewInterval).After(deadline) {
				l.cancel()
				return
			}
		}
	}
}

func indexJobOperationResource(jobID string) string {
	return "job:" + jobID
}

func indexWorkspaceOperationResource(workspaceID string) string {
	return "workspace:" + workspaceID
}
