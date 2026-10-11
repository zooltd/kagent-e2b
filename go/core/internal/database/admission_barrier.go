package database

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// ErrAdmissionClosed reports that control-plane admission of new work is
// closed for maintenance. It is raised only at the durable admission points for
// brand-new sessions, turns, and resumes — never on the recovery, callback,
// settle/cancel/delete, or worker paths. Callers map it to a temporary-
// unavailable response so a client can retry after the drain reopens.
var ErrAdmissionClosed = errors.New("admissions closed for maintenance")

// admissionBarrierID is the single global barrier row's fixed id.
const admissionBarrierID = 1

// requireAdmissionsOpen locks the global admission_barrier row FOR UPDATE inside
// the caller's transaction and returns ErrAdmissionClosed when it is closed.
// Locking the same row the close/reopen writers lock serializes admission with
// closure: an admission cannot interleave between "read open" and "commit". The
// lock is held only for the life of the admission transaction — it is released
// at commit and never spans a provider/runtime call. Call this only at points
// that durably admit NEW work; recovery and settlement paths must not call it.
//
// Deadlock safety does not depend on the barrier being the transaction's last
// lock (it is not — subsequent inserts take more row locks). It rests on the
// writers being single-row: SetAdmissionsClosed locks ONLY this row, so a close
// never waits on an admission's other locks. Two admissions can still contend
// with each other on shared rows as before — the barrier adds no new cycle.
func requireAdmissionsOpen(ctx context.Context, tx pgx.Tx) error {
	closed, err := queryOne(ctx, tx, `
		SELECT closed FROM admission_barrier WHERE id = $1 FOR UPDATE
	`, pgx.RowTo[bool], admissionBarrierID)
	if err != nil {
		return err
	}
	if closed {
		return ErrAdmissionClosed
	}
	return nil
}

// SetAdmissionsClosed flips the global admission barrier. It locks the barrier
// row FOR UPDATE so the flip serializes with every in-flight admission holding
// the same row — a new admission either sees "open" and commits before the
// close lock is granted, or waits and then sees "closed". closing=false reopens
// admissions. The operation is idempotent: closing twice or reopening an open
// barrier is a no-op that returns the current state.
func (c *Client) SetAdmissionsClosed(ctx context.Context, closed bool, reason string) (bool, error) {
	var nowClosed bool
	err := c.withTx(ctx, func(tx pgx.Tx) error {
		res, err := queryOne(ctx, tx, `
			UPDATE admission_barrier
			   SET closed = $2,
			       closed_at = CASE WHEN $2 AND NOT closed THEN now() ELSE closed_at END,
			       reason = CASE WHEN $2 THEN $3 ELSE '' END
			WHERE id = $1
			RETURNING closed
		`, pgx.RowTo[bool], admissionBarrierID, closed, reason)
		if err != nil {
			return err
		}
		nowClosed = res
		return nil
	})
	if err != nil {
		return false, err
	}
	return nowClosed, nil
}

// CheckAdmissionsOpen serializes a NEW-work admission decision with closure.
// Unlike AdmissionsClosed (a non-locking observation read), this runs
// requireAdmissionsOpen inside its own transaction so the read takes the
// barrier row FOR UPDATE and commits — a concurrent SetAdmissionsClosed either
// completes first (this then sees closed) or blocks until this commits, never
// interleaving. The lock is released at commit, before the caller performs any
// runtime/dial work: serialization is over the admission decision, not the
// subsequent connection. Use it for admission points that open a runtime
// connection but have no durable row of their own to write (a live-task
// subscription); points that already transact should call requireAdmissionsOpen
// inside that transaction instead.
func (c *Client) CheckAdmissionsOpen(ctx context.Context) error {
	return c.withTx(ctx, func(tx pgx.Tx) error {
		return requireAdmissionsOpen(ctx, tx)
	})
}

// AdmissionsClosed reports whether new-work admission is currently closed. It is
// an observation read for operators/reconciliation — it does NOT take the
// barrier lock and so does not serialize; admission decisions always go through
// requireAdmissionsOpen inside the admission transaction.
//
// A missing barrier row is not "open": the migration creates the table and its
// single row in one transaction, so an absent row means the schema is broken or
// unmigrated. The error is propagated rather than silently reporting open,
// which would mislead a drain check into thinking admissions were flowing.
func (c *Client) AdmissionsClosed(ctx context.Context) (bool, error) {
	closed, err := queryOne(ctx, c.db, `
		SELECT closed FROM admission_barrier WHERE id = $1
	`, pgx.RowTo[bool], admissionBarrierID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("admission_barrier row %d is missing: %w", admissionBarrierID, err)
	}
	return closed, err
}
