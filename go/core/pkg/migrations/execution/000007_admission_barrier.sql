-- Fork-owned admission barrier for maintenance/drain. A single-row table: the
-- fixed id=1 row is the global "admissions closed" switch. The three durable
-- admission transactions (session insert, dispatch reserve, RESUME admit) lock
-- this row FOR UPDATE and read `closed`; the close/reopen writers lock the same
-- row to flip it. Because every admission serializes on one row, an admission
-- can never interleave between "read open" and "commit" — closure and admission
-- are atomic with respect to each other. The lock is database-only; no lock is
-- held across a provider/runtime call.
--
-- The barrier distinguishes NEW admission from recovery: it is consulted only
-- at the point a brand-new session/turn/resume is durably admitted. Retries of
-- an already-accepted message (ReserveSessionDispatch's ErrMessageAccepted
-- path), reconciliation of an in-flight CREATE, TaskStore callback writes,
-- delete/suspend/cancel, and the leader-elected workers never touch it.
-- +goose Up
CREATE TABLE admission_barrier (
    -- A single global row: id is always 1.
    id         SMALLINT    PRIMARY KEY CHECK (id = 1),
    closed     BOOLEAN     NOT NULL DEFAULT false,
    closed_at  TIMESTAMPTZ,
    reason     TEXT        NOT NULL DEFAULT ''
);
INSERT INTO admission_barrier (id, closed) VALUES (1, false);

-- +goose Down
DROP TABLE IF EXISTS admission_barrier;
