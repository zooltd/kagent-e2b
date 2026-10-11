package database

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Client persists control-plane state in PostgreSQL. Callers define the narrow
// interfaces they need; SQL rows and protobuf encoding stay inside the store.
type Client struct {
	db *pgxpool.Pool
}

// NewClient wraps an existing PostgreSQL pool without connecting or migrating. The caller
// owns the pool and must close it.
func NewClient(db *pgxpool.Pool) *Client {
	return &Client{db: db}
}

// withTx commits all callback writes together on success and rolls them back on failure,
// returning callback or transaction errors. The callback must keep external network work
// outside the transaction.
func (c *Client) withTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// notFoundOr maps pgx.ErrNoRows to ErrNotFound and leaves all other errors, including nil,
// unchanged.
func notFoundOr(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	return err
}

// derefStr returns the pointed-to string, or an empty string for nil.
func derefStr(s *string) string {
	if s != nil {
		return *s
	}
	return ""
}

// nullString maps an empty string to NULL for columns that distinguish NULL
// from empty (the provider-discriminated ActorTemplate identity). Returns the
// string unchanged when non-empty.
func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// revisionProvider normalizes the revision provider discriminator, defaulting
// an empty value to Substrate so existing callers keep their provider.
func revisionProvider(provider string) string {
	if provider == "" {
		return ExecutionProviderSubstrate
	}
	return provider
}

// revisionActorTemplateUID maps the ActorTemplate UID column: an E2B revision
// stores NULL, while a Substrate revision stores the ” sentinel for "no UID
// yet" as the original schema required (actor_template_uid NOT NULL DEFAULT
// ”). Callers must not pass NULL for Substrate or ” for E2B.
func revisionActorTemplateUID(provider, uid string) any {
	if revisionProvider(provider) == ExecutionProviderE2B {
		return nil
	}
	return uid
}
