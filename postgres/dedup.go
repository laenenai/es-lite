package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/laenenai/es-lite/es"
)

// DedupStore is the es.Deduplicator over command_dedup (migration 0010)
// for one workspace. Every operation runs in a short transaction that
// sets app.workspace_id — LookupClaim's shape — so the table's RLS, the
// workspace arm ONLY with no bypass arm (see 0010's header), confines
// both halves of the flow to the workspace's own records: a workspace
// replays its own completed responses and nobody else's, not even the
// relay role under app.bypass_rls.
type DedupStore struct {
	w *Workspace
}

var _ es.Deduplicator = (*DedupStore)(nil)

// NewDedupStore returns the dedup store bound to w's workspace.
func NewDedupStore(w *Workspace) *DedupStore { return &DedupStore{w: w} }

// Lookup implements es.Deduplicator: the stored record and found=true
// when the key holds one, (zero, false, nil) when it does not.
func (d *DedupStore) Lookup(ctx context.Context, key es.DedupKey) (es.DedupRecord, bool, error) {
	var rec es.DedupRecord
	tx, err := d.w.store.pool.Begin(ctx)
	if err != nil {
		return es.DedupRecord{}, false, err
	}
	defer tx.Rollback(ctx)
	if err := setWorkspace(ctx, tx, d.w.ws); err != nil {
		return es.DedupRecord{}, false, err
	}
	err = tx.QueryRow(ctx,
		`SELECT fingerprint, response FROM command_dedup
		  WHERE workspace_id = $1 AND stream_id = $2 AND command_id = $3`,
		d.w.ws, key.StreamID, key.CommandID,
	).Scan(&rec.Fingerprint, &rec.Response)
	if errors.Is(err, pgx.ErrNoRows) {
		return es.DedupRecord{}, false, nil
	}
	if err != nil {
		return es.DedupRecord{}, false, fmt.Errorf("postgres: dedup: lookup: %w", err)
	}
	return rec, true, nil
}

// Save implements es.Deduplicator. Write-once: INSERT ... ON CONFLICT DO
// NOTHING, so a Save whose key already holds a record stores nothing and
// returns nil — the conflict is silent by contract (es.Deduplicator);
// the first completed response wins, and the caller that needs to know
// what is stored re-Lookups.
func (d *DedupStore) Save(ctx context.Context, key es.DedupKey, rec es.DedupRecord) error {
	tx, err := d.w.store.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := setWorkspace(ctx, tx, d.w.ws); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO command_dedup (workspace_id, stream_id, command_id, fingerprint, response)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT DO NOTHING`,
		d.w.ws, key.StreamID, key.CommandID, rec.Fingerprint, rec.Response,
	); err != nil {
		return fmt.Errorf("postgres: dedup: save: %w", err)
	}
	return tx.Commit(ctx)
}
