package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/laenenai/es-lite/es"
)

// DedupStore is the es.Deduplicator over command_dedup (migration 0008).
// One database is one workspace (0001's rule), so the table has no
// workspace_id and no RLS: the (stream_id, command_id) key alone
// identifies a record, and the database file's own boundary is the
// workspace boundary Postgres's policy enforces.
type DedupStore struct {
	store *Store
}

var _ es.Deduplicator = (*DedupStore)(nil)

// NewDedupStore returns a DedupStore backed by s.
func NewDedupStore(s *Store) *DedupStore { return &DedupStore{store: s} }

// Lookup implements es.Deduplicator: the stored record and found=true
// when the key holds one, (zero, false, nil) when it does not.
func (d *DedupStore) Lookup(ctx context.Context, key es.DedupKey) (es.DedupRecord, bool, error) {
	var rec es.DedupRecord
	err := d.store.db.QueryRowContext(ctx,
		`SELECT fingerprint, response FROM command_dedup
		  WHERE stream_id = ? AND command_id = ?`,
		key.StreamID, key.CommandID,
	).Scan(&rec.Fingerprint, &rec.Response)
	if errors.Is(err, sql.ErrNoRows) {
		return es.DedupRecord{}, false, nil
	}
	if err != nil {
		return es.DedupRecord{}, false, fmt.Errorf("sqlite: dedup: lookup: %w", err)
	}
	return rec, true, nil
}

// Save implements es.Deduplicator. Write-once: INSERT OR IGNORE, so a
// Save whose key already holds a record stores nothing and returns nil —
// the conflict is silent by contract (es.Deduplicator); the first
// completed response wins, and the caller that needs to know what is
// stored re-Lookups.
func (d *DedupStore) Save(ctx context.Context, key es.DedupKey, rec es.DedupRecord) error {
	_, err := d.store.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO command_dedup (stream_id, command_id, fingerprint, response)
		 VALUES (?, ?, ?, ?)`,
		key.StreamID, key.CommandID, rec.Fingerprint, rec.Response,
	)
	if err != nil {
		return fmt.Errorf("sqlite: dedup: save: %w", err)
	}
	return nil
}
