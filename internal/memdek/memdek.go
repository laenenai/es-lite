// Package memdek is an in-memory shred.WrappedDEKStore for es-lite's own tests and
// examples. It lives under internal/ ON PURPOSE: external services must NOT be able to
// wire an in-memory wrapped-DEK store into a real process — a restart would lose every
// wrapped DEK and orphan that workspace's ciphertext (2026-08-05 review E4). Production
// persists wrapped DEKs in the same database as the events (postgres.Store, which is its
// own WrappedDEKStore).
package memdek

import (
	"context"
	"sync"
)

// Store is an in-memory wrapped-DEK store. Zero value is not usable; call New.
type Store struct {
	mu      sync.Mutex
	wrapped map[string][]byte
}

// New creates an empty in-memory wrapped-DEK store.
func New() *Store { return &Store{wrapped: make(map[string][]byte)} }

// LoadWrappedDEK returns the stored wrapped DEK for a workspace, if any.
func (m *Store) LoadWrappedDEK(_ context.Context, workspaceID string) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	w, ok := m.wrapped[workspaceID]
	return w, ok, nil
}

// SaveWrappedDEK is insert-if-absent — it keeps the FIRST wrapped DEK stored for a
// workspace and ignores later writes, matching the production store's
// `ON CONFLICT (workspace_id) DO NOTHING`. This is load-bearing: under a first-write
// race two callers mint different DEKs, and both must converge on the one actually
// persisted (the Shredder re-loads after saving), or the loser would encrypt under a
// key that was never stored.
func (m *Store) SaveWrappedDEK(_ context.Context, workspaceID string, wrapped []byte, _ int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.wrapped[workspaceID]; !exists {
		m.wrapped[workspaceID] = wrapped
	}
	return nil
}

// DeleteWrappedDEK drops the workspace's wrapped DEK.
func (m *Store) DeleteWrappedDEK(_ context.Context, workspaceID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.wrapped, workspaceID)
	return nil
}
