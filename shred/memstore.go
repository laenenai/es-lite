package shred

import (
	"context"
	"sync"
)

// MemDEKStore is an in-memory WrappedDEKStore for tests and single-process
// examples. Production deployments persist wrapped DEKs in the same
// database as the events (a workspace_keys table).
type MemDEKStore struct {
	mu      sync.Mutex
	wrapped map[string][]byte
}

var _ WrappedDEKStore = (*MemDEKStore)(nil)

// NewMemDEKStore creates an empty in-memory wrapped-DEK store.
func NewMemDEKStore() *MemDEKStore {
	return &MemDEKStore{wrapped: make(map[string][]byte)}
}

func (m *MemDEKStore) LoadWrappedDEK(ctx context.Context, workspaceID string) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	w, ok := m.wrapped[workspaceID]
	return w, ok, nil
}

func (m *MemDEKStore) SaveWrappedDEK(ctx context.Context, workspaceID string, wrapped []byte, kekVersion int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.wrapped[workspaceID] = wrapped
	return nil
}

func (m *MemDEKStore) DeleteWrappedDEK(ctx context.Context, workspaceID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.wrapped, workspaceID)
	return nil
}
