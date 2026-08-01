// Package memory is an in-process KeyStore for development and tests. It
// mimics OpenBao Transit's semantics — a per-workspace KEK that never
// leaves the store, wrap/unwrap, and irreversible key deletion — without
// any external dependency. NOT for production: keys live only in memory
// and are lost on restart.
package memory

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"sync"

	"github.com/laenenai/es-lite/keystore"
)

// Store is an in-memory KeyStore. The zero value is not usable; call New.
type Store struct {
	mu   sync.Mutex
	keks map[string][]byte // workspaceID -> 32-byte KEK; deletion = shred
}

var _ keystore.KeyStore = (*Store)(nil)

// New creates an empty in-memory KeyStore.
func New() *Store { return &Store{keks: make(map[string][]byte)} }

// GenerateDEK implements keystore.KeyStore.
func (s *Store) GenerateDEK(ctx context.Context, workspaceID string) ([]byte, []byte, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	kek, ok := s.keks[workspaceID]
	if !ok {
		kek = make([]byte, keystore.DEKSize)
		if _, err := rand.Read(kek); err != nil {
			return nil, nil, 0, fmt.Errorf("memory: gen kek: %w", err)
		}
		s.keks[workspaceID] = kek
	}

	dek := make([]byte, keystore.DEKSize)
	if _, err := rand.Read(dek); err != nil {
		return nil, nil, 0, fmt.Errorf("memory: gen dek: %w", err)
	}
	wrapped, err := wrap(kek, dek)
	if err != nil {
		return nil, nil, 0, err
	}
	return dek, wrapped, 1, nil
}

// UnwrapDEK implements keystore.KeyStore.
func (s *Store) UnwrapDEK(ctx context.Context, workspaceID string, wrapped []byte) ([]byte, error) {
	s.mu.Lock()
	kek, ok := s.keks[workspaceID]
	s.mu.Unlock()
	if !ok {
		// KEK destroyed (or never existed) => the wrapped DEK is useless.
		return nil, fmt.Errorf("%w: %s", keystore.ErrShredded, workspaceID)
	}
	dek, err := unwrap(kek, wrapped)
	if err != nil {
		return nil, fmt.Errorf("memory: unwrap: %w", err)
	}
	return dek, nil
}

// ForgetWorkspace implements keystore.KeyStore. Destroying the KEK makes
// every wrapped DEK for the workspace permanently un-unwrappable.
func (s *Store) ForgetWorkspace(ctx context.Context, workspaceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.keks, workspaceID)
	return nil
}

func wrap(kek, dek []byte) ([]byte, error) {
	aead, err := newAEAD(kek)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return aead.Seal(nonce, nonce, dek, nil), nil
}

func unwrap(kek, blob []byte) ([]byte, error) {
	aead, err := newAEAD(kek)
	if err != nil {
		return nil, err
	}
	ns := aead.NonceSize()
	if len(blob) < ns {
		return nil, fmt.Errorf("wrapped dek too short")
	}
	return aead.Open(nil, blob[:ns], blob[ns:], nil)
}

func newAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
