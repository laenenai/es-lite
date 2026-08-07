// Package shred is es-lite's envelope-encryption layer for per-workspace
// crypto-shredding (ADR 0004). A Shredder turns a keystore.KeyStore plus a
// place to persist wrapped DEKs into per-workspace Ciphers that encrypt and
// decrypt event payloads. Forgetting a workspace destroys its KEK and
// evicts its cached DEK, making all of its ciphertext unrecoverable.
//
// The storage adapter calls Cipher(ctx, workspaceID) on the write path to
// encrypt each payload and on the read path to decrypt it; the Decider only
// ever sees plaintext.
package shred

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"

	"github.com/laenenai/es-lite/keystore"
)

// WrappedDEKStore persists one wrapped DEK per workspace — a small table in
// the same database as the events (workspace_keys). Kept separate from the
// KeyStore because the wrapped bytes are ordinary data, safe to store next
// to the ciphertext they protect (they are useless without the KEK).
type WrappedDEKStore interface {
	LoadWrappedDEK(ctx context.Context, workspaceID string) (wrapped []byte, ok bool, err error)
	SaveWrappedDEK(ctx context.Context, workspaceID string, wrapped []byte, kekVersion int) error
	DeleteWrappedDEK(ctx context.Context, workspaceID string) error
}

// Cipher encrypts and decrypts payloads for one workspace using its DEK.
// Safe for concurrent use.
type Cipher struct {
	aead cipher.AEAD
}

// Encrypt seals plaintext, prefixing a random nonce.
func (c Cipher) Encrypt(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("shred: nonce: %w", err)
	}
	return c.aead.Seal(nonce, nonce, plaintext, nil), nil
}

// Decrypt opens a nonce-prefixed ciphertext.
func (c Cipher) Decrypt(blob []byte) ([]byte, error) {
	ns := c.aead.NonceSize()
	if len(blob) < ns {
		return nil, errors.New("shred: ciphertext too short")
	}
	pt, err := c.aead.Open(nil, blob[:ns], blob[ns:], nil)
	if err != nil {
		return nil, fmt.Errorf("shred: open: %w", err)
	}
	return pt, nil
}

// Shredder builds per-workspace Ciphers, caching plaintext DEKs so the hot
// path does no KMS round-trips.
type Shredder struct {
	ks   keystore.KeyStore
	deks WrappedDEKStore

	mu    sync.Mutex
	cache map[string][]byte // workspaceID -> plaintext DEK
}

// New wires a Shredder over a KeyStore and a wrapped-DEK store.
func New(ks keystore.KeyStore, deks WrappedDEKStore) *Shredder {
	return &Shredder{ks: ks, deks: deks, cache: make(map[string][]byte)}
}

// WriteCipher returns the workspace's Cipher for the write path, minting and
// persisting a DEK the first time the workspace is written.
func (s *Shredder) WriteCipher(ctx context.Context, workspaceID string) (Cipher, error) {
	return s.cipher(ctx, workspaceID, true)
}

// ReadCipher returns the workspace's Cipher for the read path. It never
// provisions a DEK: a workspace with no wrapped DEK is treated as shredded
// (keystore.ErrShredded), so a forgotten workspace's ciphertext is reported
// as erased rather than silently re-keyed. Callers should only invoke this
// when there is actually ciphertext to decrypt (an empty stream needs no
// cipher).
func (s *Shredder) ReadCipher(ctx context.Context, workspaceID string) (Cipher, error) {
	return s.cipher(ctx, workspaceID, false)
}

// Cipher is the write-path cipher (kept for convenience/back-compat).
func (s *Shredder) Cipher(ctx context.Context, workspaceID string) (Cipher, error) {
	return s.WriteCipher(ctx, workspaceID)
}

func (s *Shredder) cipher(ctx context.Context, workspaceID string, provision bool) (Cipher, error) {
	dek, err := s.dek(ctx, workspaceID, provision)
	if err != nil {
		return Cipher{}, err
	}
	aead, err := newAEAD(dek)
	if err != nil {
		return Cipher{}, err
	}
	return Cipher{aead: aead}, nil
}

// dek returns the workspace's plaintext DEK, from cache or by unwrapping the
// persisted wrapped DEK. When no wrapped DEK exists, provision controls the
// outcome: true mints one (write path); false reports keystore.ErrShredded
// (read path — the workspace was forgotten, or its key is lost).
func (s *Shredder) dek(ctx context.Context, workspaceID string, provision bool) ([]byte, error) {
	s.mu.Lock()
	if dek, ok := s.cache[workspaceID]; ok {
		s.mu.Unlock()
		return dek, nil
	}
	s.mu.Unlock()

	wrapped, ok, err := s.deks.LoadWrappedDEK(ctx, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("shred: load wrapped dek: %w", err)
	}

	var dek []byte
	if !ok {
		if !provision {
			return nil, fmt.Errorf("%w: %s", keystore.ErrShredded, workspaceID)
		}
		// First write for this workspace: mint a DEK and persist it. SaveWrappedDEK is
		// insert-if-absent (ON CONFLICT (workspace_id) DO NOTHING), so under a concurrent
		// first write another writer may already have persisted a DIFFERENT wrapped DEK.
		// The PERSISTED bytes are the single source of truth: re-load and unwrap THOSE,
		// never the just-minted plaintext — otherwise the writer that lost the race would
		// encrypt every event under a key that was never stored, leaving that ciphertext
		// permanently undecryptable. Whoever wins, all writers converge on the stored key.
		// (The loser's minted DEK is simply discarded; a wasted mint, not lost data.)
		if _, wrappedNew, kekVersion, err := s.ks.GenerateDEK(ctx, workspaceID); err != nil {
			return nil, fmt.Errorf("shred: generate dek: %w", err)
		} else if err := s.deks.SaveWrappedDEK(ctx, workspaceID, wrappedNew, kekVersion); err != nil {
			return nil, fmt.Errorf("shred: save wrapped dek: %w", err)
		}
		persisted, pok, perr := s.deks.LoadWrappedDEK(ctx, workspaceID)
		if perr != nil {
			return nil, fmt.Errorf("shred: reload wrapped dek: %w", perr)
		}
		if !pok {
			return nil, fmt.Errorf("shred: wrapped dek missing immediately after save: %s", workspaceID)
		}
		dek, err = s.ks.UnwrapDEK(ctx, workspaceID, persisted)
		if err != nil {
			return nil, err
		}
	} else {
		dek, err = s.ks.UnwrapDEK(ctx, workspaceID, wrapped)
		if err != nil {
			return nil, err // includes keystore.ErrShredded
		}
	}

	s.mu.Lock()
	s.cache[workspaceID] = dek
	s.mu.Unlock()
	return dek, nil
}

// MAC returns a deterministic keyed hash of data under the workspace key,
// for PII uniqueness claims (ADR 0008). Uniqueness holds while the workspace
// lives (the HMAC is deterministic); shredding the workspace key makes the
// stored hash unlinkable to any value, keeping erasure complete. It
// provisions a DEK if the workspace has none yet (this runs on the write
// path, inside the same command that claims the value).
//
// The MAC key is a subkey derived from the DEK (domain-separated), not the
// DEK itself, so uniqueness hashing never shares key material with payload
// encryption.
func (s *Shredder) MAC(ctx context.Context, workspaceID string, data []byte) ([]byte, error) {
	dek, err := s.dek(ctx, workspaceID, true)
	if err != nil {
		return nil, err
	}
	sub := hmac.New(sha256.New, dek)
	sub.Write([]byte("es-lite/uniqueness-subkey"))
	key := sub.Sum(nil)

	m := hmac.New(sha256.New, key)
	m.Write(data)
	return m.Sum(nil), nil
}

// Forget crypto-shreds a workspace: destroy the KEK, evict the cached DEK,
// and drop the wrapped-DEK row. After this the workspace's ciphertext is
// permanently unrecoverable. The caller must also purge the workspace's
// projections and NATS-retained payloads (ADR 0004 §4) — that is
// application-owned and not done here.
func (s *Shredder) Forget(ctx context.Context, workspaceID string) error {
	if err := s.ks.ForgetWorkspace(ctx, workspaceID); err != nil {
		return fmt.Errorf("shred: forget kek: %w", err)
	}
	s.mu.Lock()
	delete(s.cache, workspaceID)
	s.mu.Unlock()
	if err := s.deks.DeleteWrappedDEK(ctx, workspaceID); err != nil {
		return fmt.Errorf("shred: delete wrapped dek: %w", err)
	}
	return nil
}

func newAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
