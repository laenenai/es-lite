// Package keystore is es-lite's per-workspace key provider for
// crypto-shredding (ADR 0004). Implementations wrap a Key-Encryption-Key
// (KEK) held in a KMS — OpenBao Transit in production (see keystore/openbao),
// an in-process key for tests (keystore/memory) — and never expose the KEK.
// Only short-lived plaintext Data-Encryption-Keys (DEKs) cross the boundary.
//
// Envelope encryption: a per-workspace DEK encrypts event payloads locally
// (fast, no per-event KMS round-trip); the DEK is stored only in wrapped
// form. Erasing a workspace destroys its KEK, after which no wrapped DEK
// can be unwrapped again — every payload under it is permanently
// unrecoverable, including in database backups.
package keystore

import (
	"context"
	"errors"
)

// DEKSize is the plaintext DEK length in bytes (AES-256).
const DEKSize = 32

// ErrShredded reports that a workspace's KEK has been destroyed, so its
// wrapped DEK can no longer be unwrapped. This is the expected, non-error
// outcome of reading a forgotten workspace — callers skip such events
// rather than failing (ADR 0005). Match with errors.Is.
var ErrShredded = errors.New("es-lite/keystore: workspace shredded (key destroyed)")

// KeyStore provides per-workspace envelope-encryption keys.
type KeyStore interface {
	// GenerateDEK mints a fresh data key for a workspace: the plaintext DEK
	// (encrypt with it, cache it, then discard) and its wrapped form (to
	// persist alongside the workspace). kekVersion identifies the KEK that
	// wrapped it, for rotation tracking.
	GenerateDEK(ctx context.Context, workspaceID string) (plaintext []byte, wrapped []byte, kekVersion int, err error)

	// UnwrapDEK recovers a plaintext DEK from its wrapped form. Returns
	// ErrShredded (wrapped via %w) if the workspace KEK has been destroyed.
	UnwrapDEK(ctx context.Context, workspaceID string, wrapped []byte) (plaintext []byte, err error)

	// ForgetWorkspace destroys the workspace KEK — the crypto-shredding
	// operation. Idempotent: forgetting an already-forgotten (or never
	// created) workspace is not an error.
	ForgetWorkspace(ctx context.Context, workspaceID string) error
}
