// Package cryptostore is a client-side es.Store decorator that encrypts event
// payloads on write and decrypts on read under a per-workspace DEK, and
// converts PII uniqueness values to a keyed HMAC before they reach the inner
// store (ADR 0009). Composed over the natsstore.Client it makes the remote
// eventstore zero-knowledge — only ciphertext and opaque value-keys cross the
// wire. Composed over a local store it simply encrypts at rest.
//
// It is workspace-scoped, mirroring postgres.Store.Workspace:
//
//	inner := natsClient.Workspace("ws_1")            // es.Store over NATS
//	store := cryptostore.New(inner, shredder, "ws_1")
//	rt := aggregate.NewRuntime(store, decider, codec) // runtime unchanged
package cryptostore

import (
	"context"
	"encoding/base64"
	"time"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/shred"
)

// Store wraps an inner es.Store with per-workspace client-side crypto.
type Store struct {
	inner    es.Store
	shredder *shred.Shredder
	ws       string
}

var _ es.Store = (*Store)(nil)

// New wraps inner with crypto for one workspace. shredder must be non-nil.
func New(inner es.Store, shredder *shred.Shredder, workspace string) *Store {
	return &Store{inner: inner, shredder: shredder, ws: workspace}
}

// aad binds a payload's ciphertext to its workspace + stream (ADR 0014 E): the AAD is
// authenticated (not encrypted), so a ciphertext moved to another stream/workspace fails to
// open. Write and read derive it identically from the envelope's own StreamID.
func (s *Store) aad(sid es.StreamID) []byte { return []byte(s.ws + "|" + sid.Canonical()) }

// Append encrypts payloads and HMACs PII constraint values, then forwards.
func (s *Store) Append(ctx context.Context, p es.AppendParams) (es.AppendResult, error) {
	cipher, err := s.shredder.WriteCipher(ctx, s.ws)
	if err != nil {
		return es.AppendResult{}, err
	}

	aad := s.aad(p.StreamID)
	enc := make([]es.EventData, len(p.Events))
	for i, ev := range p.Events {
		ct, err := cipher.EncryptWithAAD(ev.Payload, aad)
		if err != nil {
			return es.AppendResult{}, err
		}
		enc[i] = ev
		enc[i].Payload = ct
	}
	p.Events = enc

	if len(p.Constraints) > 0 {
		cons := make([]es.ConstraintOp, len(p.Constraints))
		for i, op := range p.Constraints {
			cons[i] = op
			if op.PII {
				mac, err := s.shredder.MAC(ctx, s.ws, []byte(op.Value))
				if err != nil {
					return es.AppendResult{}, err
				}
				// Send the HMAC as an opaque value_key; the inner store must
				// not re-hash it, so clear PII.
				cons[i].Value = base64.RawStdEncoding.EncodeToString(mac)
				cons[i].PII = false
			}
		}
		p.Constraints = cons
	}

	res, err := s.inner.Append(ctx, p)
	if err != nil {
		return es.AppendResult{}, err
	}
	// Returned envelopes carry ciphertext; hand plaintext back to the caller.
	if err := s.decryptAll(ctx, res.Envelopes); err != nil {
		return es.AppendResult{}, err
	}
	return res, nil
}

func (s *Store) ReadStream(ctx context.Context, sid es.StreamID, fromVersion, toVersion uint64) ([]es.Envelope, error) {
	envs, err := s.inner.ReadStream(ctx, sid, fromVersion, toVersion)
	if err != nil {
		return nil, err
	}
	return envs, s.decryptAll(ctx, envs)
}

func (s *Store) ReadStreamAsOf(ctx context.Context, sid es.StreamID, asOf time.Time) ([]es.Envelope, error) {
	envs, err := s.inner.ReadStreamAsOf(ctx, sid, asOf)
	if err != nil {
		return nil, err
	}
	return envs, s.decryptAll(ctx, envs)
}

func (s *Store) ReadAll(ctx context.Context, fromPosition uint64, limit int) ([]es.Envelope, error) {
	envs, err := s.inner.ReadAll(ctx, fromPosition, limit)
	if err != nil {
		return nil, err
	}
	return envs, s.decryptAll(ctx, envs)
}

func (s *Store) CurrentStreamVersion(ctx context.Context, sid es.StreamID) (uint64, error) {
	return s.inner.CurrentStreamVersion(ctx, sid)
}

// LookupClaim keys a PII value the same way Append does (HMAC → opaque
// value_key, PII cleared) before forwarding, so the inner store matches what it
// stored. Non-PII values pass through unchanged.
func (s *Store) LookupClaim(ctx context.Context, scope, value string, pii bool) (string, bool, error) {
	if pii {
		mac, err := s.shredder.MAC(ctx, s.ws, []byte(value))
		if err != nil {
			return "", false, err
		}
		value = base64.RawStdEncoding.EncodeToString(mac)
	}
	return s.inner.LookupClaim(ctx, scope, value, false)
}

// decryptAll decrypts each envelope's payload in place. It fetches the read
// cipher only when there is something to decrypt, so an empty result needs no
// key and a shredded workspace surfaces keystore.ErrShredded.
func (s *Store) decryptAll(ctx context.Context, envs []es.Envelope) error {
	if len(envs) == 0 {
		return nil
	}
	cipher, err := s.shredder.ReadCipher(ctx, s.ws)
	if err != nil {
		return err
	}
	for i := range envs {
		pt, err := cipher.DecryptWithAAD(envs[i].Payload, s.aad(envs[i].StreamID))
		if err != nil {
			return err
		}
		envs[i].Payload = pt
	}
	return nil
}
