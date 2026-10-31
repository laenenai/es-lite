package es

import (
	"crypto/sha256"
	"encoding/binary"
	"time"

	"github.com/google/uuid"
)

// Tamper-evident hash chain (ADR 0029). Every stored event carries
// hash = SHA-256(canonical(envelope) ‖ prev_hash), chained per stream. The
// chain is computed by the storage adapter inside the append transaction
// over the SEALED payload bytes exactly as stored, so an auditor can verify
// it without holding any key. This file is the canonical encoding an
// auditor reimplements; hash_test.go pins it with golden vectors.

// HashSize is the byte length of PrevHash and Hash (SHA-256).
const HashSize = sha256.Size

// Domain separators, length-prefixed into the encodings below so the two
// hash kinds can never collide with each other or with a later version.
const (
	envelopeDomain      = "es-lite.envelope.v1"
	unchainedHeadDomain = "es-lite.unchained-head.v1"
)

// ZeroHash is the prev_hash of version 1 of every stream: 32 zero bytes.
var ZeroHash [HashSize]byte

// CanonicalEnvelope returns the bytes EnvelopeHash hashes. Field order, with
// every variable-length field prefixed by a big-endian uint32 length and
// every fixed-width integer big-endian:
//
//	"es-lite.envelope.v1"              domain separator (len-prefixed)
//	workspace, stream_id ("type:id")   len-prefixed strings
//	version u64, event_id 16 bytes
//	type_url (len-prefixed), schema_version u32
//	occurred_at i64 unix µs, recorded_at i64 unix µs
//	correlation_id, causation_id, command_id   16 bytes each (Nil = zeros)
//	actor_type, actor_id               len-prefixed strings
//	payload                            len-prefixed, the sealed bytes as stored
//	prev_hash                          32 bytes
//
// GlobalPosition is excluded (assigned during the INSERT; attested by anchors
// instead) and so is the relay's mutable published_at marker. Timestamps are
// truncated to microseconds — the precision every backend returns — so a
// verifier recomputes from what the database hands back.
func CanonicalEnvelope(e Envelope) []byte {
	b := make([]byte, 0, 256+len(e.Payload))
	b = appendString(b, envelopeDomain)
	b = appendString(b, e.Workspace)
	b = appendString(b, e.StreamID.Canonical())
	b = binary.BigEndian.AppendUint64(b, e.Version)
	b = appendUUID(b, e.EventID)
	b = appendString(b, e.TypeURL)
	b = binary.BigEndian.AppendUint32(b, e.SchemaVersion)
	b = binary.BigEndian.AppendUint64(b, uint64(unixMicros(e.OccurredAt)))
	b = binary.BigEndian.AppendUint64(b, uint64(unixMicros(e.RecordedAt)))
	b = appendUUID(b, e.CorrelationID)
	b = appendUUID(b, e.CausationID)
	b = appendUUID(b, e.CommandID)
	b = appendString(b, e.Actor.Type)
	b = appendString(b, e.Actor.ID)
	b = appendBytes(b, e.Payload)
	b = appendFixedHash(b, e.PrevHash)
	return b
}

// EnvelopeHash computes the chain hash of e from its fields, including
// e.PrevHash. The adapter sets e.PrevHash before calling it; a verifier
// recomputes it from a returned envelope and compares with e.Hash.
func EnvelopeHash(e Envelope) [HashSize]byte {
	return sha256.Sum256(CanonicalEnvelope(e))
}

// UnchainedHeadHash is the prev_hash of the first chained event in a stream
// whose head predates migration 0002 (ADR 0029 §5): it binds the chain to
// where it started without certifying the unattested rows before it.
//
//	SHA-256("es-lite.unchained-head.v1" ‖ stream_id ‖ head_version)
//
// with the separator and stream_id length-prefixed and head_version a
// big-endian uint64.
func UnchainedHeadHash(sid StreamID, headVersion uint64) [HashSize]byte {
	b := make([]byte, 0, 64)
	b = appendString(b, unchainedHeadDomain)
	b = appendString(b, sid.Canonical())
	b = binary.BigEndian.AppendUint64(b, headVersion)
	return sha256.Sum256(b)
}

// TruncateMicros truncates t to microsecond precision in UTC — the precision
// the chain hashes and every backend stores. Adapters apply it to both
// timestamps before hashing and inserting so the returned envelope hashes
// identically to the row.
func TruncateMicros(t time.Time) time.Time {
	return t.UTC().Truncate(time.Microsecond)
}

// unixMicros is the canonical integer form of a timestamp. The zero time is
// encoded as 0 rather than its (negative) Unix value so a missing timestamp
// is a stable, recognisable value.
func unixMicros(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMicro()
}

func appendString(b []byte, s string) []byte {
	b = binary.BigEndian.AppendUint32(b, uint32(len(s)))
	return append(b, s...)
}

func appendBytes(b []byte, p []byte) []byte {
	b = binary.BigEndian.AppendUint32(b, uint32(len(p)))
	return append(b, p...)
}

func appendUUID(b []byte, id uuid.UUID) []byte {
	return append(b, id[:]...)
}

// appendFixedHash appends exactly HashSize bytes: h when it has that length,
// zeros otherwise (a nil prev_hash on an unchained row hashes as zero).
func appendFixedHash(b []byte, h []byte) []byte {
	if len(h) == HashSize {
		return append(b, h...)
	}
	return append(b, ZeroHash[:]...)
}
