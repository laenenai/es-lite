package es

import "context"

// DedupKey identifies one command's dedup record: the canonical stream id
// the command targeted plus the command's id — the UUID the API edge
// derives from the request's Idempotency-Key (cmdsvc.MetaFrom,
// draft-ietf-httpapi-idempotency-key-header-07). Both components are
// strings on purpose: the record is opaque storage keyed exactly as the
// handler flow spells them, with no validation the store would then own.
// The key's implicit third component is the workspace — §5 of the draft
// recommends a composite key, and every adapter scopes records to one
// workspace so no tenant can observe another's retries.
type DedupKey struct {
	StreamID  string
	CommandID string
}

// DedupRecord is what a completed command leaves behind and a retry
// replays (draft §2.6): the fingerprint of the request whose response was
// stored — draft §2.4's checksum-of-payload option; the SHA-256 of the
// marshaled request in the handler flow — and the marshaled response
// itself, opaque bytes the adapter returns verbatim.
type DedupRecord struct {
	Fingerprint string
	Response    []byte
}

// Deduplicator stores and replays completed-command responses: the
// storage port of the Idempotency-Key flow (design D6, finding F6). A
// command that completes has its response Saved under its
// (stream, command) key; a retrying command Looks up first and replays
// the stored response instead of appending a second time. The types live
// in es so the core adapters (sqlite, postgres) and the nested cmdsvc
// module's generated handlers share one port.
//
// Lookup reports (record, true, nil) when the key holds a record. The
// caller compares record.Fingerprint against its own request's
// fingerprint: a match is the draft §2.6 replay, a mismatch the draft
// §2.7 key-reuse error. A key with no record reports (zero, false, nil)
// — a miss, never an error.
//
// Save is WRITE-ONCE and conflict-silent: a Save whose key already holds
// a record stores nothing and returns nil — the primary-key conflict is
// NOT an error. The first completed response wins forever; a Save that
// hits the conflict (a retry, or the concurrent loser of a race) must
// never overwrite it. A caller that needs to know what is stored
// re-Lookups: whatever a conflicted Save leaves behind is the original
// record.
//
// Both adapters scope records to one workspace — postgres through the
// command_dedup table's RLS (workspace arm only, no bypass arm; see its
// migration header), sqlite by being single-workspace — so one
// workspace's dedup records are invisible to every other.
type Deduplicator interface {
	Lookup(ctx context.Context, key DedupKey) (DedupRecord, bool, error)
	Save(ctx context.Context, key DedupKey, rec DedupRecord) error
}
