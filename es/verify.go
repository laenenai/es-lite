package es

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"
)

// ChainReport is the outcome of verifying one stream's hash chain (ADR 0029
// §3). It is informative even when the chain is intact: ChainedFrom tells an
// auditor how much of the stream is attested at all.
type ChainReport struct {
	StreamID StreamID

	// ChainedFrom is the first version carrying a hash; 0 when no row in the
	// stream is hashed (the whole stream predates migration 0002). Rows
	// before it are unattested — the store can vouch for nothing about them.
	ChainedFrom uint64

	// Head is the highest version read and HeadHash its stored hash (nil
	// when the head is unchained). Together they are what an anchor fixes.
	Head     uint64
	HeadHash []byte

	// BrokenAt is the first version that failed verification, 0 when the
	// chain is intact. Err carries the detail, wrapping ErrChainBroken.
	BrokenAt uint64
	Err      error
}

// Intact reports whether verification found no break.
func (r ChainReport) Intact() bool { return r.BrokenAt == 0 && r.Err == nil }

// VerifyOptions tunes VerifyStream.
type VerifyOptions struct {
	// ChainedSince, when set, is the instant migration 0002 was applied: a
	// row with a nil hash recorded after it is reported as broken, because
	// the store writes hashes unconditionally after the migration (ADR 0029
	// §5). Adapters expose it (sqlite/postgres ChainedSince); zero disables
	// the check.
	ChainedSince time.Time

	// Strict requires ChainedFrom == 1: every row attested. The only
	// configuration that is tamper-evident end to end; a store populated
	// before 0002 cannot satisfy it.
	Strict bool

	// Page is the ReadStream batch size (default 500).
	Page int
}

// VerifyStream recomputes the hash chain of one stream from what store
// returns — version contiguity, prev_hash linkage and every hash — and
// reports the result. It is a free function over any Store, so it verifies
// through the NATS client exactly as it does against Postgres or SQLite:
// the client holds the sealed payload bytes and recomputes from them. No
// keys are needed.
//
// A store/read error is returned as-is. A broken chain returns the populated
// report and an error wrapping ErrChainBroken; an intact chain returns a nil
// error.
func VerifyStream(ctx context.Context, store Store, sid StreamID) (ChainReport, error) {
	return VerifyStreamWith(ctx, store, sid, VerifyOptions{})
}

// VerifyStreamWith is VerifyStream with options.
func VerifyStreamWith(ctx context.Context, store Store, sid StreamID, opts VerifyOptions) (ChainReport, error) {
	if err := sid.Validate(); err != nil {
		return ChainReport{StreamID: sid}, err
	}
	page := opts.Page
	if page <= 0 {
		page = 500
	}
	c := NewChainChecker(sid)
	c.ChainedSince = opts.ChainedSince
	var from uint64
	for {
		envs, err := store.ReadStream(ctx, sid, from, from+uint64(page))
		if err != nil && !errors.Is(err, ErrStreamNotFound) {
			return c.Report(), err
		}
		for _, e := range envs {
			if err := c.Next(e); err != nil {
				return c.Report(), err
			}
		}
		if len(envs) < page {
			break
		}
		from = envs[len(envs)-1].Version
	}
	rep := c.Report()
	if opts.Strict && rep.Head > 0 && rep.ChainedFrom != 1 {
		rep.BrokenAt = 1
		rep.Err = fmt.Errorf("%w: stream %s chained from version %d, strict mode requires 1", ErrChainBroken, sid, rep.ChainedFrom)
		return rep, rep.Err
	}
	return rep, nil
}

// ChainChecker verifies a stream's chain incrementally, one envelope at a
// time in version order. VerifyStream drives it over a whole stream; the
// aggregate runtime drives it over whatever prefix it folds when chain
// verification is enabled. A checker fed from the middle of a stream (a
// snapshot-seeded load) accepts the first envelope's prev_hash as given and
// verifies integrity and linkage from there.
type ChainChecker struct {
	// ChainedSince is VerifyOptions.ChainedSince; zero disables the check.
	ChainedSince time.Time

	sid     StreamID
	started bool
	last    Envelope
	rep     ChainReport
}

// NewChainChecker starts a checker for sid.
func NewChainChecker(sid StreamID) *ChainChecker {
	return &ChainChecker{sid: sid, rep: ChainReport{StreamID: sid}}
}

// Next verifies e against the envelope before it. Once it returns an error
// the checker stays broken.
func (c *ChainChecker) Next(e Envelope) error {
	if c.rep.Err != nil {
		return c.rep.Err
	}
	if err := c.check(e); err != nil {
		c.rep.BrokenAt = e.Version
		c.rep.Err = fmt.Errorf("%w: stream %s v%d: %v", ErrChainBroken, c.sid, e.Version, err)
		return c.rep.Err
	}
	if e.Hash != nil && c.rep.ChainedFrom == 0 {
		c.rep.ChainedFrom = e.Version
	}
	c.rep.Head = e.Version
	c.rep.HeadHash = e.Hash
	c.last = e
	c.started = true
	return nil
}

func (c *ChainChecker) check(e Envelope) error {
	if e.StreamID != c.sid {
		return fmt.Errorf("envelope belongs to stream %s", e.StreamID)
	}
	if c.started && e.Version != c.last.Version+1 {
		return fmt.Errorf("version not contiguous: previous %d", c.last.Version)
	}
	if !c.started && e.Version == 0 {
		return errors.New("version 0")
	}

	if e.Hash == nil {
		if e.PrevHash != nil {
			return errors.New("prev_hash set on an unhashed row")
		}
		if c.started && c.last.Hash != nil {
			return errors.New("unhashed row after a chained one")
		}
		if !c.ChainedSince.IsZero() && e.RecordedAt.After(c.ChainedSince) {
			return fmt.Errorf("unhashed row recorded at %s, after the chain started at %s",
				e.RecordedAt.UTC().Format(time.RFC3339Nano), c.ChainedSince.UTC().Format(time.RFC3339Nano))
		}
		return nil
	}

	if len(e.Hash) != HashSize {
		return fmt.Errorf("hash is %d bytes, want %d", len(e.Hash), HashSize)
	}
	if len(e.PrevHash) != HashSize {
		return fmt.Errorf("prev_hash is %d bytes, want %d", len(e.PrevHash), HashSize)
	}

	// Linkage: what prev_hash must be, when we can know it.
	var want []byte
	switch {
	case !c.started && e.Version == 1:
		want = ZeroHash[:]
	case !c.started:
		want = nil // mid-stream start: accept as given
	case c.last.Hash == nil:
		h := UnchainedHeadHash(c.sid, c.last.Version)
		want = h[:]
	default:
		want = c.last.Hash
	}
	if want != nil && !bytes.Equal(e.PrevHash, want) {
		return errors.New("prev_hash does not link to the previous version")
	}

	// Integrity: the stored hash must be the one the row's fields produce.
	if got := EnvelopeHash(e); !bytes.Equal(got[:], e.Hash) {
		return errors.New("stored hash does not match the recomputed hash")
	}
	return nil
}

// Report returns the state so far.
func (c *ChainChecker) Report() ChainReport { return c.rep }
