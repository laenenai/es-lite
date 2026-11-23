package es

import (
	"fmt"
	"time"
)

// MultiAppend appends several streams in one caller-owned transaction, one
// command. Appends execute in input order and their global positions follow
// that order; each append checks its own ExpectedVersion. The whole set is
// all-or-nothing: any conflict, duplicate event id, invalid tag or constraint
// violation rolls the entire transaction back.
//
// The appends must name distinct streams and share one command identity:
// CommandID, CorrelationID, CausationID, Actor, SpanContext, and (when set)
// OccurredAt. ValidateMultiAppend enforces that contract before the store
// draws a position.
//
// The caller owns the event ids: a non-zero EventID is preserved, and a zero
// one is minted as a UUIDv7 by the adapter. Deterministic derivation is the
// caller's job and must use a global event index across all appends — the
// runtime's per-append CommandID:index derivation restarts at zero and would
// collide on the second stream.
//
// The store does not sort the appends. Concurrent multi-appends touching the
// same streams in different orders can deadlock; the adapter surfaces that
// error and the caller retries the whole transaction, conventionally after
// sorting appends by canonical stream id.
type MultiAppend struct {
	Appends []AppendParams
}

// ValidateMultiAppend checks the MultiAppend contract: at least one append,
// every append carries at least one event, streams are distinct, and the
// shared command metadata agrees. It does not validate each AppendParams'
// stream id, tags or constraints; the adapter's per-append validation runs
// after this check and before anything is written.
func ValidateMultiAppend(m MultiAppend) error {
	if len(m.Appends) == 0 {
		return fmt.Errorf("%w: no appends", ErrEmptyMultiAppend)
	}

	first := m.Appends[0]
	seen := make(map[string]struct{}, len(m.Appends))
	for i, p := range m.Appends {
		if len(p.Events) == 0 {
			return fmt.Errorf("%w: append %d has no events", ErrEmptyMultiAppend, i)
		}
		canonical := p.StreamID.Canonical()
		if _, ok := seen[canonical]; ok {
			return fmt.Errorf("%w: stream %s", ErrDuplicateMultiStream, canonical)
		}
		seen[canonical] = struct{}{}

		if i == 0 {
			continue
		}
		switch {
		case p.CommandID != first.CommandID:
			return fmt.Errorf("%w: append %d command id differs", ErrConflictingMultiAppend, i)
		case p.CorrelationID != first.CorrelationID:
			return fmt.Errorf("%w: append %d correlation id differs", ErrConflictingMultiAppend, i)
		case p.CausationID != first.CausationID:
			return fmt.Errorf("%w: append %d causation id differs", ErrConflictingMultiAppend, i)
		case p.Actor != first.Actor:
			return fmt.Errorf("%w: append %d actor differs", ErrConflictingMultiAppend, i)
		case p.SpanContext != first.SpanContext:
			return fmt.Errorf("%w: append %d span context differs", ErrConflictingMultiAppend, i)
		case !p.OccurredAt.IsZero() && !first.OccurredAt.IsZero() &&
			!TruncateMicros(p.OccurredAt).Equal(TruncateMicros(first.OccurredAt)):
			return fmt.Errorf("%w: append %d occurred_at differs", ErrConflictingMultiAppend, i)
		}
	}
	return nil
}

// MultiOccurredAt resolves the one domain time a MultiAppend uses: the first
// non-zero OccurredAt (truncated to microseconds), or now when every append
// leaves it zero. Adapters call it after ValidateMultiAppend has established
// that all non-zero values agree, then copy the result onto every append so
// one command has one OccurredAt.
func MultiOccurredAt(appends []AppendParams, now time.Time) time.Time {
	for _, p := range appends {
		if !p.OccurredAt.IsZero() {
			return TruncateMicros(p.OccurredAt)
		}
	}
	return TruncateMicros(now)
}
