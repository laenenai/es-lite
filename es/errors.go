package es

import (
	"errors"
	"fmt"
)

// Typed errors. Framework operations return one of these, bare or
// wrapped via fmt.Errorf("...: %w", err). Consumers match with errors.Is.
var (
	// ErrConflict reports an optimistic-concurrency failure on Append:
	// the ExpectedVersion did not match the stream's current version.
	// The caller should reload the stream and retry the command.
	ErrConflict = errors.New("es-lite: append conflict (expected version mismatch)")

	// ErrStreamNotFound is returned when a read targets a stream that
	// has never been written to.
	ErrStreamNotFound = errors.New("es-lite: stream not found")

	// ErrConstraintViolated reports that a uniqueness Claim collided with an
	// existing claim: the (workspace, scope, value) was already taken. The
	// whole append is rolled back — no event is persisted. Callers decide
	// whether this is a domain error to surface (ADR 0008).
	ErrConstraintViolated = errors.New("es-lite: unique constraint violated")

	// ErrInvalidStreamID reports that a StreamID failed validation.
	ErrInvalidStreamID = errors.New("es-lite: invalid stream id")

	// ErrVersionOutOfRange reports that a time-travel read asked for a stream
	// version the stream has not reached. It is distinct from ErrConflict: no
	// write was attempted.
	ErrVersionOutOfRange = errors.New("es-lite: stream version out of range")

	// ErrInvalidConstraint reports a malformed ConstraintOp (unknown kind or
	// empty scope).
	ErrInvalidConstraint = errors.New("es-lite: invalid constraint")

	// ErrTerminal reports that a command was issued against a stream
	// whose Decider.IsTerminal returned true. The stream is closed —
	// no further events may be appended.
	ErrTerminal = errors.New("es-lite: stream is terminal")

	// ErrDecidedAtMismatch reports that a command carrying (es.v1.decided_at)
	// was handled with a Meta.OccurredAt that differs from the command's
	// value. The caller asserted two domain times; the runtime refuses rather
	// than picking one (ADR 0032). Nothing was appended.
	ErrDecidedAtMismatch = errors.New("es-lite: meta.OccurredAt differs from the command's decided_at")

	// ErrDecidedAtMissing reports that a command carrying (es.v1.decided_at)
	// left the field unset. Nothing was appended (ADR 0032).
	ErrDecidedAtMissing = errors.New("es-lite: command's decided_at is unset")

	// ErrChainBroken reports that a stream's hash chain (ADR 0029) failed
	// verification: a recomputed hash differs from the stored one, a
	// prev_hash does not link to its predecessor, versions are not
	// contiguous, or a chained stream has an unhashed row. The ChainReport
	// that accompanies it names the first broken version.
	ErrChainBroken = errors.New("es-lite: hash chain broken")

	// ErrAnchorMismatch reports that a recomputed anchor root differs from
	// the exported one (ADR 0029 §4): history up to the anchor's position
	// has been altered or truncated since the anchor was taken.
	ErrAnchorMismatch = errors.New("es-lite: anchor root mismatch")

	// ErrUnknownEventType is returned by a Codec.Decode when the
	// envelope's TypeURL is not one the codec recognizes. Usually a
	// version-skew deployment or a codec wired to the wrong aggregate.
	ErrUnknownEventType = errors.New("es-lite: unknown event type url")
)

// ConflictError is the structured form of ErrConflict a caller-supplied
// expected version produces (aggregate.Runtime.HandleAt, ADR 0031): it
// carries the versions a VersionConflict detail reports. errors.Is(err,
// ErrConflict) still matches.
type ConflictError struct {
	StreamID StreamID
	Expected uint64 // what the caller asserted
	Current  uint64 // what the stream was at when checked
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("%v: stream %s at v%d, expected v%d", ErrConflict, e.StreamID, e.Current, e.Expected)
}

// Unwrap makes errors.Is(err, ErrConflict) true.
func (e *ConflictError) Unwrap() error { return ErrConflict }
