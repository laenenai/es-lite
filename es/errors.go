package es

import (
	"errors"
	"fmt"
	"time"
)

// Typed errors. Framework operations return one of these, bare or
// wrapped via fmt.Errorf("...: %w", err). Consumers match with errors.Is.
var (
	// ErrConflict reports an optimistic-concurrency failure on Append:
	// the ExpectedVersion did not match the stream's current version.
	// The caller should reload the stream and retry the command.
	ErrConflict = errors.New("es-lite: append conflict (expected version mismatch)")

	// ErrDuplicateEventID reports that an Append carried an EventID the
	// log already stores — on this stream or another. It is distinct
	// from ErrConflict: no version was contested and nothing raced; the
	// event itself is a rerun. Retrying cannot succeed — the event is
	// already in the log — so a caller must treat it as an idempotency
	// signal, never a retry cue.
	ErrDuplicateEventID = errors.New("es-lite: event id already stored")

	// ErrStreamNotFound is returned by ReadStream when it reads a stream
	// from its beginning (fromVersion == 0, whatever the upper bound) and
	// the stream has no events at all — it was never written. An empty
	// range is not "not found": reads with fromVersion > 0, and reads
	// from 0 of a stream that exists, return an empty slice and a nil
	// error. VerifyStream and the aggregate runtime treat it as an empty
	// stream.
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

	// ErrDecidedAtSkew reports that a command carrying (es.v1.decided_at)
	// supplied a decision time further than MaxDecisionSkew from the server
	// clock. Nothing was appended (ADR 0034).
	ErrDecidedAtSkew = errors.New("es-lite: command's decided_at is outside the allowed server-time skew")

	// ErrDecidedAtMissing reports that a command carrying (es.v1.decided_at)
	// left the field unset. Nothing was appended (ADR 0032).
	ErrDecidedAtMissing = errors.New("es-lite: command's decided_at is unset")

	// ErrDecidedAtUnstampable reports that a command implements
	// es.DecidedAt — it can report a decision time — but
	// es.StampDecisionTime could not replace that time with the server
	// clock: the command is neither an es.DecisionTimeSetter nor a proto
	// message with a tagged (es.v1.decided_at) field. Decide would read
	// the client's clock, so aggregate.Runtime refuses to run it; the
	// error aggregate.Runtime returns names the command type.
	ErrDecidedAtUnstampable = errors.New("es-lite: command's decided_at cannot be stamped")

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

	// ErrPayloadTooLarge reports that an Append batch contained an event
	// whose payload exceeds the store's payload cap
	// (WithMaxPayloadBytes; 8 MiB by default). The whole batch is
	// rejected before anything is inserted, so the stream and the log
	// are untouched. Errors wrapping it carry the stream id, the
	// version the event would have taken, the event's type URL and the
	// payload's size in bytes — never the payload bytes themselves.
	ErrPayloadTooLarge = errors.New("es-lite: payload too large")
)

// DecidedAtSkewError is the structured form of ErrDecidedAtSkew: it carries
// both clocks and the accepted window so a caller can report the rejection.
type DecidedAtSkewError struct {
	DecidedAt  time.Time
	ServerTime time.Time
	MaxSkew    time.Duration
}

func (e *DecidedAtSkewError) Error() string {
	return fmt.Sprintf("%v: command %s, server %s, max %s",
		ErrDecidedAtSkew, e.DecidedAt.UTC().Format(time.RFC3339Nano),
		e.ServerTime.UTC().Format(time.RFC3339Nano), e.MaxSkew)
}

// Unwrap makes errors.Is(err, ErrDecidedAtSkew) true.
func (e *DecidedAtSkewError) Unwrap() error { return ErrDecidedAtSkew }

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
