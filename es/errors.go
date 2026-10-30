package es

import "errors"

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

	// ErrUnknownEventType is returned by a Codec.Decode when the
	// envelope's TypeURL is not one the codec recognizes. Usually a
	// version-skew deployment or a codec wired to the wrong aggregate.
	ErrUnknownEventType = errors.New("es-lite: unknown event type url")
)
