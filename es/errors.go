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

	// ErrInvalidStreamID reports that a StreamID failed validation.
	ErrInvalidStreamID = errors.New("es-lite: invalid stream id")

	// ErrTerminal reports that a command was issued against a stream
	// whose Decider.IsTerminal returned true. The stream is closed —
	// no further events may be appended.
	ErrTerminal = errors.New("es-lite: stream is terminal")

	// ErrUnknownEventType is returned by a Codec.Decode when the
	// envelope's TypeURL is not one the codec recognizes. Usually a
	// version-skew deployment or a codec wired to the wrong aggregate.
	ErrUnknownEventType = errors.New("es-lite: unknown event type url")
)
