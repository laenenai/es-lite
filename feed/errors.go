package feed

import (
	"errors"
	"fmt"
)

var (
	// ErrInvalidRequest reports a Request missing a required field.
	ErrInvalidRequest = errors.New("feed: invalid request")
	// ErrLeaseHeld reports a subscription key owned by another live stream.
	// ResetCheckpoint returns it (unless forced) while a stream is active.
	ErrLeaseHeld = errors.New("feed: subscription is held")
	// ErrLeaseLost reports a lost lease: a renewal failed (the stream is
	// cancelled because another subscriber may now own the key), or an
	// ack's owner no longer holds the key (Checkpoint rejects it with
	// the same sentinel instead of advancing a stale consumer's cursor).
	ErrLeaseLost = errors.New("feed: lease lost")
	// ErrDefinitionChanged reports a subscription whose checkpoint was
	// last acknowledged under a different definition than the request's.
	// Subscribe fails with it instead of resuming below the checkpoint
	// and silently skipping the history of newly consumed types;
	// transport layers map it to FailedPrecondition. Recovery is
	// explicit: ResetCheckpoint under the new definition.
	ErrDefinitionChanged = errors.New("feed: projection definition changed")
)

// errLeaseExpired is the cause when Renew reports false without an error.
var errLeaseExpired = errors.New("lease expired or was taken over")

// LeaseHeldError names the key and, when the store still reports it, the
// active holder. It matches ErrLeaseHeld with errors.Is.
type LeaseHeldError struct {
	Key   string
	Lease Lease
	Held  bool
}

// Error implements error. It never includes event data.
func (e *LeaseHeldError) Error() string {
	if e.Held && e.Lease.Consumer != "" {
		return fmt.Sprintf("feed: subscription %q is held by consumer %q", e.Key, e.Lease.Consumer)
	}
	return fmt.Sprintf("feed: subscription %q is held", e.Key)
}

// Is reports ErrLeaseHeld.
func (e *LeaseHeldError) Is(target error) bool { return target == ErrLeaseHeld }

// LeaseLostError reports a failed renewal and its cause, or an ack whose
// owner no longer holds the key. It matches ErrLeaseLost, and its cause,
// with errors.Is.
type LeaseLostError struct {
	Key   string
	Owner string
	Err   error
}

// Error implements error.
func (e *LeaseLostError) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("feed: lease %q for owner %q lost", e.Key, e.Owner)
	}
	return fmt.Sprintf("feed: lease %q for owner %q lost: %v", e.Key, e.Owner, e.Err)
}

// Unwrap returns the renewal cause.
func (e *LeaseLostError) Unwrap() error { return e.Err }

// Is reports ErrLeaseLost (and anything the cause matches).
func (e *LeaseLostError) Is(target error) bool {
	return target == ErrLeaseLost || (e.Err != nil && errors.Is(e.Err, target))
}

// EventTooLargeError closes a stream on a poison event: its stored payload is
// larger than MaxPayloadBytes. It names the position, type URL and size but
// never the payload.
type EventTooLargeError struct {
	Position uint64
	TypeURL  string
	Size     int
	Limit    int
}

// Error implements error.
func (e *EventTooLargeError) Error() string {
	return fmt.Sprintf("feed: event at position %d of type %q is %d bytes, exceeding the %d-byte limit",
		e.Position, e.TypeURL, e.Size, e.Limit)
}

// DefinitionChangedError reports a checkpoint that was last acknowledged
// under a different definition than the request's: the stored fingerprint
// no longer matches the code's consumed-type set. It matches
// ErrDefinitionChanged with errors.Is.
type DefinitionChangedError struct {
	// Projection is the definition name whose fingerprint drifted.
	Projection string
	// StoredHash is the fingerprint the checkpoint was acked under.
	StoredHash string
	// RequestHash is the request definition's fingerprint.
	RequestHash string
}

// Error implements error. It names the projection and the recovery:
// ResetCheckpoint under the new definition is what re-identifies the
// checkpoint and replays. The hashes are fingerprints of public type
// sets, never event data.
func (e *DefinitionChangedError) Error() string {
	return fmt.Sprintf("feed: projection %q changed definition (checkpoint %s, request %s): resume would skip the new types' history below the checkpoint; call ResetCheckpoint with the new definition to re-subscribe",
		e.Projection, e.StoredHash, e.RequestHash)
}

// Is reports ErrDefinitionChanged.
func (e *DefinitionChangedError) Is(target error) bool {
	return target == ErrDefinitionChanged
}
