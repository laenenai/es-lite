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
	// ErrLeaseLost reports that the lease could not be renewed: the stream is
	// cancelled because another subscriber may now own the key.
	ErrLeaseLost = errors.New("feed: lease lost")
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

// LeaseLostError reports a failed renewal and its cause. It matches
// ErrLeaseLost, and its cause, with errors.Is.
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
