package es

import (
	"fmt"
	"strings"
)

// Actor records who (or what) caused an event, for the audit trail.
// Auditability is a non-negotiable of es-lite (docs/adr/0001), so every
// appended event carries an Actor — even if it is the zero value for
// system-internal writes.
//
// Type is a coarse category ("user", "service", "system", "job"); ID is
// the stable principal identifier within that category. Both are free
// text — es-lite does not interpret them, it only records them.
type Actor struct {
	Type string
	ID   string

	// Via is the service this actor acted through (ADR 0041): an agent
	// runtime executing a task, an API gateway forwarding a request, a
	// worker applying an earlier decision. Zero when the actor acted for
	// itself — a service acting for itself is simply Actor{Type:
	// "service"} with a zero Via. One level only: an ActorRef cannot
	// itself delegate, so the shape is unrepresentable rather than
	// forbidden. Via is an audit fact, not telemetry: CanonicalEnvelope
	// attests it (ADR 0041 §3.2).
	Via ActorRef
}

// ActorRef identifies a principal without granting it the
// accountability of an Actor (ADR 0041 §3.1). It is a value type: the
// zero value means "not set", and no nesting is representable — there
// is no Via field to forget to check.
type ActorRef struct {
	Type string
	ID   string
}

// IsZero reports whether the ref carries no information. A ref with
// only one of its fields set is NOT zero: a partially recorded
// delegation is a defect Actor.Validate rejects, so it must be
// detectable.
func (r ActorRef) IsZero() bool { return r.Type == "" && r.ID == "" }

// Principal returns the canonical "type:id" form used for the indexed
// via_principal column, so the reverse audit query ("everything service
// S did, across users") is cheap. Returns "" for the zero ref. It must
// produce exactly what Actor.Principal produces for the same type and
// id — the stores write the two columns the same way — and a test pins
// that.
func (r ActorRef) Principal() string {
	if r.Type == "" && r.ID == "" {
		return ""
	}
	var b strings.Builder
	b.WriteString(r.Type)
	b.WriteByte(':')
	b.WriteString(r.ID)
	return b.String()
}

// Principal returns the canonical "type:id" form used for the indexed
// actor_principal column, so audit queries ("everything actor X did")
// are cheap. Returns "" for the zero Actor. Via is deliberately
// invisible here: the accountable subject is Actor alone.
func (a Actor) Principal() string {
	if a.Type == "" && a.ID == "" {
		return ""
	}
	var b strings.Builder
	b.WriteString(a.Type)
	b.WriteByte(':')
	b.WriteString(a.ID)
	return b.String()
}

// IsZero reports whether the Actor carries no information. It keeps
// its pre-ADR-0041 meaning — no type and no id — and is unaffected by
// Via.
func (a Actor) IsZero() bool { return a.Type == "" && a.ID == "" }

// Validate reports whether the actor is well formed for an append
// (ADR 0041 §3.4), so both stores can enforce the same rule without
// drifting.
//
// The zero Actor is valid: system-internal writes carry no actor. When
// Via is set it must be complete — a type without an id is a partially
// recorded delegation, which the log refuses to attest — and it must
// accompany an accountable actor: a Via with nobody to account for is
// an execution path with a hole where its subject should be. Both
// defects return ErrInvalidDelegation wrapped with what was wrong;
// nothing was written.
func (a Actor) Validate() error {
	if a.Via.IsZero() {
		return nil
	}
	if a.IsZero() {
		return fmt.Errorf("%w: via %q set but no accountable actor", ErrInvalidDelegation, a.Via.Principal())
	}
	if a.Via.Type == "" || a.Via.ID == "" {
		return fmt.Errorf("%w: partial via (type %q, id %q)", ErrInvalidDelegation, a.Via.Type, a.Via.ID)
	}
	return nil
}
