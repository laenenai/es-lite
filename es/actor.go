package es

import "strings"

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
}

// Principal returns the canonical "type:id" form used for the indexed
// actor_principal column, so audit queries ("everything actor X did")
// are cheap. Returns "" for the zero Actor.
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

// IsZero reports whether the Actor carries no information.
func (a Actor) IsZero() bool { return a.Type == "" && a.ID == "" }
