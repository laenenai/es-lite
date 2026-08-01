package es

import (
	"regexp"
	"strings"
)

// canonicalSep separates the Type and ID components in the canonical
// stream_id storage form (Type + ":" + ID). The separator is reserved:
// the slug regex forbids it inside Type or ID, so splitting is
// unambiguous.
const canonicalSep = ":"

// slugRe matches valid Type and ID components: lowercase alphanumeric
// with optional underscores and hyphens, starting alphanumeric, up to
// 128 characters. Mirrors the parent framework's rule (its ADR 0008)
// minus the tenant component — lite streams are identified by Type:ID
// only (docs/adr/0001).
var slugRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,127}$`)

// StreamID identifies one stream: an aggregate Type and an instance ID.
//
// es-lite deliberately drops the parent framework's mandatory tenant
// component. Multi-tenancy, when needed, is expressed as one database
// per tenant or an application-level prefix baked into the ID — not as
// a first-class store concern (docs/adr/0001).
type StreamID struct {
	Type string // aggregate type, slug-validated (e.g. "counter")
	ID   string // instance id, slug-validated (e.g. "main")
}

// NewStreamID constructs a validated StreamID.
func NewStreamID(typ, id string) (StreamID, error) {
	sid := StreamID{Type: typ, ID: id}
	if err := sid.Validate(); err != nil {
		return StreamID{}, err
	}
	return sid, nil
}

// Validate reports whether the StreamID is well-formed.
func (s StreamID) Validate() error {
	if !slugRe.MatchString(s.Type) || !slugRe.MatchString(s.ID) {
		return ErrInvalidStreamID
	}
	return nil
}

// Canonical returns the storage form: Type + ":" + ID.
func (s StreamID) Canonical() string {
	return s.Type + canonicalSep + s.ID
}

// String renders the identity for logs/debug. Same as Canonical today;
// kept distinct so a future qualified form doesn't churn storage.
func (s StreamID) String() string { return s.Canonical() }

// ParseCanonical splits a storage-form stream_id back into a StreamID.
// Used by storage adapters when reconstructing rows.
func ParseCanonical(canonical string) (StreamID, error) {
	i := strings.IndexByte(canonical, canonicalSep[0])
	if i <= 0 || i == len(canonical)-1 {
		return StreamID{}, ErrInvalidStreamID
	}
	return NewStreamID(canonical[:i], canonical[i+1:])
}
