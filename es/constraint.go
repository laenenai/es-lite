package es

import "fmt"

// ConstraintOpKind distinguishes a uniqueness Claim from a Release.
type ConstraintOpKind uint8

const (
	// ClaimOp reserves a (scope, value); a duplicate fails the append.
	ClaimOp ConstraintOpKind = iota
	// ReleaseOp frees a previously claimed (scope, value).
	ReleaseOp
)

// ConstraintOp is a uniqueness operation emitted by Decide and applied by the
// store in the SAME transaction as the event append (ADR 0008). A Claim that
// collides with an existing one fails the whole append with
// ErrConstraintViolated — no event is persisted. Uniqueness is scoped to the
// stream's workspace.
// Value is stored exactly as given. For personal data the caller must supply a
// keyed hash/MAC it controls, not the raw value; es-lite has no PII flag and no
// server-side keystore (ADR 0025).
type ConstraintOp struct {
	Op    ConstraintOpKind
	Scope string // namespace, e.g. "user.email"
	Value string // the value being made unique within (workspace, scope)
}

// Claim builds a Claim op.
func Claim(scope, value string) ConstraintOp {
	return ConstraintOp{Op: ClaimOp, Scope: scope, Value: value}
}

// Release builds a Release op.
func Release(scope, value string) ConstraintOp {
	return ConstraintOp{Op: ReleaseOp, Scope: scope, Value: value}
}

// Validate reports whether the op has a known kind and a non-empty scope.
func (o ConstraintOp) Validate() error {
	if o.Op != ClaimOp && o.Op != ReleaseOp {
		return fmt.Errorf("%w: unknown op %d", ErrInvalidConstraint, o.Op)
	}
	if o.Scope == "" {
		return fmt.Errorf("%w: empty scope", ErrInvalidConstraint)
	}
	return nil
}
