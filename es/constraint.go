package es

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
type ConstraintOp struct {
	Op    ConstraintOpKind
	Scope string // namespace, e.g. "user.email"
	Value string // the value being made unique within (workspace, scope)

	// PII marks Value as personal data: backends with a keystore persist a
	// keyed hash (HMAC under the workspace key) rather than plaintext, so
	// crypto-shredding the workspace unlinks it (ADR 0004/0008). Backends
	// without a keystore (SQLite) store plaintext and ignore this flag.
	PII bool
}

// Claim builds a Claim op. Set pii=true when value is personal data.
func Claim(scope, value string, pii bool) ConstraintOp {
	return ConstraintOp{Op: ClaimOp, Scope: scope, Value: value, PII: pii}
}

// Release builds a Release op.
func Release(scope, value string, pii bool) ConstraintOp {
	return ConstraintOp{Op: ReleaseOp, Scope: scope, Value: value, PII: pii}
}
