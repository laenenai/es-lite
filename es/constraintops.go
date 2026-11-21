package es

import (
	"context"
	"encoding/binary"
	"fmt"
)

// constraintOpsDomain versions the binary encoding of an append's constraint
// ops stored on the first event of the batch (ADR 0043). A decoder rejects any
// other domain, so a future layout is never mistaken for this one.
const constraintOpsDomain = "es-lite.constraint-ops.v1"

// ClaimsRebuildReport summarizes a successful RebuildClaims: Ops is the number
// of op-bearing event records replayed and Claims the number of live claims
// they produced.
type ClaimsRebuildReport struct {
	Ops, Claims int64
}

// ClaimsVerifyReport is the result of VerifyClaims. OK reports that rebuilding
// the claims index from the event log's constraint ops reproduced the live
// index exactly. FirstDivergence names the first differing (scope, value,
// stream) when it did not.
type ClaimsVerifyReport struct {
	OK              bool
	Ops             int64
	Claims          int64
	FirstDivergence string
}

// ClaimsMaintainer is implemented by stores that can reconstruct the unique
// claims index from the constraint ops recorded on their event rows
// (ADR 0043). Every op-bearing append carries at least one event, so in a
// store written by v0.21+ the index and the log always agree; these operations
// exist for restore, clone, and verification.
type ClaimsMaintainer interface {
	// RebuildClaims replaces the live claims index by replaying every
	// op-bearing event in global position order. Live claims with no
	// ops in the event log are inconsistent, so it returns
	// ErrClaimsWithoutDecisions rather than guessing at a log or deleting
	// an index it cannot reconstruct.
	RebuildClaims(context.Context) (ClaimsRebuildReport, error)

	// VerifyClaims rebuilds into a temporary index and compares it with the
	// live one, reporting the first divergence. It does not modify the live
	// index.
	VerifyClaims(context.Context) (ClaimsVerifyReport, error)
}

// EncodeConstraintOps serializes an append's constraint ops to the versioned
// binary form stored in events.constraint_ops (ADR 0043). Value is opaque
// bytes and is never normalized. The layout is:
//
//	appendString(constraintOpsDomain)
//	u32 count
//	count × { u8 kind, appendString(scope), appendString(value) }
func EncodeConstraintOps(ops []ConstraintOp) []byte {
	b := appendString(nil, constraintOpsDomain)
	b = binary.BigEndian.AppendUint32(b, uint32(len(ops)))
	for _, op := range ops {
		b = append(b, byte(op.Op))
		b = appendString(b, op.Scope)
		b = appendString(b, op.Value)
	}
	return b
}

// DecodeConstraintOps parses the encoding produced by EncodeConstraintOps. It
// rejects a wrong domain, an unknown kind, a count larger than the remaining
// bytes, any string length that exceeds the remaining buffer, and trailing
// bytes. The decoded slice grows with the data actually present; the
// attacker-controlled count never drives an allocation.
func DecodeConstraintOps(b []byte) ([]ConstraintOp, error) {
	domain, b, err := readConstraintString(b, "domain")
	if err != nil {
		return nil, err
	}
	if domain != constraintOpsDomain {
		return nil, fmt.Errorf("es-lite: constraint ops: unknown domain %q", domain)
	}
	if len(b) < 4 {
		return nil, fmt.Errorf("es-lite: constraint ops: truncated count")
	}
	count := binary.BigEndian.Uint32(b[:4])
	b = b[4:]
	if uint64(count) > uint64(len(b)) {
		return nil, fmt.Errorf("es-lite: constraint ops: count %d exceeds %d remaining bytes", count, len(b))
	}
	var ops []ConstraintOp
	for i := uint32(0); i < count; i++ {
		if len(b) < 1 {
			return nil, fmt.Errorf("es-lite: constraint ops: op %d: truncated kind", i)
		}
		kind := ConstraintOpKind(b[0])
		b = b[1:]
		if kind != ClaimOp && kind != ReleaseOp {
			return nil, fmt.Errorf("es-lite: constraint ops: op %d: unknown kind %d: %w", i, kind, ErrInvalidConstraint)
		}
		scope, rest, err := readConstraintString(b, "scope")
		if err != nil {
			return nil, err
		}
		value, rest, err := readConstraintString(rest, "value")
		if err != nil {
			return nil, err
		}
		b = rest
		ops = append(ops, ConstraintOp{Op: kind, Scope: scope, Value: value})
	}
	if len(b) != 0 {
		return nil, fmt.Errorf("es-lite: constraint ops: %d trailing bytes", len(b))
	}
	return ops, nil
}

// readConstraintString consumes a u32 length prefix and that many bytes.
func readConstraintString(b []byte, field string) (string, []byte, error) {
	if len(b) < 4 {
		return "", nil, fmt.Errorf("es-lite: constraint ops: truncated %s length", field)
	}
	n := binary.BigEndian.Uint32(b[:4])
	if uint64(n) > uint64(len(b)-4) {
		return "", nil, fmt.Errorf("es-lite: constraint ops: %s length %d exceeds %d remaining bytes", field, n, len(b)-4)
	}
	end := 4 + int(n)
	return string(b[4:end]), b[end:], nil
}
