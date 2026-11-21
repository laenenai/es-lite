package es

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
)

// decisionDomain versions the binary encoding of an append's constraint
// decisions (ADR 0043). A decoder rejects any other domain, so a future layout
// is never mistaken for this one.
const decisionDomain = "es-lite.append-decisions.v1"

// ErrClaimsWithoutDecisions reports a rebuild or verify against a store whose
// live claims index has no decision history. Every append writes its decision
// row in the same transaction, so that state is inconsistent — a partial
// restore, or rows written outside the append path — and the log cannot
// license or reconstruct it (ADR 0043 §3.5).
var ErrClaimsWithoutDecisions = errors.New("es-lite: live claims have no decision history")

// ClaimsRebuildReport summarizes a successful RebuildClaims: Decisions is the
// number of decision records replayed and Claims the number of live claims
// they produced.
type ClaimsRebuildReport struct {
	Decisions, Claims int64
}

// ClaimsVerifyReport is the result of VerifyClaims. OK reports that rebuilding
// the claims index from the decision log reproduced the live index exactly.
// FirstDivergence names the first differing (scope, value, stream) when it did
// not.
type ClaimsVerifyReport struct {
	OK              bool
	Decisions       int64
	Claims          int64
	FirstDivergence string
}

// ClaimsMaintainer is implemented by stores that can reconstruct the unique
// claims index from the append decisions recorded alongside the events
// (ADR 0043). Every claim is written with its decision row in the same
// transaction, so in a store written by v0.21+ the two always agree; these
// operations exist for restore, clone, and verification.
type ClaimsMaintainer interface {
	// RebuildClaims replaces the live claims index by replaying every
	// recorded decision in append order. Live claims with no decision
	// history are inconsistent, so it returns ErrClaimsWithoutDecisions
	// rather than guessing at a log or deleting an index it cannot
	// reconstruct.
	RebuildClaims(context.Context) (ClaimsRebuildReport, error)

	// VerifyClaims rebuilds into a temporary index and compares it with the
	// live one, reporting the first divergence. It does not modify the live
	// index.
	VerifyClaims(context.Context) (ClaimsVerifyReport, error)
}

// EncodeDecisionOps serializes an append's non-event constraint decisions to
// the versioned binary form stored in append_decisions (ADR 0043). Value is
// opaque bytes and is never normalized. The layout is:
//
//	appendString(decisionDomain)
//	u32 count
//	count × { u8 kind, appendString(scope), appendString(value) }
func EncodeDecisionOps(ops []ConstraintOp) []byte {
	b := appendString(nil, decisionDomain)
	b = binary.BigEndian.AppendUint32(b, uint32(len(ops)))
	for _, op := range ops {
		b = append(b, byte(op.Op))
		b = appendString(b, op.Scope)
		b = appendString(b, op.Value)
	}
	return b
}

// DecodeDecisionOps parses the encoding produced by EncodeDecisionOps. It
// rejects a wrong domain, an unknown kind, a count larger than the remaining
// bytes, any string length that exceeds the remaining buffer, and trailing
// bytes. The decoded slice grows with the data actually present; the
// attacker-controlled count never drives an allocation.
func DecodeDecisionOps(b []byte) ([]ConstraintOp, error) {
	domain, b, err := readDecisionString(b, "domain")
	if err != nil {
		return nil, err
	}
	if domain != decisionDomain {
		return nil, fmt.Errorf("es-lite: decision ops: unknown domain %q", domain)
	}
	if len(b) < 4 {
		return nil, fmt.Errorf("es-lite: decision ops: truncated count")
	}
	count := binary.BigEndian.Uint32(b[:4])
	b = b[4:]
	if uint64(count) > uint64(len(b)) {
		return nil, fmt.Errorf("es-lite: decision ops: count %d exceeds %d remaining bytes", count, len(b))
	}
	var ops []ConstraintOp
	for i := uint32(0); i < count; i++ {
		if len(b) < 1 {
			return nil, fmt.Errorf("es-lite: decision ops: op %d: truncated kind", i)
		}
		kind := ConstraintOpKind(b[0])
		b = b[1:]
		if kind != ClaimOp && kind != ReleaseOp {
			return nil, fmt.Errorf("es-lite: decision ops: op %d: unknown kind %d: %w", i, kind, ErrInvalidConstraint)
		}
		scope, rest, err := readDecisionString(b, "scope")
		if err != nil {
			return nil, err
		}
		value, rest, err := readDecisionString(rest, "value")
		if err != nil {
			return nil, err
		}
		b = rest
		ops = append(ops, ConstraintOp{Op: kind, Scope: scope, Value: value})
	}
	if len(b) != 0 {
		return nil, fmt.Errorf("es-lite: decision ops: %d trailing bytes", len(b))
	}
	return ops, nil
}

// readDecisionString consumes a u32 length prefix and that many bytes.
func readDecisionString(b []byte, field string) (string, []byte, error) {
	if len(b) < 4 {
		return "", nil, fmt.Errorf("es-lite: decision ops: truncated %s length", field)
	}
	n := binary.BigEndian.Uint32(b[:4])
	if uint64(n) > uint64(len(b)-4) {
		return "", nil, fmt.Errorf("es-lite: decision ops: %s length %d exceeds %d remaining bytes", field, n, len(b)-4)
	}
	end := 4 + int(n)
	return string(b[4:end]), b[end:], nil
}
