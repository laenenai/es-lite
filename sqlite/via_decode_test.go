package sqlite

import (
	"strings"
	"testing"

	"github.com/laenenai/es-lite/es"
)

// Unit tests for decodeVia, the decode behind scanEnvelope's three
// via_* columns (ADR 0041). SQLite carries no all-or-nothing CHECK
// (ADR 0041 §3.3: parity would mean another events rebuild), so unlike
// the span context's decode these shapes are NOT unreachable through
// the store: a restored dump or an out-of-band write can plant a
// half-set triple, and the strict read is the audit fact's only
// backstop. Via is an audit fact, not telemetry — a partially recorded
// delegation fails the read, naming the NULL columns, instead of
// silently reading as a zero Via that would forge an actor acting for
// itself. That is why all eight nil shapes are pinned here, against
// the helper directly rather than through the database.

func TestDecodeVia(t *testing.T) {
	const (
		ty        = "service"
		id        = "agent-runner"
		principal = "service:agent-runner"
	)
	p := func(s string) *string { return &s }

	for _, tc := range []struct {
		name                  string
		viaType, viaID, viaPr *string
		want                  es.ActorRef
		wantMissing           string // comma-joined NULL columns the error must name; empty for no error
	}{
		// The absent shape: three NULLs, every pre-0010 row and every
		// append without a delegation, reads as the zero ref.
		{"all three NULL", nil, nil, nil, es.ActorRef{}, ""},
		// Three values read as the ref they record. via_principal is
		// the derived "type:id" form the via index keys on and is not
		// re-derived or cross-checked here.
		{"all three set", p(ty), p(id), p(principal), es.ActorRef{Type: ty, ID: id}, ""},
		// The six mixed shapes are incomplete delegations: each fails,
		// naming every NULL column.
		{"via_type only", p(ty), nil, nil, es.ActorRef{}, "via_id, via_principal"},
		{"via_id only", nil, p(id), nil, es.ActorRef{}, "via_type, via_principal"},
		{"via_principal only", nil, nil, p(principal), es.ActorRef{}, "via_type, via_id"},
		{"type and id, principal NULL", p(ty), p(id), nil, es.ActorRef{}, "via_principal"},
		{"type and principal, id NULL", p(ty), nil, p(principal), es.ActorRef{}, "via_id"},
		{"id and principal, type NULL", nil, p(id), p(principal), es.ActorRef{}, "via_type"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ref, err := decodeVia(tc.viaType, tc.viaID, tc.viaPr)
			if tc.wantMissing == "" {
				if err != nil {
					t.Fatalf("decodeVia = %+v, %v; want no error", ref, err)
				}
				if ref != tc.want {
					t.Fatalf("decodeVia = %+v, want %+v", ref, tc.want)
				}
				return
			}
			if err == nil {
				t.Fatalf("decodeVia = %+v with a nil error, want the incomplete-delegation error naming %s", ref, tc.wantMissing)
			}
			if ref != (es.ActorRef{}) {
				t.Fatalf("decodeVia = %+v on an incomplete row, want the zero ref", ref)
			}
			for _, column := range strings.Split(tc.wantMissing, ", ") {
				if !strings.Contains(err.Error(), column) {
					t.Fatalf("decodeVia error %q does not name NULL column %s", err, column)
				}
			}
		})
	}
}
