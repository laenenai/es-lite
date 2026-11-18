package postgres

import (
	"strings"
	"testing"

	"github.com/laenenai/es-lite/es"
)

// Unit tests for decodeVia, the reconstruction behind scanEnvelope's
// three via_* columns. The events_via_complete CHECK makes every
// malformed shape below unreachable through the store — no SQL writer
// can plant one, which is why these cases are unit-tested against the
// helper directly rather than through the database: the strictness is
// the second line of defence, for a restored dump or a constraint
// bypassed during a migration (ADR 0041 §4). Via is an audit fact, the
// deliberate opposite of the span context's tolerance: a half-recorded
// delegation is an error naming the NULL columns, never a silent zero
// Via, and "absent" is exactly the zero ActorRef.

// strPtr boxes a string so table cases can pass the *string that the
// text via_* columns scan into.
func strPtr(s string) *string { return &s }

func TestDecodeVia(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		viaType, viaID, principal *string
		want                      es.ActorRef
		wantErr                   []string // substrings the error must name
	}{
		// The absent shape: three NULLs, the shape of every row
		// written before migration 0012 and of every append without
		// a delegation.
		{"all three NULL", nil, nil, nil, es.ActorRef{}, nil},
		// The complete shape: the only populated one the CHECK admits.
		{"all three set", strPtr("service"), strPtr("agent-runner"), strPtr("service:agent-runner"),
			es.ActorRef{Type: "service", ID: "agent-runner"}, nil},
		// Every partial shape fails, naming the NULL columns: a
		// half-recorded delegation is an audit-fact defect, not a
		// zero Via to read through.
		{"type NULL", nil, strPtr("agent-runner"), strPtr("service:agent-runner"), es.ActorRef{},
			[]string{"via_type"}},
		{"id NULL", strPtr("service"), nil, strPtr("service:agent-runner"), es.ActorRef{},
			[]string{"via_id"}},
		{"principal NULL", strPtr("service"), strPtr("agent-runner"), nil, es.ActorRef{},
			[]string{"via_principal"}},
		{"type and id NULL", nil, nil, strPtr("service:agent-runner"), es.ActorRef{},
			[]string{"via_type", "via_id"}},
		{"type and principal NULL", nil, strPtr("agent-runner"), nil, es.ActorRef{},
			[]string{"via_type", "via_principal"}},
		{"id and principal NULL", strPtr("service"), nil, nil, es.ActorRef{},
			[]string{"via_id", "via_principal"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decodeVia(tc.viaType, tc.viaID, tc.principal)
			if len(tc.wantErr) == 0 {
				if err != nil {
					t.Fatalf("decodeVia = %+v, %v; want %+v with no error", got, err, tc.want)
				}
				if got != tc.want {
					t.Fatalf("decodeVia = %+v, want %+v", got, tc.want)
				}
				return
			}
			if err == nil {
				t.Fatalf("decodeVia = %+v, want an error naming %v", got, tc.wantErr)
			}
			for _, want := range tc.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("decodeVia error %q does not name %s", err, want)
				}
			}
		})
	}
}
