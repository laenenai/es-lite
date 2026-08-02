package natsstore

import "testing"

// wsTokenIndex locates the ws token right after the prefix in
// <prefix>.<ws>.<method>. The authoritative-workspace derivation and the
// subject==identity fail-closed cross-check themselves live in natskit
// (Tenant / RequireSubjectWorkspace, architecture ADR 0005) and are tested
// there; here we only cover the index es-lite feeds them.
func TestWSTokenIndex(t *testing.T) {
	cases := map[string]int{
		"svc.eslite":       2, // svc(0) eslite(1) ws(2)
		"svc.eslite.local": 3, // + region ⇒ ws(3)
		"svc.eslite.eu1":   3,
		"a":                1,
	}
	for prefix, want := range cases {
		if got := wsTokenIndex(prefix); got != want {
			t.Fatalf("wsTokenIndex(%q) = %d, want %d", prefix, got, want)
		}
	}
}
