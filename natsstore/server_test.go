package natsstore

import (
	"context"
	"testing"
)

// TestResolveWorkspace covers architecture ADR 0005 §D: the workspace comes
// from the subject and is cross-checked against the authenticated workspace,
// fail-closed on mismatch.
func TestResolveWorkspace(t *testing.T) {
	s := &Server{}
	ctx := context.Background()

	cases := []struct {
		name    string
		ctx     context.Context
		subject string
		wantWS  string
		wantErr bool
	}{
		{"region+ws, no auth", ctx, "svc.eslite.local.ws1.append", "ws1", false},
		{"no region, ws extracted", ctx, "svc.eslite.ws1.append", "ws1", false},
		{"auth matches subject", WithWorkspace(ctx, "ws1"), "svc.eslite.local.ws1.append", "ws1", false},
		{"auth mismatch => fail-closed", WithWorkspace(ctx, "ws2"), "svc.eslite.local.ws1.append", "", true},
		{"subject too short", ctx, "svc.eslite", "", true},
		{"wildcard ws token", ctx, "svc.eslite.local.*.append", "", true},
		{"empty ws token", ctx, "svc.eslite..append", "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ws, err := s.resolveWorkspace(c.ctx, c.subject)
			if c.wantErr {
				if err == nil {
					t.Fatalf("subject %q: expected error, got ws=%q", c.subject, ws)
				}
				return
			}
			if err != nil {
				t.Fatalf("subject %q: unexpected error: %v", c.subject, err)
			}
			if ws != c.wantWS {
				t.Fatalf("subject %q: ws=%q, want %q", c.subject, ws, c.wantWS)
			}
		})
	}
}
