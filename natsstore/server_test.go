package natsstore

import (
	"context"
	"errors"
	"testing"
)

// wsTokenIndex locates the ws token right after the prefix in
// <prefix>.<ws>.<method>. The authoritative-workspace derivation and the
// subject==identity fail-closed cross-check are Tenant / RequireSubjectWorkspace
// in middleware.go (architecture ADR 0005) — see TestTenantAndRequireSubjectWorkspace.
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

func TestSubjectToken(t *testing.T) {
	cases := []struct {
		subject string
		idx     int
		want    string
	}{
		{"svc.eslite.ws1.append", 2, "ws1"},
		{"svc.eslite.ws1.append", 3, "append"},
		{"svc.eslite.ws1.append", 99, ""},
		{"svc.eslite.ws1.append", -1, ""},
	}
	for _, c := range cases {
		if got := SubjectToken(c.subject, c.idx); got != c.want {
			t.Fatalf("SubjectToken(%q, %d) = %q, want %q", c.subject, c.idx, got, c.want)
		}
	}
}

func TestTenantAndRequireSubjectWorkspace(t *testing.T) {
	wsIndex := wsTokenIndex(DefaultPrefix) // svc.eslite.<ws>.<method>

	base := func(ctx context.Context, m MsgContext) ([]byte, error) {
		return []byte(WorkspaceFrom(ctx)), nil
	}
	extract := func(m MsgContext) (Identity, error) {
		return Identity{Workspace: SubjectToken(m.Subject, wsIndex)}, nil
	}
	h := chain(base, Tenant(extract), RequireSubjectWorkspace(wsIndex))

	t.Run("matching subject/identity succeeds", func(t *testing.T) {
		b, err := h(context.Background(), MsgContext{Subject: DefaultPrefix + ".ws1.append"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if string(b) != "ws1" {
			t.Fatalf("got workspace %q, want ws1", b)
		}
	})

	t.Run("mismatched subject/identity fails closed", func(t *testing.T) {
		mismatched := chain(base, Tenant(func(MsgContext) (Identity, error) {
			return Identity{Workspace: "other-ws"}, nil
		}), RequireSubjectWorkspace(wsIndex))
		if _, err := mismatched(context.Background(), MsgContext{Subject: DefaultPrefix + ".ws1.append"}); err == nil {
			t.Fatal("expected error on subject/identity mismatch, got nil")
		}
	})

	t.Run("extractor error short-circuits before the handler", func(t *testing.T) {
		wantErr := errors.New("boom")
		called := false
		failing := chain(func(ctx context.Context, m MsgContext) ([]byte, error) {
			called = true
			return nil, nil
		}, Tenant(func(MsgContext) (Identity, error) { return Identity{}, wantErr }))
		if _, err := failing(context.Background(), MsgContext{Subject: DefaultPrefix + ".ws1.append"}); !errors.Is(err, wantErr) {
			t.Fatalf("got err %v, want %v", err, wantErr)
		}
		if called {
			t.Fatal("handler ran despite extractor error")
		}
	})
}

func TestHeaderIdentity(t *testing.T) {
	extract := HeaderIdentity("X-Workspace")
	m := MsgContext{}
	m.Header = map[string][]string{"X-Workspace": {"ws9"}}
	id, err := extract(m)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id.Workspace != "ws9" {
		t.Fatalf("got workspace %q, want ws9", id.Workspace)
	}
}
