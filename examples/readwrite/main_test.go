package main

import (
	"context"
	"io"
	"testing"

	"go.uber.org/goleak"

	"github.com/laenenai/es-lite/projection"
)

// TestReadYourWritesExample runs the example end to end: two appended notes,
// the token they produce, a bounded wait, and the rows the projection read
// model holds when the wait reports reached and live.
func TestReadYourWritesExample(t *testing.T) {
	// run stops the feed and closes the waiter on every return; the example
	// must leave no goroutines behind for a long-lived process.
	defer goleak.VerifyNone(t)

	ctx := context.Background()
	sum, err := run(ctx, "file:"+t.Name()+"?mode=memory&cache=shared", io.Discard)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	// SQLite is single-workspace: the token and the wait scope are empty.
	if sum.Token.Scope != "" {
		t.Fatalf("token scope = %q, want the empty SQLite scope", sum.Token.Scope)
	}
	if sum.Token.Position == 0 {
		t.Fatal("token position = 0, want the appended events' position")
	}
	if !sum.Wait.Reached {
		t.Fatalf("wait did not reach token position %d: %+v", sum.Token.Position, sum.Wait)
	}
	if sum.Wait.Status != projection.StatusLive {
		t.Fatalf("wait status = %v, want %v", sum.Wait.Status, projection.StatusLive)
	}
	if sum.Wait.Checkpoint.Position < sum.Token.Position {
		t.Fatalf("wait checkpoint position = %d, want at least the token %d", sum.Wait.Checkpoint.Position, sum.Token.Position)
	}

	if len(sum.Notes) != 2 {
		t.Fatalf("read model rows = %d, want 2: %+v", len(sum.Notes), sum.Notes)
	}
	if got, want := sum.Notes[0].Text, "first note"; got != want {
		t.Fatalf("row 0 text = %q, want %q", got, want)
	}
	if got, want := sum.Notes[1].Text, "second note"; got != want {
		t.Fatalf("row 1 text = %q, want %q", got, want)
	}
	for i, row := range sum.Notes {
		if row.StreamID != "note:demo" {
			t.Fatalf("row %d stream = %q, want %q", i, row.StreamID, "note:demo")
		}
	}
}
