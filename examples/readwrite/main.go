// Command readwrite is a runnable read-your-writes example on SQLite.
//
// It appends two events, takes the consistency token the appends return, and
// waits until an in-process projection over projection.LogFeed has applied
// them, then reads the projection's read model. There is no broker and no
// external service: SQLite is both the event log and the checkpoint store, and
// the waiter is woken in process by the runner's progress publisher.
//
// Run it with:
//
//	go run ./examples/readwrite
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/projection"
	"github.com/laenenai/es-lite/sqlite"
)

const (
	// projectionName is the projection half of the checkpoint's (scope,
	// projection) key.
	projectionName = "notes"
	// feedPoll is how often LogFeed asks SQLite for new events. A real
	// deployment uses the eventfeed server instead (see README.md).
	feedPoll = 5 * time.Millisecond
	// waitTimeout bounds the read-your-writes wait. A request handler would
	// derive a shorter per-request deadline from its client's timeout.
	waitTimeout = 2 * time.Second
	// demoDSN is an in-memory database. The example owns the store for the
	// whole run, and the shared cache keeps the in-memory DB alive.
	demoDSN = "file:readwrite?mode=memory&cache=shared"
)

// Note is one row of the example read model.
type Note struct {
	StreamID string
	Text     string
}

// Notes is an in-memory read model. Dispatch appends to it on the runner's
// goroutine; Snapshot reads it on the caller's, so the mutex is load-bearing.
type Notes struct {
	mu   sync.Mutex
	rows []Note
}

// apply is the projection dispatch: it updates the read model and nothing
// else. The runner writes the checkpoint only after this returns, so a waiter
// never observes a position over a row that is not there yet.
func (n *Notes) apply(_ context.Context, e es.Envelope) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.rows = append(n.rows, Note{StreamID: e.StreamID.String(), Text: string(e.Payload)})
	return nil
}

// Snapshot returns a copy of the read model's rows.
func (n *Notes) Snapshot() []Note {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]Note(nil), n.rows...)
}

// App is the example's wiring: one SQLite store, one in-memory read model, one
// projection runner, and the waiter that shares the runner's progress store.
type App struct {
	Store    *sqlite.Store
	Notes    *Notes
	Progress *sqlite.ProgressStore
	Waiter   *projection.Waiter
	Runner   *projection.Runner
}

// NewApp wires the projection and the waiter over store. SQLite is a
// single-workspace backend: its envelopes carry an empty Workspace and its
// tokens use the empty scope, so Runner.Scope and every WaitFor scope are "".
func NewApp(store *sqlite.Store) *App {
	notes := &Notes{}
	progress := sqlite.NewProgressStore(store)
	waiter := projection.NewWaiter(progress)
	runner := &projection.Runner{
		Name:          projectionName,
		Scope:         "",
		ProgressStore: progress,
		// The same-process publisher wakes the waiter on every checkpoint
		// save. Cross-process deployments add the store's notification
		// listener here instead.
		Progress: waiter,
		Dispatch: notes.apply,
	}
	return &App{
		Store:    store,
		Notes:    notes,
		Progress: progress,
		Waiter:   waiter,
		Runner:   runner,
	}
}

// Close stops the waiter's refresh worker. Call it after the feed has stopped.
func (a *App) Close() { a.Waiter.Close() }

// RunFeed runs the projection over the log until ctx ends. Run it in a
// goroutine; it returns ctx.Err() on cancellation.
func (a *App) RunFeed(ctx context.Context) error {
	return a.Runner.Run(ctx, &projection.LogFeed{Source: a.Store, Poll: feedPoll})
}

// AppendNotes appends two note events and returns the token covering both.
func AppendNotes(ctx context.Context, store *sqlite.Store) (es.Token, error) {
	sid, err := es.NewStreamID("note", "demo")
	if err != nil {
		return es.Token{}, err
	}
	texts := []string{"first note", "second note"}
	results := make([]es.AppendResult, 0, len(texts))
	for i, text := range texts {
		res, err := store.Append(ctx, es.AppendParams{
			StreamID:        sid,
			ExpectedVersion: uint64(i),
			Events: []es.EventData{{
				TypeURL:       "note.v1.Added",
				SchemaVersion: 1,
				Payload:       []byte(text),
			}},
		})
		if err != nil {
			return es.Token{}, fmt.Errorf("append note %d: %w", i+1, err)
		}
		results = append(results, res)
	}
	return es.TokenFor("", results...), nil
}

// Summary is what run demonstrates and prints.
type Summary struct {
	Token es.Token
	Wait  projection.WaitResult
	Notes []Note
}

// writef writes the example's output. A failed stdout write is not worth
// failing the demonstration over, so the error is deliberately dropped.
func writef(out io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(out, format, args...)
}

// run executes the whole example over dsn: open the store, start the
// projection, append, wait for the token, and read the projection. The feed
// runs in a goroutine so the wait races a genuinely asynchronous apply, not a
// same-goroutine call.
func run(ctx context.Context, dsn string, out io.Writer) (Summary, error) {
	store, err := sqlite.Open(ctx, dsn)
	if err != nil {
		return Summary{}, fmt.Errorf("open sqlite: %w", err)
	}
	defer store.Close()

	app := NewApp(store)
	feedCtx, stop := context.WithCancel(ctx)
	defer stop()
	feedDone := make(chan error, 1)
	go func() { feedDone <- app.RunFeed(feedCtx) }()
	defer func() {
		stop()
		// Run returns ctx.Err() on cancellation; that is this example's
		// normal shutdown. Any other feed failure is still worth printing.
		if err := <-feedDone; err != nil && !errors.Is(err, context.Canceled) {
			writef(out, "feed stopped: %v\n", err)
		}
		app.Close()
	}()

	tok, err := AppendNotes(ctx, store)
	if err != nil {
		return Summary{}, err
	}
	writef(out, "appended 2 events: token position %d, wire %q\n", tok.Position, tok.String())

	res, err := app.Waiter.WaitFor(ctx, "", projectionName, tok, waitTimeout)
	if err != nil {
		return Summary{}, fmt.Errorf("WaitFor: %w", err)
	}
	writef(out, "wait: reached=%v status=%s position=%d\n", res.Reached, res.Status, res.Checkpoint.Position)

	notes := app.Notes.Snapshot()
	writef(out, "read model:\n")
	for _, n := range notes {
		writef(out, "  %s: %s\n", n.StreamID, n.Text)
	}

	sum := Summary{Token: tok, Wait: res, Notes: notes}
	if !res.Reached {
		return sum, fmt.Errorf("projection did not apply token position %d within %s", tok.Position, waitTimeout)
	}
	return sum, nil
}

func main() {
	if _, err := run(context.Background(), demoDSN, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "readwrite:", err)
		os.Exit(1)
	}
}
