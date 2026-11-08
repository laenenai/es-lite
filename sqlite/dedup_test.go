package sqlite_test

import (
	"context"
	"testing"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/sqlite"
)

// The command-dedup adapter (design D6, finding F6) over migration
// 0008's command_dedup table: the es.Deduplicator the generated handler
// flow (Task 33) will replay stored responses through. One database is
// one workspace (0001's rule), so there is no workspace-scoping to test
// here — the file's own boundary is the isolation Postgres's RLS
// enforces — leaving the two contract halves: round trip and the
// conflict-silent Save.

// TestDedupRoundTrip pins the storage half of the Deduplicator contract:
// Save stores the completed command's record, Lookup returns it
// verbatim, and a key that was never saved is a miss — (zero, false,
// nil) — not an error and not a made-up record.
func TestDedupRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	d := sqlite.NewDedupStore(s)

	key := es.DedupKey{
		StreamID:  "counter:rt-1",
		CommandID: "0192f3a0-0000-7000-8000-0000000000c1",
	}

	// The never-saved key: a miss with the zero record and no error.
	rec, found, err := d.Lookup(ctx, key)
	if err != nil {
		t.Fatalf("lookup before save: %v", err)
	}
	if found {
		t.Fatal("lookup before save: found=true, want false — the key was never saved")
	}
	if rec.Fingerprint != "" || rec.Response != nil {
		t.Fatalf("lookup before save: record = %+v, want the zero record", rec)
	}

	saved := es.DedupRecord{
		Fingerprint: "fp-round-trip",
		Response:    []byte(`{"count":1}`),
	}
	if err := d.Save(ctx, key, saved); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, found, err := d.Lookup(ctx, key)
	if err != nil {
		t.Fatalf("lookup after save: %v", err)
	}
	if !found {
		t.Fatal("lookup after save: found=false, want true")
	}
	if got.Fingerprint != saved.Fingerprint {
		t.Fatalf("fingerprint = %q, want %q", got.Fingerprint, saved.Fingerprint)
	}
	if string(got.Response) != string(saved.Response) {
		t.Fatalf("response = %q, want %q", got.Response, saved.Response)
	}
}

// TestDedupSaveConflictIsNotError pins the conflict-silent half of the
// contract (es.Deduplicator, migration 0008's header): the first
// completed response wins forever, so a Save whose key already holds a
// record stores nothing and returns nil — the conflict is NOT an error —
// and the re-Lookup the contract prescribes returns the FIRST record.
func TestDedupSaveConflictIsNotError(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	d := sqlite.NewDedupStore(s)

	key := es.DedupKey{
		StreamID:  "counter:conflict-1",
		CommandID: "0192f3a0-0000-7000-8000-0000000000c3",
	}
	first := es.DedupRecord{
		Fingerprint: "fp-first",
		Response:    []byte(`{"result":"first"}`),
	}
	second := es.DedupRecord{
		Fingerprint: "fp-second",
		Response:    []byte(`{"result":"second"}`),
	}

	if err := d.Save(ctx, key, first); err != nil {
		t.Fatalf("first save: %v", err)
	}
	// The conflicting Save: INSERT OR IGNORE swallows the primary-key
	// hit and returns nil.
	if err := d.Save(ctx, key, second); err != nil {
		t.Fatalf("second save: %v — a Save that hits the conflict is not an error by contract", err)
	}
	// The re-Lookup must return the FIRST record: the silent conflict
	// must not have overwritten it.
	got, found, err := d.Lookup(ctx, key)
	if err != nil || !found {
		t.Fatalf("re-lookup: found=%v err=%v", found, err)
	}
	if got.Fingerprint != first.Fingerprint || string(got.Response) != string(first.Response) {
		t.Fatalf("re-lookup = %+v, want the FIRST record %+v", got, first)
	}
}
