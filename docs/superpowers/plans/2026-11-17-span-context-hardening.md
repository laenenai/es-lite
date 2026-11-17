# Span-Context Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the four residual risks from the ADR 0040 review: untested decode guards, SQLite's non-integer flags case, the caller-owned `AppendTx` path, and the test/doc polish left deferred.

**Architecture:** Pull each store's stored-value decode into a small unexported function so it can be unit-tested directly (the `CHECK` makes the bad states unreachable through SQL, so integration tests cannot plant them). Make SQLite's scan tolerate any non-integer value. Prove the `AppendTx` path persists a caller-supplied context. Then batch the deferred test and doc nits.

**Tech Stack:** Go 1.27, `pgx`/Postgres 17, `modernc.org/sqlite`.

**Spec:** `docs/adr/0040-producer-span-context.md` (already merged) and the final-review findings recorded in PR #21.

## Global Constraints

- No new module dependencies; no OpenTelemetry import; no schema change.
- Telemetry must never fail a read: every malformed stored value decodes to absent.
- Existing behaviour, hashes and the wire are unchanged.
- Go 1.27; `GOWORK=off` for every command. Conventional commits.

## Review Focus

1. **A non-integer value in SQLite's `trace_flags`** must read as absent, not error the scan — the dynamic-typing hole the previous wave left open. → Task 1 test.
2. **The caller-owned `AppendTx` path** must persist a context the caller supplies — it is outside any store decorator, so it is the path most likely to silently carry nothing. → Task 1 test.
3. **NULL still means absent** after the decode refactor — no behaviour change for every existing row. → Task 1 tests.
4. **One batch, many events**: a two-event batch carries the same context to both. → Task 2 test.
5. **Absent is exactly zero**, not merely "invalid" — a fabricated sparse context must not pass. → Task 2 assertion.

---

### Task 1: Store decode hardening

**Files:**
- Modify: `postgres/store.go` (the inline flags guard in `scanEnvelope`, ~558-575)
- Modify: `sqlite/store.go` (the `*int64` scan at ~673 and the guard at ~708-722)
- Create: `postgres/spancontext_decode_test.go` (`package postgres`)
- Create: `sqlite/spancontext_decode_test.go` (`package sqlite`)
- Modify: `postgres/spancontext_test.go` (add `TestSpanContextAppendTxRoundTrip`)

**Interfaces:**
- Consumes: `es.SpanContext`, `es.ParseSpanContext`.
- Produces: `func decodeSpanContext(traceID, spanID []byte, flags *int16) es.SpanContext` in `postgres`; `func decodeSpanContext(traceID, spanID []byte, flags any) es.SpanContext` in `sqlite`. Both unexported.

- [ ] **Step 1: Write the failing unit tests**

`postgres/spancontext_decode_test.go` (internal package): with valid 16/8-byte ids, table cases — `flags == nil` → zero; `-1` → zero; `300` → zero; `255` with valid ids → populated; `0` with valid ids and `nil` ids → zero; ids of length 15 → zero; ids of length 16/8 but all-zero → zero.

`sqlite/spancontext_decode_test.go` (internal package): the same plus `flags = int64(300)`, `int64(-1)`, `float64(1)`, `"abc"`, `[]byte{1}` → zero for every non-`int64` type; `int64(1)` with valid ids → populated.

- [ ] **Step 2: Run to verify they fail**

`GOWORK=off go test ./postgres/ -run TestDecodeSpanContext` and `./sqlite/ -run TestDecodeSpanContext`
Expected: build failure — `undefined: decodeSpanContext`.

- [ ] **Step 3: Implement the helpers and wire the scans**

Postgres: move the guard body into `decodeSpanContext`; `scanEnvelope` calls it with the scanned `*int16`.

SQLite: change the scan target from `flags *int16` to `var flags any`; `decodeSpanContext` type-switches: `nil` → zero; `int64` in `0..255` → `uint8`; every other type or out-of-range value → zero. Then `ParseSpanContext`, and on error zero.

Both helpers carry a doc comment: the `CHECK` makes malformed rows unreachable from the store; this is the restored-dump tolerance of ADR 0040 §4.

- [ ] **Step 4: Add the `AppendTx` round-trip test**

In `postgres/spancontext_test.go`, follow `postgres/tx_test.go` (`s.WorkspaceStore("…")`, `s.Pool().Begin`, `ws.AppendTx(ctx, narrowTx{tx}, params)`), append with a populated `SpanContext`, commit, and assert `ws.ReadStream` returns it. Use a distinct workspace name so the per-test truncate isolation is irrelevant.

- [ ] **Step 5: Run**

`GOWORK=off go test ./postgres/ -run 'TestDecodeSpanContext|TestSpanContext'` with `PG_DSN`, `GOWORK=off go test ./sqlite/ -run 'TestDecodeSpanContext|TestSpanContext'`, then `GOWORK=off go test ./...`, `golangci-lint run ./...`, `gofmt -l postgres sqlite`.

- [ ] **Step 6: Commit**

```bash
git add postgres/store.go sqlite/store.go postgres/spancontext_decode_test.go sqlite/spancontext_decode_test.go postgres/spancontext_test.go
git commit -m "fix(postgres,sqlite): make span-context decode total and tested"
```

---

### Task 2: Deferred test and doc polish

**Files:**
- Modify: `es/spancontext_test.go` (array conversions, a non-nil-empty case, sentinel identity)
- Modify: `es/hash_test.go` (`t.Fatalf` → `t.Fatal`)
- Modify: `postgres/spancontext_test.go` (one two-event batch in the round trip)
- Modify: `feedtest/conformance.go` (absent case asserts exact zero)
- Modify: `README.md` (the feed-timing clause)

**Interfaces:**
- Consumes: nothing new.
- Produces: nothing new.

- [ ] **Step 1: Tighten the tests**

- `es/spancontext_test.go`: replace the `bytes.Repeat`-rebuilt expectations with `[16]byte(trace)` / `[8]byte(span)`; add `{"empty non-nil", []byte{}, []byte{}, 0, false, false}`; assert `err != es.ErrMalformedSpanContext` alongside `errors.Is`.
- `es/hash_test.go`: `t.Fatalf("span context leaked into the canonical encoding")` → `t.Fatal(...)`.
- `postgres/spancontext_test.go`: `TestSpanContextRoundTrip` appends both events in one `AppendParams.Events` slice, then asserts both envelopes on `ReadStream` and `ReadAll`.
- `feedtest/conformance.go`: the absent case asserts `got[1].SpanContext != (es.SpanContext{})` in addition to `!IsValid()`.

- [ ] **Step 2: Fix the README timing clause**

In the "Tamper-evident trace context" bullet, make the remote-feed claim conditional:

```markdown
  id, flags) travels with the envelope and survives the store round trip and,
  once the feed contract carries it (ADR 0040 §3.5), the remote feed; but it is
  telemetry: ...
```

- [ ] **Step 3: Run**

`GOWORK=off go test ./es/ ./sqlite/`, `PG_DSN=... GOWORK=off go test ./postgres/ -run 'TestSpanContext|TestPostgresFeedConformance'`, `GOWORK=off go test ./...`, `golangci-lint run ./...`, `go vet`, `gofmt`, `git diff --check`.

- [ ] **Step 4: Commit**

```bash
git add es/spancontext_test.go es/hash_test.go postgres/spancontext_test.go feedtest/conformance.go README.md
git commit -m "test: tighten span-context assertions and qualify the feed claim"
```

---

## Out of scope

- The platform half of ADR 0040 (`AppendParams.SpanContext` at the tenant `AppendTx` call sites, the append/dispatch decorators, the relay serializer, the feed-client tolerance rule). It lives in the platform repository and is unaffected by these changes.
