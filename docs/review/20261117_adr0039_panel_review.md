# Panel Review: ADR 0039 — Scoped Feed Wake-Ups

> Produced by a separate model (GLM-5.3 via the LiteLLM provider) acting as a
> combined CTO / CPO / CISO review panel. Read-only review of
> `docs/adr/0039-scoped-feed-wakeups.md` before implementation. The ADR was
> revised in response; see its appendix "Panel review changes".

- **Reviewers:** combined CTO / CPO / CISO panel (adversarial, read-only)
- **Date:** 2026-11-17
- **Artifact under review:** `docs/adr/0039-scoped-feed-wakeups.md` (Proposed, not implemented — verified: no `ScopedWaiter`, `Hub`, `WithAfterAppend`, `pg_notify`, `NotifyListener`, or `WithAppendNotify` exists anywhere in the module)
- **Related reviewed in evidence:** ADRs 0002, 0035, 0036, 0037; spec §3/§5.12; README; `docs/deploy/README.md`; code as listed in §1.2

**Verdict (summary): Approve with changes.** The seam design (Phase A+B) is sound, minimal, and safe; the C2 (LISTEN/NOTIFY) section is not approvable as written — its security mitigation is internally incoherent, its "can never fail the append" claim is factually false, and the Hub invariants as specified leak map entries in a normal interleaving. Details and the minimal change set are in §7.

---

## 1. Claim verification

### 1.1 Claims verified true (evidence checked)

| ADR 0039 claim | Evidence | Verdict |
|---|---|---|
| `defaultPoll = 250ms`, ±20% jitter (`feed/server.go:21,164`) | `feed/server.go:21`, `:164-166` (`0.8 + 0.4*Float64()`) | ✅ |
| `feed.Waiter` optional wakeup, `(woke, err)` (`feed/ports.go:182-194`) | `feed/ports.go:192-194` | ✅ |
| `idleWait` is a wake-confirmed floor; genuine wake bypasses, timeout keeps it (`feed/server.go:616-649`) | `feed/server.go:631-648`; pinned by `TestWaiterWakeBypassesFloor` (`feed/server_test.go:1519-1538`) | ✅ |
| Clamp loop's `wait` uses the seam without a floor (`feed/server.go:584-597`) | `feed/server.go:584-597` (discards `woke`) | ✅ |
| One shared `Waiter` for all streams | `Config.Waiter` (`feed/server.go:42`), used by every stream | ✅ |
| `Server.stream` already carries `req` (`feed/server.go:389`) | `feed/server.go:389` | ✅ |
| `Workspace.append` at `postgres/wsstore.go:185`; C2's insertion point "after the events, the `log_order` row and the claims" matches the body's shape | `postgres/wsstore.go:185-371` (events 254-333, log_order 346-360, claims 362-364) | ✅ |
| `postgres.Option` pattern (`postgres/store.go:43`) | `postgres/store.go:43` | ✅ |
| `aggregate.WithAppend` replaces the append fn (`aggregate/runtime.go:212-219`, used at `398-401`) | `runtime.go:214-220`, `:398-401` | ✅ |
| Wrapping erases `*postgres.Workspace`'s `AppendTx`/`ReadStreamTx` (`postgres/store.go:355-370`) | `store.go:356-366` | ✅ |
| A claim-only append succeeds with zero envelopes; `Envelope.Workspace` empty on SQLite | `wsstore.go:171-181` (no event-count check), `:346` (`len(p.Events) > 0` gate), `runtime.go:371-374`; `es/envelope.go:29-30` | ✅ |
| "one real transaction id per non-idle poll round" quote from ADR 0037 | `docs/adr/0037:173-174` | ✅ |
| ADR 0002's "no push source ships" / "no append-time NOTIFY" | `docs/adr/0002:65-76`, `:98`; already anticipates "a future ADR's to add" (`:76`) | ✅ amendment procedurally anticipated |
| Spec §5.12 defers LISTEN/NOTIFY as "a post-gate, separately-ADR'd decision" | spec `:258-259` | ✅ |
| pgx provides `WaitForNotification`; no new module dep | `pgx v5.10.0` `conn.go:415` returns `(*pgconn.Notification, error)` with `Channel`/`Payload` (`pgconn/pgconn.go:42-46`); `go.mod` has no NATS (ADR 0036 removal, `docs/adr/0036:60-64`) | ✅ |
| NOTIFY payload limit 8000 bytes; NOTIFY is transactional; PgBouncer transaction pooling breaks LISTEN; LISTEN needs no table privilege | PostgreSQL 18 docs (sql-notify): "must be shorter than 8000 bytes"; "not delivered until and unless the transaction is committed" | ✅ |
| LISTEN/NOTIFY is not RLS-filtered; payload visible to all listeners | PG docs: "Notifications are visible to all users." | ✅ — and it's worse than the ADR states (see CISO-1) |

### 1.2 Claims found wrong, unsound, or unverifiable

These are findings, cross-referenced from the lens sections below:

1. **"The hint can never fail the append" (§3.3 C2, line 281) is false.** `pg_notify` executes inside the append transaction; per the PostgreSQL docs: *"If this queue becomes full, transactions calling NOTIFY will fail at commit."* The append's own COMMIT fails — which is not merely the §2 anti-goal's "committed append into an apparent failure"; it prevents the commit entirely. The ADR's parenthetical only covers the 8000-byte payload limit (CISO-1d).
2. **§2 anti-goal "any new lock on the append path" is contradicted by the ADR's own §3.3/§5**, which knowingly add the commit-time notify-queue lock for C2 (CTO-1).
3. **The §6 HMAC mitigation is incoherent as written** — "an HMAC of the scope under a key held only by the gateway" cannot work when the payload is emitted by `pg_notify` inside the *appender's* transaction (CISO-1a).
4. **The Hub invariants, exactly as specified, leak map entries** in a normal interleaving (Wake-during-park followed by last-waiter exit), contradicting the §8 acceptance criterion "10k scopes that come and go leave … no map entries" (CTO-2).
5. **§1's "a global wake converts one append into N fence draws" is imprecise post-errata**: since the 2026-11-07 idle short-circuit (ADR 0037 Errata §1; `postgres/wsstore.go:689-701`), a woken stream of an *unrelated* scope short-circuits on its own committed max and burns no xid — it costs a RawHead-shaped transaction, not a fence draw (CTO-4).
6. **"Validate at Open" for the payload limit (§3.3 C2, line 281) is not implementable as stated**: `Open` is per-Store (`postgres/store.go:127`); the workspace ID arrives per workspace handle (`store.go:364`) or per append (CTO-5).
7. **The C2 callback spec is self-contradictory**: "a callback or a one-method interface (`func(scope string)`)" (line 262) vs. "calls `WakeAll` on the callback (the reason a callback needs both a scope and a 'wake all' entry point)" (line 266) (CTO-6).
8. **ADR 0037's deferral rationale is stale relative to the code** — it says the Waiter "cannot distinguish … both return nil" (`docs/adr/0037:316-317`), but the implemented port returns `(bool, error)` (`feed/ports.go:193`). ADR 0039 correctly describes the code; the planned 0037 note should also correct this, since it's why the deferral lifts cleanly (CTO-9).

---

## 2. CTO / Architecture

### 2.1 The core mechanism is correct

The "wake cannot lose an event" argument (§4) holds under code inspection: a wake only causes an additional `ReadAllBounded`, which is fence-bounded (`postgres/wsstore.go:786-791`); the cursor moves only on delivered pages (`feed/server.go:554`); a blocked fence stores `(boundL, fenceR)` and the next round resolves via one `fenceCleared` statement with no fresh xid (`wsstore.go:676-687`). The claim that the *same* xid a poll would have burned is merely re-timed at low append rates is consistent with `fenceBound`'s structure. The wake-confirmed floor (`server.go:631-648`) genuinely contains both wake storms (an empty post-wake read re-floors at `Poll` minus elapsed, so a `WakeAll` costs each stream ~2 immediate reads, not a spin) and the accepted read→register gap. The phasing A (seam+hub) → B (decorator) → C (write-path change, last, gated on measurements) is the right risk order, and Phase A alone already delivers the product outcome for in-process deployments.

No contradiction with ADR 0036 was found: C1's cross-process transport stays application-owned; the NATS snippet is gateway-side wiring, not es-lite code; the module stays broker-free (`go.mod`). ADR 0037's LISTEN/NOTIFY section explicitly reserved this decision as "a post-gate, separately-ADR'd decision" — this ADR is that decision, and the fence/checkpoint/lease semantics it must not touch are untouched.

### 2.2 Findings

**[CTO-2] High — the Hub invariants as specified leak map entries, failing the ADR's own §8 leak criterion.**
Evidence: ADR 0039 §3.2 invariants 1–2 (lines 150-160): deletion happens *only* in `WaitScope`'s deferred decrement, "when `refs` reaches zero and the map still holds that same generation"; `Wake` "closes the current channel and installs a fresh one."
Killer interleaving: waiter parks on scope A gen G1 → `Wake(A)` closes G1 and installs G2 (refs=0, no registrant) → waiter exits; decrement sees `map[A] == G2 ≠ G1` and does not delete → no deferred decrementer ever exists for G2. The entry persists and is merely replaced by later wakes — exactly the pattern "workspace with appends but no live subscriber," which invariant 2's own rationale names as the normal case (lines 156-158). The §8 criterion "10k scopes that come and go leave … no map entries" fails under this interleaving, and the §9 test list ("refcount cleanup at 10k scopes") would pass only if it never interleaves a wake between registration and exit — a test that would pass while the invariant still leaks.
Recommendation: change the invariant to **close-and-remove on `Wake`; `WaitScope`'s get-or-create lazily installs the fresh generation**. Waiters already hold the copied channel reference, so the close is still observed; the "map still holds that same generation" guard still protects against deleting a newer entry. This also collapses N rapid wakes into one close per park cycle (natural coalescing). Add the wake-after-last-waiter interleaving to the 10k-scope test.

**[CTO-1] High — §2's anti-goal contradicts the ADR's own C2 design.**
Evidence: §2 anti-goal (line 84): "A default-on append-time `NOTIFY`, or any new lock on the append path"; §3.3 C2 (lines 270-273) and §5 accept "the commit-time notify-queue lock that serialises committing notifiers."
The spec's §3 anti-goal (spec line 39: "no new locks taken by appends") is the rule C2 breaks when opted in, and ADR 0039 never states that amendment explicitly the way it amends ADR 0002.
Recommendation: reword the anti-goal to "no new lock on the **default** append path"; add an explicit amendment note (parallel to the ADR 0002 errata in §8) acknowledging that the opt-in C2 path deliberately takes the notify-queue lock at commit, and that this is the accepted, measured cost of opting in.

**[CTO-5] Medium — "Validate at Open" for the NOTIFY payload limit is not implementable as stated.**
Evidence: §3.3 C2 (line 281); `Open` is per-Store (`postgres/store.go:127-143`), while the payload (workspace ID) is only known per workspace handle (`store.go:364-366`) or per `AppendTx` call.
Recommendation: pick one and write it down: (a) always hash/encode the scope to a fixed-length token (recommended — constant length makes the 8000-byte limit unreachable and also serves the CISO mitigations), or (b) validate at `WorkspaceStore` creation. (a) is strictly better; it also removes a per-append length check.

**[CTO-6] Medium — the C2 callback interface is unspecified and self-contradictory.**
Evidence: §3.3 C2 lines 262-266 (quoted in §1.2 item 7).
Recommendation: specify concretely, e.g. `type NotifyHandler interface { OnNotify(scope string); OnReconnected() }`, and state that `postgres` must not import `feed` (the one-way coupling claim is good — keep it testable by a compile-time check).

**[CTO-4] Low — §1's motivation overstates the global-wake cost.**
Evidence: §1 lines 36-41 vs `postgres/wsstore.go:689-701` (the F9 idle short-circuit: `RawHead <= from` → return, no xid). A global wake costs unrelated scopes one RawHead-shaped transaction each (a real round-trip cost — the herd still stampedes the pool), but not "N fence draws." Same-scope streams do draw.
Recommendation: restate precisely; the scoped-wake case (§5) is already accurate, including that the non-matching projection's no-op read still burns a fence draw (traced: `RawHead > from` → `drawFence` → empty page).

**[CTO-7] Medium — no rate/amplification bound for C2 in v1, and the deferral of coalescing conflates two different requirements.** See CISO-1c; the cost answer to open question 2 is "defer," but the abuse answer is "required for C2." These must be separated in the ADR.

**[CTO-9] Low — ADR 0037's "both return nil" is stale.**
Evidence: `docs/adr/0037:316-317` vs `feed/ports.go:193`. The planned ADR 0037 note (§8, line 424) should correct it — the implemented `woke` signal is precisely why ADR 0039 can lift the deferral without the "page-confirmed floor" construction 0037 sketched.

**[CTO-10] Low — Hub godoc should pin the zero-value contract as a panic.** §3.2 invariant 7 says "programming error, not a silent no-op" without saying what that means concretely. Write "panics" and test it.

**[CTO-11] Info — `WakeAll` is O(live scopes) under the mutex and un-jittered.** Acceptable at the documented envelope (10k closes is microseconds, and the floor caps each stream at ~2 reads per `WakeAll`), but pin the listener's minimum reconnect backoff ≥ `Poll` in the runbook so a flapping listener cannot lockstep the fleet.

---

## 3. CPO / Product

### 3.1 Does it deliver the latency outcome?

Mechanically yes, on a clean fence: wake → `ReadAllBounded` → fence round (RawHead + sequence state + xid draw + horizon check) → page query → sink. That is ~5–7 statements ≈ low single-digit milliseconds on a local network, so "p50 < 10 ms on a clean fence" is plausible but must be measured (the ADR says so, §8). The honesty in §5 is correct and must survive into user-facing docs: **under contention, p99 remains ~Poll**, and only wakes emitted by the *committing* writer itself rescue the delay (C1 fires after `Append` returns; C2 fires at commit). For the stated gateway use case (two projections per workspace, quiet-to-moderate load), this is the right outcome: single-digit p50 where users feel it, unchanged p99 tail where the fence governs.

### 3.2 Findings

**[CPO-1] High — C1 does not cover the platform's integrated append path, and the miss is silent.**
Evidence: §3.3 C1 documents that `AppendTx` and `aggregate.WithAppend` are not hooked (lines 216-227) — but ADR 0035 exists because the platform appends inside `tenancy.TenantTx` (`docs/adr/0035:15-18`). Every event appended on that path gets no hint unless each call site wakes after its own commit; latency then silently regresses to polling for exactly the events the realtime gateway most likely cares about, with no signal distinguishing "wake missing" from "fence blocked" (`Observer.OnStall` covers only the latter, `feed/ports.go:227-236`).
Recommendation: state the product consequence in §3.3 in one sentence, and elevate "expose the hook at the adapter layer when the caller-owned API is needed" (line 226) from a parenthetical to a Phase B/C deliverable — a `postgres.Workspace`-level hook that fires after the caller's commit is the only way C1 covers the ADR 0035 shape without per-call-site discipline. At minimum, the runbook needs a "did my appends wake the gateway?" diagnostic.

**[CPO-2] Medium — silent partial adoption across a fleet.**
Cross-process C1 requires every appender replica to wire the decorator + publish. One forgotten service degrades latency invisibly; the poll safety net masks it. Recommendation: add cheap wake observability (Hub counters for `Wake`/`WakeAll`, or an `Observer` signal on wake-driven page reads) so an operator can answer "are wakes arriving for scope X?" Without it, support burden for the headline feature is guesswork.

**[CPO-3] Medium — the README/docs claim must carry both regimes.**
§8's README note must state the conditional explicitly: single-digit p50 on a clean fence; p99 ≈ `Poll` under a blocked fence; a lost wake costs at most one poll. The existing guarantees block's "Push. No LISTEN/NOTIFY ships" (README.md:204-206) must be updated, and the number reported should be the measured one, not the design target.

**[CPO-4] Low — the C1 example publish is not obviously non-blocking.**
`nc.Publish` (line 237) is fire-and-forget by intent, but a NATS client with a full pending buffer while disconnected can block; `fn`'s contract is "do not block" by documentation only (line 207). Show a bounded/async publish in the example and say which process runs which half (appender publishes, gateway subscribes) — the current snippet reads as one program.

**[CPO-5] Verdict on sequencing: A+B first is right, and does not delay value.** Phase A alone serves in-process and single-bus deployments; Phase B is small; C is the only high-risk surface and its beneficiary (zero-wiring coverage, including `AppendTx`, which C1 structurally misses — C2's notify lives in the shared append body and fires at the caller's commit) is real but not urgent. The one demanded change: §9's Phase C is written unconditionally; make the C2 gate explicit (see §7 and the Q4 answer).

---

## 4. CISO / Security

### 4.1 The C2 threat model, restated

The verified facts: LISTEN/NOTIFY has **no** row-level security ("Notifications are visible to all users" — PG docs); any session may `pg_notify` on any channel it can name (no privilege gates NOTIFY); the notification event carries channel, notifier PID, and payload (`pgconn/pgconn.go:42-46`). The existing trust model in the runbook is "workspaces that share one database login are mutually trusting" (`docs/deploy/README.md:263-271`); C2 widens the circle to **every role that can CONNECT to the database** — a strictly larger set than the documented boundary, and one the ADR's §6 does not confront head-on.

### 4.2 Findings

**[CISO-1] Blocker — the C2 security story (§6 + §3.3 C2) is unsound as written; four defects, one fix set.**

- **(a) The HMAC mitigation is incoherent.** §6 (lines 357-359) proposes "an HMAC of the scope under a key held only by the gateway." The payload is emitted by `pg_notify` inside the *appender's* append body (§3.3 C2; `postgres/wsstore.go:185` is where it would run) — so the appender holds the key, not the gateway, and "key held only by the gateway" is false by construction. Even done correctly (shared key appender→gateway), the residual leak remains: token *equality* still correlates workspaces across events, and per-token volume/timing still leaks activity rates.
- **(b) Trusted-listener must be a hard precondition, not one option among four.** As written, an operator reading §6 can reasonably conclude that "unguessable channel name + opaque payload" makes C2 safe in a co-tenant database. It does not (see (c), and (a)'s residuals). The rule must be: **enable C2 only when every role that can CONNECT to that database is trusted, or the database is dedicated** to the feed/append path. Otherwise use C1.
- **(c) Notification injection is unaddressed.** Any role with CONNECT can `pg_notify` on a known channel and spoof wakes. An unguessable high-entropy channel name mitigates both eavesdropping and injection (it is a capability), so it must be *mandatory*, not advisory. Impact of injection is real but bounded more narrowly than the ADR's "DoS" bullet suggests: on a caught-up scope, each fake wake costs the streams a RawHead-shaped short-circuit round (`wsstore.go:689-701`) — pool/CPU pressure, **no xid burn**; on a backlogged scope it costs real fence draws. Because v1 ships no per-scope bound and defers coalescing (§5, lines 337-338), the abuse path is unbounded. Fix: a listener-level per-scope throttle (e.g., drop notifies for a scope within N ms of the last wake — parked waiters already got the close) is a few lines and caps amplification without touching the hub's semantics or the latency claim.
- **(d) "The hint can never fail the append" (line 281) is false.** PG docs: a full notification queue makes "transactions calling NOTIFY … fail at commit." A wedged listener session (alive but not draining — SIGSTOP, GC pause, network black hole) pins queue cleanup ("no cleanup can take place if a session executes LISTEN and then enters a transaction for a very long time"), so under sustained appends the *appends* begin failing at commit — strictly worse than §2's anti-goal. Additionally undocumented: "A transaction that has executed NOTIFY cannot be prepared for two-phase commit" — a hard incompatibility for any ADR 0035 caller using prepared transactions.
- **Fix set (all ADR/runbook text plus one small mechanism):** make trusted-CONNECT the precondition and unguessable channels mandatory (b); rewrite the HMAC paragraph as optional defense-in-depth with correct key placement and stated residuals (a); require the listener throttle for C2 (c); replace the "can never fail" sentence with the honest failure domain — payload length (solved by hashing, CTO-5) and queue exhaustion — plus runbook alerting on `pg_notification_queue_usage()` and the documented 2PC exclusion (d).

**[CISO-2] High — `WithAfterAppend`'s panic behavior contradicts the ADR's own anti-goal.**
Evidence: §2 anti-goal (line 85) "A wake that can turn a committed append into an apparent failure"; §3.1 (lines 204-206) "a failed hint must never surface as a failed append (which would invite a retry and a duplicate)"; §6 (lines 372-375) concedes "A panic in `fn` propagates out of `Append` after the append has committed" and leaves it as a documented contract. A post-commit panic is an apparent failure that invites a retry and, without `WithDeterministicEventIDs`, a duplicate — the exact outcome the decorator's no-error design exists to prevent.
Recommendation: pick one explicitly. Either (preferred) `recover()` inside the decorator — it is the only way the decorator honors its own no-error design, and the loss is a hint (delay-only by §4) — or rescope the anti-goal to wake-*mechanism* failures and state loudly in the godoc that a panicking `fn` surfaces as a failed append after durability and may duplicate on retry. Do not leave the two sentences in conflict.

**[CISO-3] Medium — LISTEN channel identifier handling must be specified.**
`pg_notify($1, $2)` parameterizes the channel safely as a value, but the listener's `LISTEN` cannot take a bind parameter — a naive implementation interpolates the configured channel into the statement (injection foot-gun) and ignores NAMEDATALEN truncation. Specify: validate strict identifier syntax and length at `Open` and/or quote via `pgx.Identifier.Sanitize`, and document that the channel name is a security capability (CISO-1b/c) that must be high-entropy and never logged. Residual, worth one runbook line: roles holding `pg_read_all_stats` can read other sessions' query text and thereby learn the channel name.

**[CISO-4] Low — listener connection hygiene.** Dedicated connection (never pooled/returned — the ADR says this), set `application_name` for `pg_stat_activity` triage, jittered reconnect backoff with a floor ≥ `Poll` (see CTO-11). The `pgconn.Notification` PID field enables the standard self-notify filter if the listener process also appends; duplicates are harmless by design, so this is optional.

**[CISO-5] Verified clean — privileges and supply chain.** C2 needs no new grants (LISTEN requires none; the runbook's feed grants are unchanged, `docs/deploy/README.md:191-201`), writes no rows, no schema change, no bypass arm anywhere (the feed tables' `FORCE ROW LEVEL SECURITY` no-bypass posture is untouched). No new module dependencies (`go.mod`; pgx `WaitForNotification` verified present in the pinned v5.10.0). Workspace IDs as payloads are metadata the store already treats as non-secret (README.md:356-358) — the *new* exposure is cross-workspace correlation, correctly identified in §6.

---

## 5. Answers to the four open questions (ADR §10)

**Q1 — Is workspace scope sufficient, or is type-scoping needed?**
**Workspace scope is sufficient; do not add type scoping in v1.** The non-matching projection's no-op read is one fence round per append (bounded by scope, index-served, and re-timed rather than additive at low rates); type scoping would explode the wake key to (scope × type-set) semantics, complicate the payload/channel encoding, and buy nothing where it matters — at high append rates wakes are mostly unobserved anyway (streams are un-parked, §3.2's accepted gap). Record the per-scope read amplification in the Phase C benchmark (the F9 `draws` counter, `postgres/wsstore.go:103-109`, makes this measurable) and revisit only with the coalescing knob.

**Q2 — Per-scope coalescing window in v1?**
**As a cost knob: no — wait for measurements.** As specified, the wake-confirmed floor already re-imposes `Poll` after every empty post-wake read, so coalescing saves almost nothing in the cost model. **As an abuse bound for C2: yes, required** — but implement it at the listener (per-scope throttle, CISO-1c), not in the hub, so the hint semantics and the A/B seam stay unchanged. The ADR currently merges these two questions into one deferral; split them.

**Q3 — Is the C2 metadata exposure acceptable with the trusted-listener rule, or must opaque-payload be the default?**
**Neither option as stated.** The trusted-listener rule must be the *hard precondition* — C2 is enabled only when every CONNECT-capable role on that database is trusted or the database is dedicated; co-tenant databases with untrusted roles must not enable C2 at all (use C1). HMAC-opaque payloads cannot be the default "fix": the key-placement premise in §6 is wrong (CISO-1a), and the residual correlation/volume leak remains. Keep HMAC as optional defense-in-depth, correctly described, for deployments that trust the roles but want to reduce payload legibility in incidental logging. This is the blocker's (a)+(b) resolution.

**Q4 — C2 in the same release as A/B, or after C1 is measured in production?**
**A/B in the first release; C2 gated.** A/B are small, carry zero default-build risk, and deliver the outcome for in-process and own-bus deployments. C2 should ship only after (i) C1's latency win is measured on a clean fence in production, and (ii) the CISO-1 security rewrite (trust precondition, mandatory channel entropy, listener throttle, honest failure-domain, `pg_notification_queue_usage` alerting, 2PC exclusion) lands in the ADR and runbook. The ADR's §9 Phase C is currently unconditional; mark it conditional on that gate. Note C2's genuine product advantage — it is the only option that covers `AppendTx`/caller-owned commits with zero per-call-site wiring — which is precisely why it's worth gating properly rather than shipping half-specified.

---

## 6. Missing tests and acceptance criteria

Additions required beyond §8/§9 (existing criteria — default-build byte-identity, scoping, liveness, 10k-scope `-race`, both-regime latency, append benchmarks, C2 correctness — are adequate and verified as testable against the current harness):

1. **Hub stale-entry interleaving** (pins CTO-2's fix): wake a scope while its last waiter is parked, let the waiter exit, assert no map entry remains; include wakes in the 10k-scope test. Without this the specified invariant fails its own acceptance criterion.
2. **`Wake` on a never-registered scope leaves the map empty** — invariant 2 (lines 155-158) has no test in the §9 list.
3. **Hub zero-value panic contract** (invariant 7) — pin whatever "programming error" is decided to mean.
4. **C2 empty-batch boundary:** a claim-only append (zero events — reachable per `runtime.go:371-374` and `wsstore.go:346`'s precedent) emits no notify; the "once per non-empty batch" claim (line 253) needs the zero-side test alongside the recording-`DBTX` no-notify test.
5. **Listener flap inside the chaos gate:** §8's "killed listener reconnects and WakeAll reaches waiters" is a single kill; the feedgate chaos matrix (`postgres/feedgate_test.go:674`) should inject listener kill/restart mid-chaos and re-assert set equality.
6. **WithAfterAppend panic contract test** (pins CISO-2's decision): whichever behavior is chosen (recover or propagate) is pinned by a test, because the choice is load-bearing for the duplicate-on-retry story.
7. **`WithAfterAppend` + `WithAppend` interplay:** a runtime constructed over a wrapped store with a `WithAppend` override must document (test-assert) that the hook does *not* fire — the ADR's documented boundary (lines 219-223).
8. **Fence-draw accounting in the latency benchmarks:** report the `draws` counter delta (wake on vs off) so wake-driven xid burn is a measured number, not a paragraph — the counter already exists (`wsstore.go:103-109`).
9. **ScopedWaiter compat path:** once `ScopedWaiter` exists, pin that a waiter implementing only `Wait` still routes through `Wait` (the decorator-silent-downgrade rule, lines 115-119, is otherwise unverifiable).

---

## 7. Verdict and minimal changes required to approve

### Verdict: **Approve with changes**

The decision itself — scoped wakes over the existing wake-confirmed `Waiter` seam, an in-process hub, and an opt-in-only append-path hook — is the simplest correct design available inside the ADR 0036 constraint set. The lossless argument (§4) was verified against the fence code and holds. Phases A and B are approvable for implementation immediately after the ADR text is corrected. Phase C (C2) is **not** approvable as written.

### Minimal set of changes required to approve

1. **(Blocker, CISO-1) Rewrite the C2 security and failure model:** trusted-CONNECT as a hard precondition for enabling C2; mandatory high-entropy channel names; correct the HMAC key-placement description and demote it to optional defense-in-depth with residuals stated; add a listener-level per-scope throttle as a C2 requirement; replace "the hint can never fail the append" with the honest failure domain (queue exhaustion fails the append's commit; payload limit solved by hashing), plus runbook alerting on `pg_notification_queue_usage()` and the documented two-phase-commit exclusion.
2. **(High, CTO-2) Fix the Hub's generation lifecycle:** close-and-remove on `Wake`, get-or-create on `WaitScope`, plus the stale-entry interleaving test. As specified, the invariants fail the ADR's own §8 leak criterion.
3. **(High, CTO-1 / CPO-1) Two wording/elevation fixes:** rescope the §2 anti-goal to the *default* append path and add the explicit spec-§3 amendment note; state C1's `AppendTx`/`WithAppend` coverage gap as a product consequence and elevate the adapter-layer hook for the caller-owned path from a parenthetical to a planned deliverable.
4. **(High, CISO-2) Decide and document the `WithAfterAppend` panic contract** — recover in the decorator, or explicitly rescope the anti-goal and state the retry-duplicate risk. The current text contradicts itself.
5. **(Medium, CTO-5/6, CISO-3) Resolve the builder-facing ambiguities before Phase C:** payload validation placement (recommend: always hash the scope — fixed length, unreachable limit, serves the channel/payload capability story); the two-signal `NotifyHandler` interface shape; LISTEN identifier quoting/length validation at `Open`.
6. **(Low, CTO-4/9) Accuracy corrections:** restate §1's "N fence draws" to the post-errata cost shape; use the planned ADR 0037 note to correct its stale "both return nil" description of the `Waiter` contract.

Items 1–4 gate the ADR's acceptance; item 5 gates the start of Phase C; item 6 should ride along in the same edit. Everything else in this report is advisory.
