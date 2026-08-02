# ADR 0009: NATS Eventstore Service with a Zero-Knowledge Client

- **Status:** Accepted (design; service + client deferred)
- **Date:** 2026-08-02

## Context

es-lite is a library embedded per service (ADR 0002). We also want to run
it as an **independent NATS service** so that client services need only
NATS — no database drivers, no KMS credentials, no version-locked library
— and non-Go clients (e.g. a TypeScript SDK) can participate.

The chosen shape is a **generic storage service with client-side crypto**:
the client holds the codec *and* the encryption, so the server stores only
opaque ciphertext plus metadata. This makes the server **zero-knowledge**
about payload content. The alternative — a command gateway that decrypts
and runs deciders server-side — was considered and is recorded under
*Alternatives*; it trades the zero-knowledge property for fewer network
hops.

The main cost of the storage-service shape is network round-trips
(folding requires reading the stream back to the client to decrypt), and
this ADR addresses it with a client-side snapshot cache in NATS KV.

## Decision

### `es.Store` is the wire protocol

The server wraps a real `es.Store` (Postgres, ADR 0004) and exposes its
~five methods over NATS request/reply. The client is an `es.Store`
implementation whose methods are NATS requests. The existing
`aggregate.Runtime`, `Decider`, upcasters, and codec run **unchanged** on
the client — you construct `aggregate.NewRuntime(natsStore, decider,
codec)`. The interface es-lite already has *is* the protocol; no new core
concepts.

The server is generic: because it only moves opaque envelopes, one server
serves every aggregate.

### The server is zero-knowledge; encryption is a client-side Store decorator

The client holds the workspace key material (via OpenBao, ADR 0006) and
does all crypto. Encryption is expressed as an `es.Store` that wraps
another `es.Store`:

```
runtime → encrypt(shredder) → natsClient(es.Store) → [NATS] → server → postgres(es.Store)
             │
   encrypts EventData.Payload on Append, decrypts Envelope.Payload on read,
   computes the HMAC value_key for PII constraints before forwarding
```

The decorator composes over any inner store, so the same code encrypts
whether the backend is the NATS client or a local Postgres. The server
persists ciphertext and never sees plaintext.

**Keys move to the clients.** This is the deliberate inverse of a gateway:
we trade centralized key custody for a server that cannot read payloads.
Behind a trusted NATS mesh this is a stronger confidentiality posture; it
would be the wrong trade if clients were many and untrusted.

### Uniqueness survives client-side encryption

Because ADR 0008 uniqueness is enforced on an **HMAC of the value**, not
on ciphertext, the client computes the `value_key` (plaintext bytes for
non-PII, `shred.MAC` for PII) and sends it in the constraint op; the
server enforces the `UNIQUE` index on opaque bytes without knowing the
plaintext. The design choice made in ADR 0008 pays off precisely here.

### Workspace comes from NATS auth, not the client

Even though payloads are opaque, the server scopes every operation to the
**authenticated** identity's workspace (RLS, ADR 0004) — never a
client-supplied field. Ciphertext is useless without keys, but it is not
handed across workspaces. The server still sees metadata (stream ids,
workspace, actor, correlation, **type URLs**, timestamps, claim *scopes*):
zero-knowledge on payload, not on shape. The client↔server hop therefore
runs over **mTLS** to protect that metadata in transit.

### The embeddable library stays first-class

`es.Store` being the seam means each service chooses its backend without
changing decider/runtime code: latency-critical / same-team services
**embed** (direct Postgres, zero hops); peripheral, cross-language, or
loosely-coupled services use the **NATS client**. A service can move
between the two as a one-line wiring change. Not everything routes through
NATS.

### Network hops, and the NATS-KV snapshot cache that mitigates them

A command folds state (a read) then appends — ~**2 NATS round-trips**,
more under contention, because with client-side crypto the ciphertext must
return to the client to be decrypted and folded. To bring the hot path
back toward one hop, the client keeps a **snapshot cache in NATS KV**:

- The client reads `snapshot@vN` from KV, then reads only the tail
  (`version > N`) from the eventstore and folds on top. The snapshot is a
  **pure cache**: correctness always comes from the log, so KV may be
  stale, absent, or wrong — the client just folds a longer tail.
- **Snapshots are client-produced.** The zero-knowledge server cannot fold
  (it cannot decrypt), so a crypto-capable client — an async snapshotter,
  or write-through after a fold — computes them. This reshapes ADR 0007
  (which assumed a server-side folder + table) for the service model.
- **The snapshot blob is client-encrypted** under the workspace DEK before
  it is written to KV. Otherwise KV would hold folded plaintext PII and
  break zero-knowledge. KV holds ciphertext like everything else.
- **Invalidation** (ADR 0007's rules, applied to a shared cache): store a
  fold-version / `state_schema_version` with the blob and refold on
  mismatch (guards decider and upcaster changes); a shredded workspace's
  blob is inert (undecryptable) and may be purged; never seed a
  time-travel / as-of read from a snapshot.
- **TTL eviction.** Give the snapshot KV bucket a TTL (`MaxAge`, or
  per-key TTL on newer NATS). Because the snapshot is a pure cache, TTL
  expiry is always safe — the client just folds a longer tail. TTL bounds
  staleness, auto-evicts without a sweeper, and ages out post-shred inert
  blobs on its own. It complements, not replaces, the fold-version guard
  (correctness) and explicit purge-on-shred (immediate erasure).
- **Size:** NATS KV values cap ~1 MB by default; oversized states use the
  Object Store or are kept small.

Building this cache is justified now — unlike in the library model where
folding is microseconds and ADR 0007 deferred it — because the service
architecture makes the fold latency real and immediate. Sequencing: build
the service + client + encrypt decorator first (prove the round-trip),
then layer the KV snapshot cache.

### Subjects and protocol

Following the `nats-contracts` taxonomy: request/reply on
`svc.eslite.<method>` (e.g. `append`, `read-stream`, `read-all`) bound to
a **queue group** so stateless server replicas load-balance over one
Postgres (ADR 0002). Requests and replies are proto messages; store errors
map to typed reply codes so clients can react —
`ErrConflict`, `ErrConstraintViolated`, `ErrTerminal`,
`keystore.ErrShredded`. The existing JetStream `evt.<workspace>.…` stream
(ADR 0003) is unchanged and now carries ciphertext end to end, which only
crypto-capable projections decrypt.

### A dedicated NATS account

es-lite runs in its **own NATS account** (`ESLITE`). Accounts are NATS's
isolation boundary, so the eventstore's subjects, JetStream streams, and
KV buckets are walled off from the rest of the mesh, with independent
quotas and connection limits. Client accounts reach it through **service
and stream exports/imports**: the `ESLITE` account exports the
`svc.eslite.*` request/reply service and the `evt.*` projection stream;
client accounts import them.

This is also how workspace-from-auth is enforced structurally: an imported
service call carries the caller's user JWT, and es-lite maps the
authenticated user to a workspace — the client cannot forge it.

**Account is not workspace.** At 100k workspaces you do not mint 100k
accounts (accounts are heavyweight). There are a handful of client
accounts (per service/team); `workspace` is a claim *within* the caller's
user JWT that es-lite maps to the RLS scope (ADR 0004). Accounts isolate
the *service*; the JWT claim scopes the *tenant*.

## Consequences

### Positive

- **Zero-knowledge server.** Compromising the eventstore leaks ciphertext
  and metadata, never payload plaintext. Erasure is "destroy the key";
  server rows are already opaque.
- **Thin, language-agnostic clients** that need only NATS + proto
  contracts; the eventstore deploys and scales independently.
- **Central enforcement** of ordering, optimistic concurrency, uniqueness,
  and workspace isolation, on data the server cannot read.
- **No new core concepts** — `es.Store` is the protocol; encryption and
  the KV cache are composable layers; the library stays first-class.

### Negative

- **More network hops.** ~2 round-trips per command and retries across the
  network; the KV snapshot cache mitigates the read hop but does not
  eliminate it. Hot, latency-critical paths should embed the library
  instead.
- **Keys distributed to clients.** Every crypto-capable client needs
  OpenBao access — a wider key-access surface than a gateway. Acceptable
  for trusted mesh services, not for untrusted clients.
- **Metadata is visible** to the server and must be protected in transit
  (mTLS); type URLs and actors are not hidden.
- **Another cache surface.** The KV snapshot adds a shared, encrypted,
  fold-versioned cache with its own invalidation discipline.
- **A central dependency**, though horizontally scalable and stateless
  over shared Postgres.

## Alternatives Considered

### Command gateway (server decides, server decrypts)

One deployable embeds the deciders, decrypts to fold, and runs
read-decide-append server-side — **one** network hop per command, retries
staying next to the DB. Rejected as the primary design because it forfeits
the zero-knowledge property and puts keys and plaintext on the server. It
remains the right choice for a deployment that prioritizes lowest latency
and can trust the server with plaintext; recorded here as the explicit
lower-latency alternative.

### Library only (no service)

Fastest (no hops) and simplest, but requires every client to embed Go, a
database driver, and KMS credentials — which is exactly what this ADR
removes for thin/cross-language clients. The library remains first-class
*alongside* the service, not instead of it.

### Server-side snapshots (ADR 0007 as written)

Not applicable to the zero-knowledge service: the server cannot fold, so
it cannot populate a server-side snapshot table. Snapshots move
client-side into encrypted NATS KV (above). ADR 0007's deferral still
governs the library/embedded model.
