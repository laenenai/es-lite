# ADR 0006: OpenBao KeyStore Adapter

- **Status:** Accepted (implemented; validated against OpenBao 2.6.1)
- **Date:** 2026-08-01

## Context

ADR 0004 makes per-workspace crypto-shredding the default erasure
mechanism and defines a `keystore.KeyStore` abstraction for the key
material. This ADR records the production implementation: OpenBao (the
open-source Vault fork) via its **Transit** secrets engine.

Transit is encryption-as-a-service — named keys whose material never
leaves the server, with `encrypt`/`decrypt`/`datakey` operations and
irreversible key deletion. That deletion is exactly the crypto-shred
primitive es-lite needs.

## Decision

### Envelope encryption with Transit as the KEK

Per-event round-trips to OpenBao would put a network hop on the hot path.
Instead the adapter uses envelope encryption:

- **Per-workspace Transit key = the KEK.** `POST transit/keys/<ws>` with
  deletion enabled. Its material never leaves OpenBao.
- **DEK via datakey.** `GenerateDEK` calls
  `transit/datakey/plaintext/<ws>`, which returns a plaintext DEK and its
  wrapped (Transit-ciphertext) form. The plaintext DEK encrypts payloads
  locally (AES-256-GCM, in `shred`); only the wrapped form is persisted
  (`workspace_keys`).
- **Unwrap on cache miss.** `UnwrapDEK` calls `transit/decrypt/<ws>`. The
  `shred.Shredder` caches plaintext DEKs, so steady state does zero
  OpenBao calls.
- **Shred by key deletion.** `ForgetWorkspace` issues
  `DELETE transit/keys/<ws>`. Afterwards no wrapped DEK for the workspace
  can be unwrapped — the payloads are unrecoverable, including in DB
  backups. A subsequent `decrypt` returns an error the adapter maps to
  `keystore.ErrShredded`.

### The per-workspace Transit key is the erasure boundary

A single shared KEK is rejected: you cannot delete it to erase one
workspace. Merely deleting the wrapped-DEK *row* from the database is also
insufficient — backups would still hold it. Only destroying a
**per-workspace** Transit key erases the workspace everywhere, so each
workspace gets its own key.

### HTTP, not the Vault SDK

The adapter speaks the Transit HTTP API directly with `net/http` + JSON.
The Transit surface it needs is four endpoints (`keys`, `keys/.../config`,
`datakey`, `decrypt`, plus key delete); pulling the Vault Go SDK for that
would add a large dependency tree to an intentionally lite module. Token
goes in the `X-Vault-Token` header — OpenBao and Vault are wire-
compatible here.

### Authentication

In production the service authenticates via OpenBao's **Kubernetes auth**:
the pod's service-account JWT is exchanged for a short-lived token, and a
policy scopes it to `transit/.../<prefix>*`. The static-token `Config`
field used in dev/tests is the injection point for whatever token the
auth flow yields; token acquisition/renewal is deliberately left to the
caller (or a sidecar) rather than baked into the adapter.

## Consequences

### Positive

- **Crypto-shredding is one API call**, verified: generate → unwrap →
  delete key → unwrap now fails with `ErrShredded` (integration test
  against OpenBao 2.6.1 in `keystore/openbao`).
- **Hot path stays local.** Envelope encryption + DEK cache means Transit
  is hit only on cache miss and on shred, not per event.
- **No heavy dependency.** `net/http` keeps the module lean and the
  adapter easy to audit.
- **Vault-compatible.** The same adapter works against HashiCorp Vault's
  Transit engine unchanged.

### Negative

- **100k+ Transit keys.** One key per workspace means a large key count
  in the mount. Each key is small and es-lite never `LIST`s them, but the
  count should be load-tested; if a single mount strains, shard across
  mounts by `hash(workspace_id)` — shredding still deletes one key.
- **OpenBao is on the read/rebuild path.** Decrypt needs the KEK. The DEK
  cache absorbs steady state, but a cold cache during an OpenBao outage
  fails decrypt for un-cached workspaces. Warm the cache and alarm on
  OpenBao availability. Losing a KEK unintentionally is indistinguishable
  from erasure — key durability/backup is operationally critical.
- **Error-mapping is heuristic.** "Key missing" is inferred from OpenBao's
  error text/status to produce `ErrShredded`. Transit does not give a
  dedicated code, so the adapter matches known messages; new OpenBao
  versions should be checked against the mapping.

## Alternatives Considered

### Direct Transit encrypt/decrypt per event (no local DEK)

Simplest, but a network round-trip per event — untenable on the write and
rebuild paths at scale. Envelope encryption with a cached DEK is the
standard answer and what Transit's `datakey` endpoint exists for.

### Vault Go SDK

Rejected for dependency weight. The adapter needs a handful of endpoints;
the SDK brings a large transitive tree into a module whose whole premise
is smallness.

### Shared KEK with per-workspace DEKs deleted from the DB

Rejected: erasure would depend on the wrapped-DEK row being absent from
all backups, which is not a guarantee. A per-workspace KEK makes erasure
provable and backup-independent.
