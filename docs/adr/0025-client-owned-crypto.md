# ADR 0025: Client-Owned Cryptography and Erasure

- **Status:** Accepted (supersedes the crypto/erasure portions of ADRs 0004
  and 0006; amends ADR 0008's PII handling)
- **Date:** 2026-10-28

## Context

ADRs 0004 and 0006 designed per-workspace envelope encryption, an OpenBao
Transit keystore and `ForgetWorkspace` crypto-shredding. That implementation
was removed in commit `d451cf4` ("remove all crypto — es-lite is
zero-knowledge"). The code now stores payloads as opaque bytes and performs no
encryption, decryption or key management, but the README, ADRs and the
`PII` flag still promised server-side erasure and HMAC'd uniqueness.

Two positions are defensible: a managed service that owns keys and offers
erasure, or a keyless library that leaves cryptography to its callers. The
project chose the latter. This ADR records that decision and the obligations it
creates.

## Decision

1. **The server is keyless.** `es-lite`, `es-lited` and `es-relayd` persist,
   order and move only the bytes a caller supplies. They do not encrypt,
   decrypt, hold keys, derive keys, hash PII or destroy keys.
2. **Crypto and erasure are client-owned.** Callers that need confidentiality
   seal payloads before `Append` and open them after read. Callers that need
   crypto-shredding keep key material outside es-lite and destroy it
   themselves. es-lite provides no `ForgetWorkspace`.
3. **Uniqueness values are opaque.** `ConstraintOp` has no `PII` flag. For
   personal data the caller supplies a keyed hash/MAC it controls; the store
   stores and compares exactly those bytes. `LookupClaim` takes the same value
   as `Claim`.
4. **Snapshots are opaque caches.** `snapshot.Cache` implementations store the
   state bytes they are given. A caller that puts PII in snapshots must seal
   them or keep them out of shared storage; a caller that shreds a key must
   purge the corresponding snapshots and read models.
5. **The public documentation must not claim more.** Zero-knowledge is
   conditional on the client actually encrypting. Erasure claims must include
   JetStream, snapshots, unique claims, read models, indexes and backups.

## Consequences

- The server is simpler and has no key-custody or key-availability failure
  mode. A server compromise exposes ciphertext only when clients encrypted.
- es-lite cannot promise GDPR erasure by itself. The erasure story becomes an
  integration responsibility and must be documented and tested by adopters.
- Existing plaintext payloads and claim values remain plaintext. Removing the
  `PII` flag is a Go API break, but not a persisted-format change; an adopter
  that wants keyed claims must rewrite them out of band.
- The project may ship an optional client-side sealing/shredding helper, but
  the helper is not part of the server's trust boundary.

## Alternatives considered

- **Restore server-side envelope encryption + OpenBao keystore.** Rejected for
  now: it reverses the zero-knowledge design, adds key custody, backup and
  availability concerns, and requires migrating existing plaintext data.
- **Remove cryptography from the product story entirely.** Rejected: the
  keyless storage model is useful, and "clients own crypto" is a coherent
  position when stated honestly.

## Migration

No event-log, snapshot or unique-claim row changes format. The Go API change
(`Claim`/`Release` lose the `pii` argument, `LookupClaim` loses its `pii`
argument) is intentional and cheap. If a deployment previously relied on the
old HMAC implementation, it must re-index claims with its own keyed hash before
relying on cross-version uniqueness.
