# Architecture Decision Records

This directory records the load-bearing architectural decisions for
**es-lite** — a deliberately small event-sourcing library, extracted
from the design space explored by the sibling `eventstore` framework
but stripped to the core that most applications actually need.

Each ADR captures one decision: the context, the alternatives
considered, and the consequences accepted. ADRs are immutable — if a
decision changes, write a new ADR that supersedes the old one rather
than editing history.

## Index

| #    | Title                                                                                     | Status   |
| ---- | ----------------------------------------------------------------------------------------- | -------- |
| 0001 | [Event-Sourced Log as Source of Truth](./0001-event-sourced-lite-architecture.md)         | Accepted |
| 0002 | [Deployment Topology and Delivery](./0002-deployment-topology-and-delivery.md)             | Accepted |
| 0003 | [NATS/JetStream Delivery and Projections](./0003-nats-jetstream-delivery.md)               | Accepted (implemented) |
| 0004 | [Workspaces and Tenant Isolation](./0004-workspaces-and-tenant-isolation.md)               | Accepted for isolation; crypto/erasure superseded by 0025 |
| 0005 | [Projection Rebuild](./0005-projection-rebuild.md)                                         | Accepted (implemented) |
| 0006 | [OpenBao KeyStore Adapter](./0006-openbao-keystore.md)                                     | Superseded by 0025 (server-side keystore removed) |
| 0007 | [Snapshots](./0007-snapshots-deferred.md)                                                   | Implemented (cache; sealing is client-owned per 0025) |
| 0008 | [Uniqueness Constraints](./0008-uniqueness-constraints.md)                                 | Accepted; PII handling amended by 0025 |
| 0009 | [NATS Eventstore Service (zero-knowledge client)](./0009-nats-eventstore-service.md)       | Accepted (implemented) |
| 0010 | [Schema Migrations](./0010-schema-migrations.md)                                           | Accepted |
| 0025 | [Client-Owned Cryptography and Erasure](./0025-client-owned-crypto.md)                     | Accepted (supersedes 0004/0006 crypto; amends 0008) |
| 0028 | [In-Repo NATS Middleware and Observability](./0028-in-repo-nats-middleware-and-observability.md) | Accepted |
| 0029 | [Tamper-Evident Event Log (Per-Stream Hash Chain)](./0029-tamper-evident-hash-chain.md) | Accepted (implemented; reverses the 0001 cut) |
| 0030 | [Projector and Reaction Codegen from ProjectionSpec / ReactionSpec](./0030-projection-and-reaction-codegen.md) | Proposed |
| 0031 | [Connect Command Service Codegen](./0031-connect-command-service-codegen.md)                | Proposed |
| 0032 | [Bounded Conflict Retry and `OccurredAt` from `decided_at`](./0032-conflict-retry-and-decided-at.md) | Accepted (implemented) |
| 0033 | [Publish `es/v1/options.proto` as a Buf Module](./0033-publish-options-to-bsr.md)          | Proposed |

ADRs 0029–0033 are the es-lite side of the `contracts` integration; the
ordered implementation plan is in
[`docs/proposals/2026-10-30-contracts-integration-plan.md`](../proposals/2026-10-30-contracts-integration-plan.md).

## Conventions

- **Status:** Proposed, Accepted, Deferred, Superseded by ADR-XXXX, Deprecated.
- **Format:** loosely MADR-style — context, decision, consequences,
  alternatives. Keep each ADR to one decision.
- **Numbering:** sequential, zero-padded to four digits.
- **Tone:** prose, not bullets-only. A future maintainer should be able
  to reconstruct the reasoning without needing to ask anyone.

## Relationship to `eventstore`

`es-lite` is not a fork. It is a separate module that reuses the
*ideas* the parent proved out — proto-defined aggregates, the Decider
model, a storage-neutral `Store` — while deliberately **omitting** the
parent's heavier machinery (durable command-bus workflows, synchronous
projections, `state_cache`, server-side crypto-shredding, Cedar authz,
mandatory multi-tenancy). ADR 0001 lists the cuts and the reasoning.
Cryptography and erasure are client-owned (ADR 0025). The tamper-evident
hash chain, cut in 0001, was added back by ADR 0029 in a keyless form.
When a cut later proves necessary, the parent's ADRs are the reference for
how to add it back.
