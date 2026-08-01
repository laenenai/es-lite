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
| 0004 | [Workspaces and Tenant Isolation](./0004-workspaces-and-tenant-isolation.md)               | Accepted (amends 0001) |
| 0005 | [Projection Rebuild](./0005-projection-rebuild.md)                                         | Accepted (design; primitive deferred) |
| 0006 | [OpenBao KeyStore Adapter](./0006-openbao-keystore.md)                                     | Accepted (implemented) |
| 0007 | [Snapshots (Deferred)](./0007-snapshots-deferred.md)                                       | Deferred |

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
projections, `state_cache`, crypto-shredding, Cedar authz, mandatory
multi-tenancy, the tamper-evident hash chain). ADR 0001 lists the cuts
and the reasoning. When a cut later proves necessary, the parent's ADRs
are the reference for how to add it back.
