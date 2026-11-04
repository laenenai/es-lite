# ADR 0036: No Broker in es-lite; Postgres and SQLite Only

- **Status:** Proposed
- **Date:** 2026-11-04
- **Supersedes:** ADR 0003, ADR 0009, ADR 0028
- **Related:** ADR 0001 (log as source of truth), ADR 0002 (delivery topology), ADR 0005 (projection rebuild), ADR 0007 (snapshots)

## Context

es-lite grew a second runtime beside the library: `es-lited` served `es.Store`
over NATS request/reply, `es-relayd` drained Postgres into JetStream, and the
`natsjs` adapter carried events to projections. Snapshots had a NATS KV
implementation (`snapshot/natskv`), and `leader` used a NATS KV lease for
singleton relay election. Those adapters were recorded in ADR 0003 (JetStream
delivery), ADR 0009 (the NATS eventstore service) and ADR 0028 (in-repo NATS
middleware and observability).

The platform this module belongs to has since adopted a transport-neutral event
feed. Projections consume through the `feed` package (checkpoint, lease and log
ports with a `Subscribe` loop) or the in-process `projection.LogFeed`;
deployments provide the Postgres-backed `Log`, `CheckpointStore` and
`LeaseStore`. Durable projection position is already the log's
`global_position`, and snapshots can be stored in the same database as the
events. Nothing in the supported deployment path needs a message broker.

Keeping the NATS code in the module therefore imposed an unused runtime
dependency, a second operational surface (subjects, accounts, streams, KV
buckets, daemons, manifests) and a second set of delivery semantics to
document and test. The core store, aggregate runtime, migrations, hash chain,
projections and feed are all broker-independent.

## Decision

es-lite ships no broker integration and no daemon.

- **Delivery is the log plus `feed`.** `delivery.Poller` and `delivery.Relay`
  remain transport-neutral: they tail the append-only log and hand batches to
  a caller-supplied handler. Projections consume through `feed.Server` (the
  platform's service) or `projection.LogFeed` (in-process). There is no
  JetStream publisher, consumer, stream provisioning or gap detector in this
  module.
- **Snapshots use the `snapshot.Cache` interface.** The interface stays; the
  NATS KV implementation is removed. The platform implements the cache over
  its database (for example the same Postgres store that holds events and
  checkpoints). The cache remains a re-derivable optimization, not a source of
  truth.
- **Leader election, where a workload needs a singleton, uses Postgres
  leases.** The feed already fences subscribers with a `LeaseStore`; deployments
  that need a singleton poller or relay use the same database-backed lease
  mechanism. There is no KV bucket election in the module.
- **The NATS adapters and daemons are removed.** `natsjs`, `natsstore`,
  `snapshot/natskv`, `leader`, `cmd/es-lited`, `cmd/es-relayd`, the JetStream
  example packages and the es-lited/es-relayd manifests are deleted. The
  module no longer depends on `github.com/nats-io/nats.go` or its transitive
  `nkeys`/`nuid` packages.

A runnable eventstore service is the platform's `serve` (or an application
embedding this library). es-lite itself is a library with the Postgres and
SQLite backends.

## Alternatives considered

- **Keep the NATS adapters as optional packages.** Rejected: Go modules do not
  have optional dependencies, so every consumer would keep fetching and
  auditing the NATS module for code the platform does not run. It would also
  keep two delivery paths (JetStream and `feed`) that must not drift.
- **Extract NATS support to a submodule or a separate repository.** Rejected:
  it preserves a second runtime to build, version, secure and document while
  no current consumer needs it. If a broker adapter is ever required again, it
  can be reintroduced as a separate module against the stable `es.Store`,
  `feed` and `snapshot.Cache` interfaces.
- **Keep `es-lited` as a standalone service without NATS.** Rejected: that is
  a new transport and service design, not a removal. The platform service
  already owns the transport, authentication and authorization boundary.

## Consequences

- **Breaking for importers of the deleted packages.** `natsjs`, `natsstore`,
  `snapshot/natskv`, `leader`, `cmd/es-lited` and `cmd/es-relayd` no longer
  exist. Code importing them must move to `feed`/`projection.LogFeed`, a
  database-backed snapshot cache, database leases, or the platform service.
- **The module is smaller and broker-free.** `go mod tidy` drops
  `github.com/nats-io/nats.go`, `nkeys` and `nuid`; builds and tests need no
  NATS server or credentials.
- **Consumers that need a service use the platform.** es-lite no longer ships
  an image or a daemon. Deployment, workload identity, authorization and
  multi-workspace routing are the platform's concern; the library exposes the
  store, runtime, feed core and snapshot interface those services build on.
- **Historical ADRs stay readable.** ADR 0003, 0009 and 0028 are marked
  superseded rather than rewritten, so the reasoning that led to the broker
  adapters remains available.
