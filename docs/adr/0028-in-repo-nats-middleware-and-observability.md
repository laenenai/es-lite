# ADR 0028: In-Repo NATS Middleware and Observability

- **Status:** Accepted
- **Date:** 2026-10-28

## Context

The module previously depended on the private `github.com/laenenai/natskit`
repository for NATS identity middleware and `natskit/obs` for structured
logging, OpenTelemetry wiring and NATS connection helpers. Commit `e1114ce`
removed those private dependencies so the public module builds without
credentials.

## Decision

The required behavior lives in this repository:

- `natsstore` implements the identity/tenant middleware, the
  subject/workspace cross-check, the NATS Micro request pipeline and the
  request/reply wire contract.
- `cmd/es-lited/obs.go` and `cmd/es-relayd/obs.go` implement structured
  logging, optional OTLP metrics/traces, NATS connection/reconnect handling and
  the server request middleware.

There is no private module dependency and no `GOPRIVATE`/token requirement.

## Consequences

- The public build and container are self-contained.
- The observability/NATS helper code is duplicated between the two commands
  until it is extracted into a shared internal package.
- TLS environment parity (`NATS_CA`/client certs) that natskit provided is not
  implemented here yet; see the security review.
- The `natsstore` JSON wire contract is local and unversioned; proto/versioning
  remains a follow-up decision.

## Alternatives considered

- **Keep the private natskit dependency.** Rejected: it blocks external builds
  and requires a PAT in CI/release.
- **Publish natskit as a separate public module.** Possible later, but it
  increases the dependency surface for a small amount of code.
