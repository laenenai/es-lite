# Deployment

es-lite ships **no daemon and no image**. It is a Go library with Postgres and
SQLite backends, an in-process delivery path (`delivery`) and a
transport-neutral feed core (`feed`). A runnable eventstore service is the
platform's `serve`; deployment, workload identity, authorization and
multi-workspace routing are the platform's concern (ADR 0036).

For the decisions behind the library boundary, see
[`docs/adr`](../adr).
