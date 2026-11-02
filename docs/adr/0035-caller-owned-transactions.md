# ADR 0035: Caller-Owned Transactions for Atomic Appends

- **Status:** Accepted (implemented)
- **Date:** 2026-11-02
- **Related:** ADR 0004 (workspace RLS), ADR 0008 (uniqueness), issue #13

## Context

`tenancy` (`github.com/laenenai/tenancy`) hands a data-plane caller a
tenant-scoped `TenantTx`, not a pool. `TenantTx` deliberately exposes only
`Exec`, `Query` and `QueryRow` — the sqlc `DBTX` shape — and has no `Commit`,
`Rollback`, `Begin` or pool accessor. `Scoped.Tx` owns the transaction
boundary and has already set the tenant RLS context (`app.tenant_id`).

The platform needs to append an es-lite event and an audit record in the same
tenant transaction: either both commit or neither does. Recorded-but-not-done
and done-but-not-recorded are both failures the audit log exists to prevent.

Today `postgres.Store.Workspace` returns only `es.Store`, whose
`Append`/`ReadStream` open their own transaction on the store's `pgxpool`.
Joining a caller transaction therefore required exposing the pool and
reimplementing the append SQL outside es-lite, including the optimistic
concurrency guard, hash chain, commit clock and uniqueness claims.

ADR 0008 rejected exposing the storage transaction, because a general `Tx`
would leak the backend through `es.Store` and push the uniqueness invariant
into callers. This decision keeps that boundary: es-lite does not expose a
transaction to commit or roll back, and it still owns the append invariants.
It accepts a statement handle only for the duration of one append or read.

## Decision

### 1. A narrow `postgres.DBTX` interface

```go
type DBTX interface {
    Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
    Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
    QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}
```

This is exactly the method set `tenancy.TenantTx` exposes, so the platform can
pass it directly without es-lite importing tenancy. `*pgxpool.Pool` and
`pgx.Tx` also satisfy it.

### 2. An additive, workspace-scoped handle

`Store.Workspace` keeps its existing signature and behaviour. A new factory
returns the concrete workspace handle:

```go
func (s *Store) WorkspaceStore(workspaceID string) *Workspace
```

`*Workspace` implements `es.Store` and additionally exposes:

```go
func (w *Workspace) AppendTx(ctx context.Context, db DBTX, p es.AppendParams) (es.AppendResult, error)
func (w *Workspace) ReadStreamTx(ctx context.Context, db DBTX, sid es.StreamID, fromVersion, toVersion uint64) ([]es.Envelope, error)
```

Adding `WorkspaceStore` rather than changing `Workspace`'s return type keeps
every existing call site and interface satisfied.

### 3. The caller owns the boundary; es-lite owns the invariants

`AppendTx` and `ReadStreamTx` never commit or roll back `db`. They set
`app.workspace_id` on that transaction with
`set_config(..., is_local=true)`, so the workspace RLS policy applies for the
rest of the transaction and the caller's `app.tenant_id` is untouched. The
append path is the same code as `Append`: optimistic concurrency, uniqueness
claims, hash chain, `global_position`, the commit clock and opaque payloads
are unchanged.

`db` must be an active transaction. Passing a pool is unsupported because a
`is_local=true` setting lasts only for the statement outside a transaction
block; the method documentation states this.

## Alternatives considered

- **Context-carried transaction (audit's `WithTx`).** Works, but enlistment is
  invisible to the type system: forgetting it produces a correct-looking
  append that commits independently. The platform is already inside a typed
  `TenantTx`, so an explicit parameter is clearer here.
- **Expose `pgx.Tx` from `Workspace`.** Rejected: `tenancy.TenantTx` is not a
  `pgx.Tx` (it has no commit), and requiring one would force callers to expose
  the pool this API exists to avoid.
- **Change `Workspace` to return `*Workspace`.** Rejected as unnecessary and
  potentially breaking for callers that use the method value or an interface
  requiring `Workspace(string) es.Store`.
- **Context-free helpers taking a workspace string.** Rejected: it moves the
  RLS scope into an argument separate from the handle, inviting a workspace
  and tenant mismatch.

## Consequences

- The platform can run es-lite and audit appends in one `tenancy.TenantTx`
  without a pool and without copying es-lite SQL.
- Existing `Append`/`ReadStream` users are unaffected; both still open and own
  their transaction.
- The transaction-aware read is limited to `ReadStreamTx`. Other reads
  (`ReadStreamAsOf`, `ReadAll`, `CurrentStreamVersion`, `LookupClaim`) remain
  self-transacting until a caller needs them in-transaction.
- Tests cover commit/rollback, read-your-writes, workspace isolation in one
  transaction, tenant-context preservation, optimistic-concurrency conflict,
  hash chain, `global_position` and `recorded_at`.
