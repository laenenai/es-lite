# Postgres transaction-scaffolding duplication (review 2026-11-07, lead finding)

- **Filed:** 2026-11-07 (Task 36 documentation truth pass)
- **Area:** postgres storage
- **Status:** Open

## Problem

The 2026-11-07 review's lead noted "~10× copy-pasted transaction scaffolding
in `postgres/wsstore.go`" — the begin / defer-rollback / set-workspace (or
set-bypass) / run / commit shape repeated across the store's transactional
methods. The consolidated review deferred it as a not-defect:
"postgres transaction-scaffolding refactor (folded opportunistically into
Tasks 16–19 as those files are touched, tracked in Task 36's checklist)"
(`docs/review/20261107_consolidated_review.md` §3).

Tasks 16–19 touched and re-verified these paths but did not extract a shared
helper — each site still carries its own begin/rollback/pin/commit block
(`postgres/anchor.go`, `postgres/drain.go`, `postgres/feed.go`
`runWorkspace`, `postgres/store.go`, `postgres/wsstore.go`). The duplication
is real but every site has small deliberate differences (bypass vs workspace
pinning, error-voicing conventions, fence interactions), so the refactor is
a behavior-preserving cleanup, not a bug fix.

## Acceptance criteria

- One unexported helper (or a small set) owns the transaction scaffolding
  shape; each caller contributes its pinning and body.
- No behavior change: error messages, wrap prefixes (`postgres: …`),
  rollback/commit semantics and the fence interactions stay byte-compatible
  with today's suites (`go test ./postgres`, `task test:pg`,
  `task test:feedgate` all green untouched).
- The review's finding is referenced in the helper's doc comment so the
  history stays traceable.
