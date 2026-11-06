# Reconnect-swallowed feed errors are unobservable (review 20261104 A12/A14)

- **Filed:** 2026-11-05
- **Area:** eventfeed / projection reliability
- **Status:** Open

## Problem

The reconnect loop in `projection/reconnecting.go` deliberately drops the
inner feed's error and retries from the last acknowledged position: the
failure mode is delay and redelivery, never loss, so swallowing is safe
for correctness. But the swallowed error is invisible — an operator cannot
distinguish "quiet" from "reconnect-failing in a loop". The review
([20261104_feed_review.md](../review/20261104_feed_review.md), items A12
and A14) requires making it observable: an observer hook for
reconnect-swallowed errors, alongside the reconnect-hygiene hardening
(backoff reset only after sustained health).

Until the hook lands, `.golangci.yml` carries an SA4006 exclusion scoped
to `projection/reconnecting.go` — the gate stays honest about what the
code does instead of pretending the error was handled. The exclusion is
the trigger's other half: it must not outlive the fix.

## Acceptance criteria

- [ ] The observer hook for reconnect-swallowed errors lands — the same
      Observer shape ADR 0037's stall-observability semantics
      ("Stall semantics: delay, never loss") already established for
      `OnStall` — and the reconnect loop reports every swallowed error
      through it.
- [ ] The SA4006 exclusion for `projection/reconnecting.go` in
      `.golangci.yml` is deleted in the same change, and
      `golangci-lint` passes without it.
