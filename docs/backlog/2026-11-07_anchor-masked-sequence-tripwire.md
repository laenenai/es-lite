# Anchor path can silently record a root over zero heads when the sequence grant is missing (Task 19 review follow-up)

- **Filed:** 2026-11-07 (recorded during Task 36; found by the Task 19 review)
- **Area:** postgres storage / anchors
- **Status:** Open (parked for the final whole-branch review by the controller)

## Problem

Parked follow-up from the Task 19 review (rated Important, outside that
task's scope), recorded here so it cannot be lost:

The anchor path's fence reads its bound through the `pg_sequences` view,
which **masks `last_value` to NULL for a role without `USAGE` on the
sequence**. An under-granted anchor job therefore resolves its bound to 0 —
`heads(0)` and `orderRows(0)` both return nothing — and `Anchor` would
silently record a root over zero heads as a real anchor, with `CheckAnchor`
re-blessing it (nil). The runbook's grants make the outage impossible when
followed (`GRANT USAGE ON SEQUENCE … TO <anchor job>`), and the
least-privilege e2e test drives the granted path; nothing, however, trips
the zero-heads shape itself.

Proposed cheap tripwire (from the review, to be evaluated): before the
INSERT, `len(heads) > 0` is knowable, and a `log_order` row whose
`from_position > upto` would be a self-contradiction at a resolved bound of
0 — a check with no false-positive on a genuinely fresh log. Side
observation also to evaluate: `heads` and `orderRows` interpret a resolved
bound of 0 differently.

## Acceptance criteria

- An `Anchor` call whose resolved bound yields zero heads on a non-empty
  log fails loudly instead of recording the empty anchor, with no false
  positive on a genuinely empty database.
- A test pins the masked-sequence shape (grant-less role → bound 0) end to
  end.
- The runbook's anchor-job grant note explains the failure the tripwire
  turns on.
