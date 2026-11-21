-- Append decisions (ADR 0043): the durable record of the constraint ops
-- an append applied, so the unique_claims index can be reconstructed from
-- the log alone after a restore, a clone, or a verification rebuild.
--
-- One row records one append that carried at least one constraint op, not
-- one op: the encoded ops (es.EncodeDecisionOps) are the append's original
-- Claim/Release sequence, in order, so a rebuild replays the decisions
-- rather than guessing at their effects. An append with no constraints
-- writes no row. The row is written inside the append transaction, AFTER
-- the claim index has been updated in that same transaction: a constraint
-- violation aborts the append and rolls the decision row back with the
-- claims and events, so a committed append can never have a decision row
-- without its claim effects (or the reverse). A claim-only append — zero
-- events — still writes a row, with from_version == to_version ==
-- ExpectedVersion: the decision is the append's only durable effect, and
-- dropping it would lose the claim's provenance.
--
-- from_version/to_version are the appended span:
-- [from_version, to_version), to_version = ExpectedVersion + len(events).
-- command_id is the append's CommandID as events stores it, including the
-- zero UUID when the caller left it unset. The one-time cutover backfill
-- (AdoptClaims, ADR 0043 §3.5) has no command behind its synthetic Claims
-- and therefore writes NULL; live appends always write a string.
--
-- AUTOINCREMENT is deliberate: id is the replay order — the identity order
-- in which the appends committed — so SQLite must never reuse an id after
-- a DELETE (a plain INTEGER PRIMARY KEY would hand the highest deleted
-- rowid out again, and a replay would see the new decision inside the
-- old one's span). created_at is table bookkeeping, not part of any
-- contract.
--
-- One SQLite database is one workspace: no workspace_id column, no RLS,
-- no policy, and no grants (like every migration before this one).

CREATE TABLE IF NOT EXISTS append_decisions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_id    TEXT    NOT NULL,
    from_version INTEGER NOT NULL,
    to_version   INTEGER NOT NULL,
    command_id   TEXT    NULL,
    ops          BLOB    NOT NULL,
    created_at   TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    CHECK (from_version <= to_version)
);
