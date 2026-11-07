package postgres_test

import (
	"context"
	"slices"
	"strings"
	"testing"
)

// TestFeedIndexesExist pins the feed page indexes (spec §5.2). CREATE INDEX
// on the HASH-partitioned parent creates one child index per partition with
// auto-generated suffixed names, so existence is asserted by matching the
// indexdef column lists in pg_indexes rather than by name: both column sets
// must be present on every events partition (8 here, events_p0..p7).
func TestFeedIndexesExist(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	for _, idx := range []struct {
		name string
		cols string
	}{
		{"events_ws_pos_idx", "(workspace_id, global_position)"},
		{"events_ws_type_pos_idx", "(workspace_id, type_url, global_position)"},
	} {
		rows, err := s.Pool().Query(ctx, `
			SELECT tablename FROM pg_indexes
			WHERE tablename LIKE 'events_p%'
			  AND indexdef ILIKE '%`+idx.cols+`%'
			ORDER BY tablename`)
		if err != nil {
			t.Fatalf("query %s: %v", idx.name, err)
		}
		var tables []string
		for rows.Next() {
			var table string
			if err := rows.Scan(&table); err != nil {
				t.Fatalf("scan %s: %v", idx.name, err)
			}
			tables = append(tables, table)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatalf("rows %s: %v", idx.name, err)
		}
		for _, p := range []string{"events_p0", "events_p1", "events_p2", "events_p3", "events_p4", "events_p5", "events_p6", "events_p7"} {
			if !slices.Contains(tables, p) {
				t.Fatalf("%s: no index matching %s on %s (found on %v)",
					idx.name, idx.cols, p, tables)
			}
		}
	}
}

// TestAllBypassPoliciesRoleGated pins migration 0007's policy shape
// (review F8/F12) through pg_policies — not file greps, which would
// false-positive on comments (the Task 13 review ruling). All three
// policies that carry a bypass arm — events, unique_claims and
// anchors — must keep the arm (app.bypass_rls), gate it on es_relay
// membership (pg_has_role), and guard that test with EXISTS so it
// cannot hard-fail (pg_has_role errors) on a database where the role
// has not been created yet.
func TestAllBypassPoliciesRoleGated(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)

	for _, table := range []string{"events", "unique_claims", "anchors"} {
		rows, err := s.Pool().Query(ctx, `
			SELECT policyname, qual FROM pg_policies
			WHERE schemaname = 'public' AND tablename = $1`, table)
		if err != nil {
			t.Fatalf("read policies for %s: %v", table, err)
		}
		var policies []struct{ name, qual string }
		for rows.Next() {
			var name, qual string
			if err := rows.Scan(&name, &qual); err != nil {
				rows.Close()
				t.Fatalf("scan policy for %s: %v", table, err)
			}
			policies = append(policies, struct{ name, qual string }{name, qual})
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatalf("policies rows for %s: %v", table, err)
		}
		if len(policies) != 1 {
			t.Fatalf("%s: %d policies, want exactly 1", table, len(policies))
		}
		p := policies[0]
		for _, want := range []string{
			"app.bypass_rls", // the bypass arm survives...
			"pg_has_role",    // ...gated on es_relay membership (F8)...
			"EXISTS",         // ...behind the missing-role guard (F12)
		} {
			if !strings.Contains(p.qual, want) {
				t.Errorf("%s: policy %q qual %q lacks %q — the bypass arm must be role-gated behind an EXISTS guard",
					table, p.name, p.qual, want)
			}
		}
	}
}

// TestCheckpointsTableDropped pins migration 0008: the bare checkpoints
// table 0001 created is gone. It was dead on Postgres — only SQLite
// implements delivery.Checkpoints, and the feed's per-subscription
// progress lives in feed_checkpoints — and it was the one es-lite
// table without an RLS policy.
func TestCheckpointsTableDropped(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)

	var dropped bool
	if err := s.Pool().QueryRow(ctx, `SELECT to_regclass('checkpoints') IS NULL`).Scan(&dropped); err != nil {
		t.Fatalf("to_regclass: %v", err)
	}
	if !dropped {
		t.Fatal("the checkpoints table still exists — 0008 drops it (dead on Postgres: only SQLite implements delivery.Checkpoints)")
	}
}
