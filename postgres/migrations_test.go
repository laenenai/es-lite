package postgres_test

import (
	"context"
	"slices"
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
