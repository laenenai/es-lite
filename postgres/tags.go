package postgres

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/laenenai/es-lite/es"
)

var _ es.TagReader = (*Workspace)(nil)

// ReadByTags implements es.TagReader. It returns the events carrying every
// tag in q.Tags, ordered by ascending global position, with FromPosition
// exclusive and ToPosition inclusive (0 = no upper bound). An empty result is
// (nil, nil): unlike ReadStream, "no tag matches" is a normal answer, not a
// missing stream.
//
// The query drives events in position order and probes the tag-first primary
// key once per tag. That shape stops at LIMIT instead of materializing the
// whole intersection, which is what a change feed wants for a common tag;
// the planner may still prefer a candidate-first intersection for a rare
// tag, and both plans satisfy the same semantics.
func (w *Workspace) ReadByTags(ctx context.Context, q es.TagQuery) ([]es.Envelope, error) {
	if err := q.Validate(); err != nil {
		return nil, err
	}

	tx, err := w.store.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := setWorkspace(ctx, tx, w.ws); err != nil {
		return nil, fmt.Errorf("set workspace: %w", err)
	}

	keys := make([]string, 0, len(q.Tags))
	for k := range q.Tags {
		keys = append(keys, k)
	}
	// Deterministic parameter order keeps the statement stable for tests
	// and plan caches; the semantics do not depend on it.
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString(`SELECT ` + envelopeCols + ` FROM events e
		WHERE e.workspace_id = $1
		  AND e.global_position > $2
		  AND ($3::bigint = 0 OR e.global_position <= $3)`)
	args := []any{w.ws, int64(q.FromPosition), int64(q.ToPosition)}
	for _, k := range keys {
		args = append(args, k, q.Tags[k])
		keyArg := len(args) - 1
		valueArg := len(args)
		fmt.Fprintf(&b, `
		  AND EXISTS (SELECT 1 FROM event_tags t
		              WHERE t.workspace_id = e.workspace_id
		                AND t.tag_key = $%d
		                AND t.tag_value = $%d
		                AND t.global_position = e.global_position)`,
			keyArg, valueArg)
	}
	args = append(args, q.EffectiveLimit())
	fmt.Fprintf(&b, `
		ORDER BY e.global_position
		LIMIT $%d`, len(args))

	rows, err := tx.Query(ctx, b.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("read by tags: %w", err)
	}
	defer rows.Close()

	var out []es.Envelope
	for rows.Next() {
		env, err := scanEnvelope(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, env)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read by tags: %w", err)
	}
	return out, nil
}
