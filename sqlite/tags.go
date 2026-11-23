package sqlite

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/laenenai/es-lite/es"
)

var _ es.TagReader = (*Store)(nil)

// ReadByTags implements es.TagReader. SQLite is one workspace per database,
// so the query has no workspace predicate; the rest of the contract is the
// same as Postgres: every tag in q.Tags (AND), ascending global position,
// FromPosition exclusive, ToPosition inclusive (0 = no bound), LIMIT applied
// last. An empty result is (nil, nil).
func (s *Store) ReadByTags(ctx context.Context, q es.TagQuery) ([]es.Envelope, error) {
	if err := q.Validate(); err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(q.Tags))
	for k := range q.Tags {
		keys = append(keys, k)
	}
	// Deterministic parameter order; the semantics do not depend on it.
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString(`SELECT ` + envelopeCols + ` FROM events e
		WHERE e.global_position > ?
		  AND (? = 0 OR e.global_position <= ?)`)
	args := []any{int64(q.FromPosition), int64(q.ToPosition), int64(q.ToPosition)}
	for _, k := range keys {
		b.WriteString(`
		  AND EXISTS (SELECT 1 FROM event_tags t
		              WHERE t.tag_key = ?
		                AND t.tag_value = ?
		                AND t.global_position = e.global_position)`)
		args = append(args, k, q.Tags[k])
	}
	b.WriteString(`
		ORDER BY e.global_position
		LIMIT ?`)
	args = append(args, q.EffectiveLimit())

	envs, err := s.queryEnvelopes(ctx, b.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("sqlite: read by tags: %w", err)
	}
	return envs, nil
}
