// Package migrate loads versioned SQL migrations embedded per backend
// (ADR 0010). Files are named "<version>_<name>.sql" with a zero-padded
// numeric prefix (e.g. "0001_initial_schema.sql"); Load parses and sorts them
// by version. Applying them (with a history table + a lock) is the backend
// adapter's job, since SQLite and Postgres use different drivers.
package migrate

import (
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
)

// Migration is one numbered SQL migration.
type Migration struct {
	Version int
	Name    string
	SQL     string
}

// Load reads and sorts all "*.sql" migrations under dir in fsys.
func Load(fsys fs.FS, dir string) ([]Migration, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, fmt.Errorf("migrate: read %s: %w", dir, err)
	}
	var migs []Migration
	seen := map[int]string{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".sql") {
			continue
		}
		version, short, err := parseName(name)
		if err != nil {
			return nil, err
		}
		if prev, ok := seen[version]; ok {
			return nil, fmt.Errorf("migrate: duplicate version %d (%s and %s)", version, prev, name)
		}
		seen[version] = name
		body, err := fs.ReadFile(fsys, dir+"/"+name)
		if err != nil {
			return nil, fmt.Errorf("migrate: read %s: %w", name, err)
		}
		migs = append(migs, Migration{Version: version, Name: short, SQL: string(body)})
	}
	sort.Slice(migs, func(i, j int) bool { return migs[i].Version < migs[j].Version })
	return migs, nil
}

// parseName splits "0001_initial_schema.sql" into (1, "initial_schema").
func parseName(name string) (int, string, error) {
	base := strings.TrimSuffix(name, ".sql")
	i := strings.IndexByte(base, '_')
	if i <= 0 {
		return 0, "", fmt.Errorf("migrate: %q must be <version>_<name>.sql", name)
	}
	version, err := strconv.Atoi(base[:i])
	if err != nil {
		return 0, "", fmt.Errorf("migrate: %q has a non-numeric version prefix: %w", name, err)
	}
	return version, base[i+1:], nil
}
