package projection

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Status is a projection's checkpoint state. The values are persisted as text
// ("live", "rebuilding", "stalled") so a database CHECK constraint can guard
// them; StatusUnknown means "no row" and is never written by the SQL stores.
type Status uint8

const (
	// StatusUnknown is the zero value: no checkpoint exists.
	StatusUnknown Status = iota
	// StatusLive means the projector is consuming the live feed.
	StatusLive
	// StatusRebuilding means a rebuild is in progress.
	StatusRebuilding
	// StatusStalled means the projector stopped advancing (for example after
	// an unrecoverable error) and a reader must not wait for it forever.
	StatusStalled
)

// String returns the persisted text of s: "unknown", "live", "rebuilding" or
// "stalled". An out-of-range value formats as "Status(n)".
func (s Status) String() string {
	switch s {
	case StatusUnknown:
		return "unknown"
	case StatusLive:
		return "live"
	case StatusRebuilding:
		return "rebuilding"
	case StatusStalled:
		return "stalled"
	default:
		return fmt.Sprintf("Status(%d)", uint8(s))
	}
}

// ParseStatus parses the persisted text of a Status. It reports false for
// anything String would not produce.
func ParseStatus(text string) (Status, bool) {
	switch text {
	case "unknown":
		return StatusUnknown, true
	case "live":
		return StatusLive, true
	case "rebuilding":
		return StatusRebuilding, true
	case "stalled":
		return StatusStalled, true
	default:
		return StatusUnknown, false
	}
}

// Checkpoint is one projection's progress within a scope: the highest applied
// global position, the projector's state and when the row last changed. Scope
// and Projection identify the row; a reader writes them because it may hold a
// Checkpoint without the key it loaded it by.
type Checkpoint struct {
	Scope      string
	Projection string
	Position   uint64
	State      Status
	UpdatedAt  time.Time
}

// ProgressStore persists projection checkpoints for read-your-writes (ADR
// 0045). SaveProgress is write-through and runs after a read-model commit;
// readers compare Position and State, never UpdatedAt.
//
// Position never goes backwards within a state: a save at or below the stored
// position in the same state leaves the position alone. A save in a different
// state replaces the position, because a rebuild legitimately restarts it.
// SetStatus never moves the position.
type ProgressStore interface {
	// LoadProgress returns the checkpoint for (scope, projection). found is
	// false when no row exists, in which case the Checkpoint is the zero
	// value.
	LoadProgress(ctx context.Context, scope, projection string) (Checkpoint, bool, error)
	// SaveProgress stores cp under (scope, projection), which override
	// cp.Scope and cp.Projection. It creates the row when absent and
	// refreshes UpdatedAt on every successful save, even when the position
	// does not move.
	SaveProgress(ctx context.Context, scope, projection string, cp Checkpoint) error
	// ResetProgress replaces the row with position 0 and the given state and
	// returns the stored checkpoint. It creates the row when absent.
	ResetProgress(ctx context.Context, scope, projection string, state Status) (Checkpoint, error)
	// SetStatus changes only the state (and UpdatedAt). It creates a
	// zero-position row when absent and never moves the position.
	SetStatus(ctx context.Context, scope, projection string, state Status) error
}

// ProgressPublisher receives checkpoint advances for the in-process waiters of
// read-your-writes. Implementations must not block the projector: Advance runs
// on the apply path.
type ProgressPublisher interface {
	// Advance reports the checkpoint a projection just saved.
	Advance(cp Checkpoint)
	// SetStatus reports a state change that did not move the position.
	SetStatus(scope, projection string, state Status)
}

// MemoryProgressStore is an in-process ProgressStore for tests and throwaway
// projectors. It is safe for concurrent use.
type MemoryProgressStore struct {
	mu   sync.Mutex
	rows map[progressKey]Checkpoint
}

// NewMemoryProgressStore returns an empty MemoryProgressStore.
func NewMemoryProgressStore() *MemoryProgressStore {
	return &MemoryProgressStore{rows: map[progressKey]Checkpoint{}}
}

// progressKey identifies one projection's row within a scope.
type progressKey struct {
	scope      string
	projection string
}

var _ ProgressStore = (*MemoryProgressStore)(nil)

// LoadProgress implements ProgressStore.
func (m *MemoryProgressStore) LoadProgress(_ context.Context, scope, projection string) (Checkpoint, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp, ok := m.rows[progressKey{scope: scope, projection: projection}]
	return cp, ok, nil
}

// SaveProgress implements ProgressStore.
func (m *MemoryProgressStore) SaveProgress(_ context.Context, scope, projection string, cp Checkpoint) error {
	cp.Scope, cp.Projection = scope, projection
	m.mu.Lock()
	defer m.mu.Unlock()
	key := progressKey{scope: scope, projection: projection}
	if prev, ok := m.rows[key]; ok && prev.State == cp.State && prev.Position > cp.Position {
		cp.Position = prev.Position
	}
	cp.UpdatedAt = time.Now().UTC()
	m.rows[key] = cp
	return nil
}

// ResetProgress implements ProgressStore.
func (m *MemoryProgressStore) ResetProgress(_ context.Context, scope, projection string, state Status) (Checkpoint, error) {
	cp := Checkpoint{
		Scope:      scope,
		Projection: projection,
		State:      state,
		UpdatedAt:  time.Now().UTC(),
	}
	m.mu.Lock()
	m.rows[progressKey{scope: scope, projection: projection}] = cp
	m.mu.Unlock()
	return cp, nil
}

// SetStatus implements ProgressStore.
func (m *MemoryProgressStore) SetStatus(_ context.Context, scope, projection string, state Status) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := progressKey{scope: scope, projection: projection}
	cp, ok := m.rows[key]
	if !ok {
		cp = Checkpoint{Scope: scope, Projection: projection}
	}
	cp.State = state
	cp.UpdatedAt = time.Now().UTC()
	m.rows[key] = cp
	return nil
}
