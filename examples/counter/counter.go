// Package counter is a worked example aggregate: a bounded counter. It
// shows the split es-lite asks of you — the .proto defines the shapes
// (see proto/counter/v1/counter.proto), and you hand-write the Decider
// and its error sentinels. Everything else (sum types, codec, storage,
// delivery) is machinery.
package counter

import (
	"errors"

	counterv1 "github.com/laenenai/es-lite/gen/counter/v1"
	"github.com/laenenai/es-lite/es"
)

// Domain errors, surfaced verbatim by aggregate.Runtime.Handle when
// Decide rejects a command.
var (
	ErrAlreadyInitialized = errors.New("counter: already initialized")
	ErrNotInitialized     = errors.New("counter: not initialized")
	ErrBadRange           = errors.New("counter: min must be <= max")
	ErrOutOfRange         = errors.New("counter: value would fall outside [min,max]")
	ErrUnknownCommand     = errors.New("counter: unknown command")
)

// Decider is the counter's business logic: three pure functions plus a
// terminal check. State is the proto Counter message used directly.
var Decider = es.Decider[*counterv1.Counter, counterv1.Command, counterv1.Event]{
	Initial: func() *counterv1.Counter { return &counterv1.Counter{} },

	Decide: func(s *counterv1.Counter, c counterv1.Command) ([]counterv1.Event, error) {
		switch cmd := c.(type) {
		case *counterv1.Init:
			if s.GetInitialized() {
				return nil, ErrAlreadyInitialized
			}
			if cmd.GetMin() > cmd.GetMax() {
				return nil, ErrBadRange
			}
			if cmd.GetInitial() < cmd.GetMin() || cmd.GetInitial() > cmd.GetMax() {
				return nil, ErrOutOfRange
			}
			return []counterv1.Event{&counterv1.Initialized{
				Min: cmd.GetMin(), Max: cmd.GetMax(), Value: cmd.GetInitial(),
			}}, nil

		case *counterv1.Increment:
			if !s.GetInitialized() {
				return nil, ErrNotInitialized
			}
			if s.GetCount()+cmd.GetBy() > s.GetMax() {
				return nil, ErrOutOfRange
			}
			return []counterv1.Event{&counterv1.Incremented{By: cmd.GetBy()}}, nil

		case *counterv1.Decrement:
			if !s.GetInitialized() {
				return nil, ErrNotInitialized
			}
			if s.GetCount()-cmd.GetBy() < s.GetMin() {
				return nil, ErrOutOfRange
			}
			return []counterv1.Event{&counterv1.Decremented{By: cmd.GetBy()}}, nil

		case *counterv1.Close:
			if !s.GetInitialized() {
				return nil, ErrNotInitialized
			}
			return []counterv1.Event{&counterv1.Closed{}}, nil

		default:
			return nil, ErrUnknownCommand
		}
	},

	Evolve: func(s *counterv1.Counter, e counterv1.Event) *counterv1.Counter {
		switch ev := e.(type) {
		case *counterv1.Initialized:
			s.Initialized = true
			s.Min = ev.GetMin()
			s.Max = ev.GetMax()
			s.Count = ev.GetValue()
		case *counterv1.Incremented:
			s.Count += ev.GetBy()
		case *counterv1.Decremented:
			s.Count -= ev.GetBy()
		case *counterv1.Closed:
			s.Closed = true
		}
		return s
	},

	IsTerminal: func(s *counterv1.Counter) bool { return s.GetClosed() },
}
