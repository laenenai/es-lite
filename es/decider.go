package es

// Decider is es-lite's aggregate model — pure functions describing how a
// stream's state evolves and which events a command produces. Adopted
// from the parent framework's ADR 0003; es-lite drops the parent's
// constraint-operations return value (cross-aggregate uniqueness is out
// of scope), leaving the classic three-function shape.
//
// All functions MUST be pure:
//
//   - Initial returns the zero-value state for a stream with no events.
//   - Decide is the business rule: given current state and a command,
//     return the events to append, or an error to reject the command
//     with no state change.
//   - Evolve folds one event into state. It runs during replay to
//     rebuild state from history, so it must never call time.Now,
//     generate IDs, perform I/O, or depend on anything outside its two
//     arguments. Determinism here is what makes time-travel exact.
//   - IsTerminal (optional) reports whether the stream is closed. The
//     runtime rejects further commands on terminal streams. nil means
//     "never terminal".
//
// Type parameters: S = state, C = command sum type, E = event sum type.
// C and E are sealed interfaces (proto oneof containers); see the codec.
type Decider[S, C, E any] struct {
	Initial    func() S
	Decide     func(state S, cmd C) (events []E, err error)
	Evolve     func(state S, event E) S
	IsTerminal func(state S) bool
}
