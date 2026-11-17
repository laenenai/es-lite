package es

import "context"

// WithAfterAppend wraps store so fn runs once, synchronously, after every
// successful Append — after the append transaction has committed and before
// Append returns to the caller. fn is never called on an error: the hook
// only ever sees a durable result.
//
// fn has no error return on purpose: the append is already durable, and a
// failed hint must never look like a failed append (which would invite a
// retry and a duplicate). A panic from fn is recovered and discarded for the
// same reason: the append stays committed, the hint is lost, and delivery
// falls back to the poll. Log inside fn and do not block.
//
// The workspace must be closed over by the caller (e.g. via the fn
// closure); it is NOT reliably available from AppendResult. A claim-only
// append succeeds with zero envelopes, so indexing Envelopes[0] is wrong,
// and Envelope.Workspace is empty on single-workspace backends (SQLite).
//
// Two boundaries are deliberate:
//
//   - AppendTx is not hooked. AppendTx is not part of es.Store at all; the
//     caller owns that transaction and only the caller knows when it
//     commits, so the caller must wake after its own commit.
//   - aggregate.WithAppend is not hooked. It replaces store.Append for the
//     call and therefore bypasses this decorator; the same rule applies —
//     wake after the commit.
//
// The wrapper returns es.Store, so it erases concrete methods a caller may
// have used on the wrapped store. Wrap only the handle used for plain
// appends.
func WithAfterAppend(store Store, fn func(context.Context, AppendResult)) Store {
	if fn == nil {
		return store
	}
	return &afterAppendStore{Store: store, fn: fn}
}

// afterAppendStore is the WithAfterAppend decorator. Store is embedded so
// every method but Append passes through unchanged; AppendTx is not in
// Store and therefore not visible here at all.
type afterAppendStore struct {
	Store
	fn func(context.Context, AppendResult)
}

var _ Store = (*afterAppendStore)(nil)

// Append delegates to the wrapped store, then fires the hint once on
// success. On error the hook is skipped and the error is returned verbatim.
func (s *afterAppendStore) Append(ctx context.Context, p AppendParams) (AppendResult, error) {
	res, err := s.Store.Append(ctx, p)
	if err != nil {
		return res, err
	}
	s.call(ctx, res)
	return res, nil
}

// call runs the hint with a panic barrier: the append is committed by now,
// so a panicking hint must not turn a durable append into a caller-visible
// failure. Delivery falls back to the poll.
func (s *afterAppendStore) call(ctx context.Context, res AppendResult) {
	defer func() { _ = recover() }()
	s.fn(ctx, res)
}
