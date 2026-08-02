package es

// StateCodec serializes an aggregate's folded state S for the snapshot cache
// (ADR 0007/0009). For proto state it is a proto.Marshal/Unmarshal wrapper.
// Snapshots are a pure optimization: a StateCodec round-trip must reproduce
// the state exactly, and the state is always re-derivable from the log, so a
// codec change simply invalidates old snapshots (bump the fold version).
type StateCodec[S any] interface {
	Encode(state S) ([]byte, error)
	Decode(data []byte) (S, error)
}
