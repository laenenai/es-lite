// Package snapshots is a worked example of the NATS KV snapshot cache
// (ADR 0007/0009): the runtime seeds folding from a client-encrypted snapshot
// in NATS KV and folds only the tail, offers bounded-staleness reads, and
// invalidates snapshots on a fold-version bump. It runs as an integration test
// (snapshots_test.go) when NATS_URL is set — see task test:snapshots.
package snapshots
