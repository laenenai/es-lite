// Package pipeline is a worked end-to-end example wiring the whole es-lite
// stack together: commands are handled against a workspace-scoped Postgres
// store (events encrypted at rest under a per-workspace DEK), a delivery.Relay
// drains the log and publishes to JetStream via the natsjs.Publisher, and a
// durable JetStream consumer projects the events into an in-memory read model.
//
// It exists as an integration test (pipeline_test.go), which runs when both
// PG_DSN and NATS_URL are set. See task test:pipeline.
package pipeline
