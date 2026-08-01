// Package es is the core of es-lite: a small event-sourcing library
// where the append-only event log is the source of truth.
//
// The design is captured in docs/adr/0001. In one paragraph: aggregates
// are modeled as a Decider (three pure functions); commands produce
// events; events are appended to a storage-neutral Store as an immutable,
// totally-ordered log; current state is derived by folding a stream's
// events through Decider.Evolve; downstream consumers tail the log via a
// poller (the log doubles as the outbox). History, auditability, and
// time-travel fall out of the log being immutable and append-only.
package es
