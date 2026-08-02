// Package zeroknowledge is a worked example of the ADR 0009 deployment: a
// NATS eventstore service (holding no keys) plus a client that encrypts
// payloads client-side. It runs as an integration test (zeroknowledge_test.go)
// when PG_DSN and NATS_URL are set — see task test:zeroknowledge.
//
// The point it demonstrates: commands issued through the normal aggregate
// runtime travel to the server as ciphertext, the server stores ciphertext it
// cannot read, and the client reads them back and decrypts — with the runtime,
// decider, and codec entirely unchanged from the embedded case.
package zeroknowledge
