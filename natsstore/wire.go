// Package natsstore exposes an es.Store over NATS request/reply (ADR 0009):
// a Server that serves any es.Store, and a Client that IS an es.Store backed
// by NATS. Because es.Store is the whole contract, the existing aggregate
// runtime/decider/codec run unchanged on the client side.
//
// Payloads are opaque here — the wire carries whatever bytes it is given. In
// the zero-knowledge deployment (ADR 0009) the client wraps this Client in
// cryptostore, so only ciphertext ever reaches the Server. The Server holds
// no keys and never decrypts.
package natsstore

import (
	"time"

	"github.com/laenenai/es-lite/es"
)

// Subject layout under the (dedicated) ESLITE account. Requests are queue-
// subscribed so stateless Server replicas load-balance over one database.
const (
	DefaultPrefix = "svc.eslite"
	DefaultQueue  = "eslite"

	mAppend         = "append"
	mReadStream     = "read_stream"
	mReadStreamAsOf = "read_stream_asof"
	mReadAll        = "read_all"
	mCurrentVersion = "current_version"
	mLookupClaim    = "lookup_claim"

	defaultTimeout = 5 * time.Second
)

// ---- wire messages (JSON; both ends are Go in model A) --------------------

type eventDataWire struct {
	EventID       string `json:"event_id,omitempty"`
	TypeURL       string `json:"type_url"`
	SchemaVersion uint32 `json:"schema_version"`
	Payload       []byte `json:"payload"` // opaque (ciphertext in zero-knowledge mode)
}

type constraintWire struct {
	Op    uint8  `json:"op"`
	Scope string `json:"scope"`
	Value string `json:"value"` // opaque key (client may have HMAC'd it)
	PII   bool   `json:"pii"`
}

type envelopeWire struct {
	EventID        string `json:"event_id"`
	Workspace      string `json:"workspace"`
	StreamType     string `json:"stream_type"`
	StreamID       string `json:"stream_id"`
	Version        uint64 `json:"version"`
	GlobalPosition uint64 `json:"global_position"`
	TypeURL        string `json:"type_url"`
	SchemaVersion  uint32 `json:"schema_version"`
	OccurredAt     string `json:"occurred_at"`
	RecordedAt     string `json:"recorded_at"`
	CorrelationID  string `json:"correlation_id"`
	CausationID    string `json:"causation_id"`
	CommandID      string `json:"command_id"`
	ActorType      string `json:"actor_type"`
	ActorID        string `json:"actor_id"`
	Payload        []byte `json:"payload"`
}

// Request structs carry NO workspace field: the workspace is the subject's
// <ws> token (architecture ADR 0005), cross-checked against auth server-side.
type appendReq struct {
	StreamType      string           `json:"stream_type"`
	StreamID        string           `json:"stream_id"`
	ExpectedVersion uint64           `json:"expected_version"`
	Events          []eventDataWire  `json:"events"`
	Constraints     []constraintWire `json:"constraints,omitempty"`
	CommandID       string           `json:"command_id,omitempty"`
	CorrelationID   string           `json:"correlation_id,omitempty"`
	CausationID     string           `json:"causation_id,omitempty"`
	ActorType       string           `json:"actor_type,omitempty"`
	ActorID         string           `json:"actor_id,omitempty"`
	OccurredAt      string           `json:"occurred_at,omitempty"`
}

type appendResp struct {
	ErrKind     string         `json:"err_kind,omitempty"`
	ErrMsg      string         `json:"err_msg,omitempty"`
	FromVersion uint64         `json:"from_version"`
	ToVersion   uint64         `json:"to_version"`
	Envelopes   []envelopeWire `json:"envelopes,omitempty"`
}

type readStreamReq struct {
	StreamType  string `json:"stream_type"`
	StreamID    string `json:"stream_id"`
	FromVersion uint64 `json:"from_version"`
	ToVersion   uint64 `json:"to_version"`
	AsOf        string `json:"as_of,omitempty"` // set for read_stream_asof
}

type readAllReq struct {
	FromPosition uint64 `json:"from_position"`
	Limit        int    `json:"limit"`
}

type currentVersionReq struct {
	StreamType string `json:"stream_type"`
	StreamID   string `json:"stream_id"`
}

type lookupClaimReq struct {
	Scope string `json:"scope"`
	Value string `json:"value"` // opaque key (client may have HMAC'd PII)
	PII   bool   `json:"pii"`
}

type lookupClaimResp struct {
	ErrKind  string `json:"err_kind,omitempty"`
	ErrMsg   string `json:"err_msg,omitempty"`
	StreamID string `json:"stream_id"`
	Found    bool   `json:"found"`
}

type readResp struct {
	ErrKind   string         `json:"err_kind,omitempty"`
	ErrMsg    string         `json:"err_msg,omitempty"`
	Envelopes []envelopeWire `json:"envelopes,omitempty"`
}

type versionResp struct {
	ErrKind string `json:"err_kind,omitempty"`
	ErrMsg  string `json:"err_msg,omitempty"`
	Version uint64 `json:"version"`
}

// ---- error mapping across the wire ----------------------------------------

func errKind(err error) (kind, msg string) {
	switch {
	case err == nil:
		return "", ""
	case is(err, es.ErrConflict):
		return "conflict", err.Error()
	case is(err, es.ErrConstraintViolated):
		return "constraint", err.Error()
	case is(err, es.ErrTerminal):
		return "terminal", err.Error()
	case is(err, es.ErrStreamNotFound):
		return "not_found", err.Error()
	case is(err, es.ErrInvalidStreamID):
		return "invalid_stream", err.Error()
	default:
		return "internal", err.Error()
	}
}

// errFromKind reconstructs a matching sentinel client-side so callers can use
// errors.Is across the network boundary.
func errFromKind(kind, msg string) error {
	switch kind {
	case "":
		return nil
	case "conflict":
		return wrap(es.ErrConflict, msg)
	case "constraint":
		return wrap(es.ErrConstraintViolated, msg)
	case "terminal":
		return wrap(es.ErrTerminal, msg)
	case "not_found":
		return wrap(es.ErrStreamNotFound, msg)
	case "invalid_stream":
		return wrap(es.ErrInvalidStreamID, msg)
	default:
		return newErr(msg)
	}
}
