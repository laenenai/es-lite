// Package natsjs is the NATS/JetStream delivery adapter for es-lite
// (ADR 0003). It bridges the DB event log to a JetStream stream and drives
// projections as durable consumers.
//
// The relay side (Publisher) plugs into either backend's delivery path — the
// Postgres Store.Drain or the SQLite delivery.Poller — as a delivery.Handler.
// Each event is published with Nats-Msg-Id = global_position, so JetStream
// deduplicates: the relay can run N-way or fail over without emitting an
// event twice. The DB log stays the source of truth; JetStream is transport
// and fast-replay substrate, never the archive.
//
// This package uses the official nats.go/jetstream client directly (natskit
// is core-NATS only). Connection creation is left to the caller so it can
// reuse the service's existing nats.Conn / credentials.
package natsjs

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"

	"github.com/laenenai/es-lite/es"
)

// Header names carried on each published message. The body is the event
// payload bytes; everything else rides in headers so consumers can route and
// filter without decoding the body.
const (
	HdrMsgID          = "Nats-Msg-Id" // JetStream dedup key = global_position
	HdrEventID        = "Es-Event-Id"
	HdrTypeURL        = "Es-Type-Url"
	HdrSchemaVersion  = "Es-Schema-Version"
	HdrStream         = "Es-Stream"      // canonical "type:id"
	HdrWorkspace      = "Es-Workspace"
	HdrVersion        = "Es-Version"
	HdrGlobalPosition = "Es-Global-Position"
	HdrOccurredAt     = "Es-Occurred-At"
	HdrRecordedAt     = "Es-Recorded-At"
	HdrCorrelationID  = "Es-Correlation-Id"
	HdrCausationID    = "Es-Causation-Id"
	HdrCommandID      = "Es-Command-Id"
	HdrActorType      = "Es-Actor-Type"
	HdrActorID        = "Es-Actor-Id"
)

// SubjectFunc derives a NATS subject from an event. The default follows the
// nats-contracts lifecycle-event taxonomy: evt.<workspace>.<aggregate>.<event>.
type SubjectFunc func(e es.Envelope) string

// DefaultSubject builds evt.<workspace>.<aggregate>.<event>, lower-casing the
// event type and substituting "default" for an empty workspace (single-
// workspace backends). Tokens are sanitized to valid NATS subject characters.
func DefaultSubject(e es.Envelope) string {
	ws := subjectToken(e.Workspace)
	if ws == "" {
		ws = "default"
	}
	aggregate := subjectToken(e.StreamID.Type)
	event := subjectToken(strings.ToLower(shortType(e.TypeURL)))
	return "evt." + ws + "." + aggregate + "." + event
}

// shortType returns the last dot-separated segment of a proto type URL,
// e.g. "counter.v1.Incremented" -> "Incremented".
func shortType(typeURL string) string {
	if i := strings.LastIndexByte(typeURL, '.'); i >= 0 {
		return typeURL[i+1:]
	}
	return typeURL
}

// subjectToken replaces characters that are not valid in a NATS subject
// token ('.', ' ', '*', '>', and control/reserved chars) with '_'.
func subjectToken(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

const tsLayout = "2006-01-02T15:04:05.000000000Z07:00"

// toMsg encodes an Envelope as a JetStream message: metadata in headers,
// payload in the body, Nats-Msg-Id set to the global position for dedup.
func toMsg(subject string, e es.Envelope) *nats.Msg {
	h := nats.Header{}
	h.Set(HdrMsgID, strconv.FormatUint(e.GlobalPosition, 10))
	h.Set(HdrEventID, e.EventID.String())
	h.Set(HdrTypeURL, e.TypeURL)
	h.Set(HdrSchemaVersion, strconv.FormatUint(uint64(e.SchemaVersion), 10))
	h.Set(HdrStream, e.StreamID.Canonical())
	h.Set(HdrWorkspace, e.Workspace)
	h.Set(HdrVersion, strconv.FormatUint(e.Version, 10))
	h.Set(HdrGlobalPosition, strconv.FormatUint(e.GlobalPosition, 10))
	h.Set(HdrOccurredAt, e.OccurredAt.UTC().Format(tsLayout))
	h.Set(HdrRecordedAt, e.RecordedAt.UTC().Format(tsLayout))
	h.Set(HdrCorrelationID, e.CorrelationID.String())
	h.Set(HdrCausationID, e.CausationID.String())
	h.Set(HdrCommandID, e.CommandID.String())
	h.Set(HdrActorType, e.Actor.Type)
	h.Set(HdrActorID, e.Actor.ID)
	return &nats.Msg{Subject: subject, Header: h, Data: e.Payload}
}

// DecodeEnvelope reconstructs an Envelope from a delivered message's headers
// and body. Projection handlers use it to get back a typed es.Envelope.
func DecodeEnvelope(header nats.Header, data []byte) (es.Envelope, error) {
	var e es.Envelope
	sid, err := es.ParseCanonical(header.Get(HdrStream))
	if err != nil {
		return es.Envelope{}, fmt.Errorf("natsjs: decode stream %q: %w", header.Get(HdrStream), err)
	}
	e.StreamID = sid
	e.Workspace = header.Get(HdrWorkspace)
	e.TypeURL = header.Get(HdrTypeURL)
	e.EventID, _ = uuid.Parse(header.Get(HdrEventID))
	e.CorrelationID, _ = uuid.Parse(header.Get(HdrCorrelationID))
	e.CausationID, _ = uuid.Parse(header.Get(HdrCausationID))
	e.CommandID, _ = uuid.Parse(header.Get(HdrCommandID))
	e.Actor = es.Actor{Type: header.Get(HdrActorType), ID: header.Get(HdrActorID)}
	if v, err := strconv.ParseUint(header.Get(HdrVersion), 10, 64); err == nil {
		e.Version = v
	}
	if v, err := strconv.ParseUint(header.Get(HdrGlobalPosition), 10, 64); err == nil {
		e.GlobalPosition = v
	}
	if v, err := strconv.ParseUint(header.Get(HdrSchemaVersion), 10, 32); err == nil {
		e.SchemaVersion = uint32(v)
	}
	if t, err := time.Parse(tsLayout, header.Get(HdrOccurredAt)); err == nil {
		e.OccurredAt = t
	}
	if t, err := time.Parse(tsLayout, header.Get(HdrRecordedAt)); err == nil {
		e.RecordedAt = t
	}
	e.Payload = data
	return e, nil
}
