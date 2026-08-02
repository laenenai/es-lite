package natsstore

import (
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/laenenai/es-lite/es"
)

const tsLayout = time.RFC3339Nano

func is(err, target error) bool { return errors.Is(err, target) }
func newErr(msg string) error   { return errors.New(msg) }

// remoteErr carries a remote error's message while unwrapping to a local
// sentinel, so errors.Is works across the network boundary.
type remoteErr struct {
	sentinel error
	msg      string
}

func (e *remoteErr) Error() string { return e.msg }
func (e *remoteErr) Unwrap() error { return e.sentinel }

func wrap(sentinel error, msg string) error {
	if msg == "" {
		return sentinel
	}
	return &remoteErr{sentinel: sentinel, msg: msg}
}

func fmtTS(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(tsLayout)
}

func parseTS(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(tsLayout, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

func toEnvelopeWire(e es.Envelope) envelopeWire {
	return envelopeWire{
		EventID:        e.EventID.String(),
		Workspace:      e.Workspace,
		StreamType:     e.StreamID.Type,
		StreamID:       e.StreamID.ID,
		Version:        e.Version,
		GlobalPosition: e.GlobalPosition,
		TypeURL:        e.TypeURL,
		SchemaVersion:  e.SchemaVersion,
		OccurredAt:     fmtTS(e.OccurredAt),
		RecordedAt:     fmtTS(e.RecordedAt),
		CorrelationID:  e.CorrelationID.String(),
		CausationID:    e.CausationID.String(),
		CommandID:      e.CommandID.String(),
		ActorType:      e.Actor.Type,
		ActorID:        e.Actor.ID,
		Payload:        e.Payload,
	}
}

func fromEnvelopeWire(w envelopeWire) (es.Envelope, error) {
	sid, err := es.NewStreamID(w.StreamType, w.StreamID)
	if err != nil {
		return es.Envelope{}, err
	}
	e := es.Envelope{
		StreamID:       sid,
		Workspace:      w.Workspace,
		Version:        w.Version,
		GlobalPosition: w.GlobalPosition,
		TypeURL:        w.TypeURL,
		SchemaVersion:  w.SchemaVersion,
		OccurredAt:     parseTS(w.OccurredAt),
		RecordedAt:     parseTS(w.RecordedAt),
		Actor:          es.Actor{Type: w.ActorType, ID: w.ActorID},
		Payload:        w.Payload,
	}
	e.EventID, _ = uuid.Parse(w.EventID)
	e.CorrelationID, _ = uuid.Parse(w.CorrelationID)
	e.CausationID, _ = uuid.Parse(w.CausationID)
	e.CommandID, _ = uuid.Parse(w.CommandID)
	return e, nil
}

func toEnvelopesWire(envs []es.Envelope) []envelopeWire {
	out := make([]envelopeWire, len(envs))
	for i, e := range envs {
		out[i] = toEnvelopeWire(e)
	}
	return out
}

func fromEnvelopesWire(ws []envelopeWire) ([]es.Envelope, error) {
	out := make([]es.Envelope, len(ws))
	for i, w := range ws {
		e, err := fromEnvelopeWire(w)
		if err != nil {
			return nil, err
		}
		out[i] = e
	}
	return out, nil
}

func toConstraintsWire(ops []es.ConstraintOp) []constraintWire {
	if len(ops) == 0 {
		return nil
	}
	out := make([]constraintWire, len(ops))
	for i, op := range ops {
		out[i] = constraintWire{Op: uint8(op.Op), Scope: op.Scope, Value: op.Value, PII: op.PII}
	}
	return out
}

func fromConstraintsWire(ws []constraintWire) []es.ConstraintOp {
	if len(ws) == 0 {
		return nil
	}
	out := make([]es.ConstraintOp, len(ws))
	for i, w := range ws {
		out[i] = es.ConstraintOp{Op: es.ConstraintOpKind(w.Op), Scope: w.Scope, Value: w.Value, PII: w.PII}
	}
	return out
}

func toEventDataWire(evs []es.EventData) []eventDataWire {
	out := make([]eventDataWire, len(evs))
	for i, e := range evs {
		out[i] = eventDataWire{EventID: uuidStr(e.EventID), TypeURL: e.TypeURL, SchemaVersion: e.SchemaVersion, Payload: e.Payload}
	}
	return out
}

func fromEventDataWire(ws []eventDataWire) []es.EventData {
	out := make([]es.EventData, len(ws))
	for i, w := range ws {
		out[i] = es.EventData{EventID: parseUUID(w.EventID), TypeURL: w.TypeURL, SchemaVersion: w.SchemaVersion, Payload: w.Payload}
	}
	return out
}

func parseUUID(s string) uuid.UUID {
	id, _ := uuid.Parse(s)
	return id
}

func uuidStr(id uuid.UUID) string {
	if id == uuid.Nil {
		return ""
	}
	return id.String()
}
