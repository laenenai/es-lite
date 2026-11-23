package es

import (
	"github.com/google/uuid"
)

// Meta is the command-scoped audit/causality metadata a caller supplies
// when handling a command. It is copied onto every event the command
// produces, giving the log its audit trail. The zero Meta is valid: the
// runtime fills in a fresh CommandID and defaults CorrelationID to it.
// Meta deliberately carries no time: the runtime stamps every event's
// Envelope.OccurredAt from the server clock (ADR 0034).
type Meta struct {
	CommandID     uuid.UUID // this command's id; generated if zero
	CorrelationID uuid.UUID // originating flow; defaults to CommandID if zero
	CausationID   uuid.UUID // the cause of this command (e.g. a prior event id)
	Actor         Actor     // who issued the command

	// Tags are batch-scoped tags copied onto every event the command
	// produces (for FlowDoc, flowdoc.space). The store validates them and
	// overlays each event's own Decider.Tags on top.
	Tags map[string]string
}
