package es

import (
	"time"

	"github.com/google/uuid"
)

// Meta is the command-scoped audit/causality metadata a caller supplies
// when handling a command. It is copied onto every event the command
// produces, giving the log its audit trail. The zero Meta is valid: the
// runtime fills in a fresh CommandID and OccurredAt=now when they are
// unset.
type Meta struct {
	CommandID     uuid.UUID // this command's id; generated if zero
	CorrelationID uuid.UUID // originating flow; defaults to CommandID if zero
	CausationID   uuid.UUID // the cause of this command (e.g. a prior event id)
	Actor         Actor     // who issued the command
	OccurredAt    time.Time // domain time; defaults to now if zero
}
