package natsjs

import (
	"github.com/nats-io/nats.go"

	"github.com/laenenai/es-lite/es"
)

// MsgFor exposes toMsg to the external test package.
func MsgFor(subject string, e es.Envelope) *nats.Msg { return toMsg(subject, e) }
