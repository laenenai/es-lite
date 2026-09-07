package natsstore

import (
	"context"
	"errors"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

// This file is the local replacement for natskit's identity/middleware layer
// (architecture ADR 0005 §E, ADR 0009): a small NATS Micro request pipeline
// with an authoritative-workspace concept threaded through context.Context,
// built directly on nats-io/nats.go's public micro package instead of the
// private natskit module. Behavior (subject layout, fail-closed
// subject==identity cross-check, wire-compatible Nats-Service-Error headers)
// is unchanged; only the implementation moved in-repo.

// Identity is the authoritative principal derived for a request. Only
// Workspace is used today; es-lite carries no other identity fields.
type Identity struct {
	Workspace string
}

// MsgContext carries the inbound request fields middleware and handlers need.
type MsgContext struct {
	Subject string
	Data    []byte
	Header  nats.Header
}

// SvcHandler handles one endpoint request and returns the (already-encoded)
// response body, or an error to surface as a NATS Micro protocol error
// response (Nats-Service-Error / Nats-Service-Error-Code headers).
type SvcHandler func(ctx context.Context, m MsgContext) ([]byte, error)

// Middleware wraps a SvcHandler with cross-cutting behavior. Middleware run
// in the order passed to WithMiddleware, outermost first.
type Middleware func(SvcHandler) SvcHandler

// IdentityExtractor derives the authoritative Identity for a request.
type IdentityExtractor func(MsgContext) (Identity, error)

// HeaderIdentity returns an IdentityExtractor that reads the workspace from
// the named NATS message header, for edge/callout deployments that stamp the
// authoritative workspace onto the request instead of relying on the
// subject's ws token.
func HeaderIdentity(header string) IdentityExtractor {
	return func(m MsgContext) (Identity, error) {
		return Identity{Workspace: m.Header.Get(header)}, nil
	}
}

type identityKey struct{}

// Tenant installs the Identity extract derives into the request context, so
// downstream handlers (via WorkspaceFrom) and RequireSubjectWorkspace can read
// it. Fails closed: an extractor error short-circuits the request before the
// endpoint handler runs.
func Tenant(extract IdentityExtractor) Middleware {
	return func(next SvcHandler) SvcHandler {
		return func(ctx context.Context, m MsgContext) ([]byte, error) {
			id, err := extract(m)
			if err != nil {
				return nil, err
			}
			return next(context.WithValue(ctx, identityKey{}, id), m)
		}
	}
}

// WorkspaceFrom returns the authoritative workspace Tenant installed into
// ctx, or "" if none was installed.
func WorkspaceFrom(ctx context.Context) string {
	id, _ := ctx.Value(identityKey{}).(Identity)
	return id.Workspace
}

// RequireSubjectWorkspace re-derives the workspace from the subject's ws
// token (index wsIndex) and rejects the request if it doesn't match the
// identity Tenant installed — the fail-closed subject==identity cross-check
// (architecture ADR 0005 §E).
func RequireSubjectWorkspace(wsIndex int) Middleware {
	return func(next SvcHandler) SvcHandler {
		return func(ctx context.Context, m MsgContext) ([]byte, error) {
			subjWS := SubjectToken(m.Subject, wsIndex)
			if subjWS == "" || subjWS != WorkspaceFrom(ctx) {
				return nil, errors.New("natsstore: subject workspace does not match identity")
			}
			return next(ctx, m)
		}
	}
}

// SubjectToken returns the dot-delimited token at idx in subject, or "" if
// idx is out of range.
func SubjectToken(subject string, idx int) string {
	toks := strings.Split(subject, ".")
	if idx < 0 || idx >= len(toks) {
		return ""
	}
	return toks[idx]
}

// chain composes middleware around base in the order given (mw[0] runs
// outermost, i.e. first).
func chain(base SvcHandler, mw ...Middleware) SvcHandler {
	h := base
	for i := len(mw) - 1; i >= 0; i-- {
		h = mw[i](h)
	}
	return h
}

// asMicroHandler adapts a SvcHandler to micro.Handler: ctx is the Serve-level
// context (so in-flight requests observe shutdown the same way natskit's
// wrapper did), and a non-nil handler error becomes a NATS Micro error
// response instead of a normal reply.
func asMicroHandler(ctx context.Context, h SvcHandler) micro.HandlerFunc {
	return func(req micro.Request) {
		mc := MsgContext{
			Subject: req.Subject(),
			Data:    req.Data(),
			Header:  nats.Header(req.Headers()),
		}
		b, err := h(ctx, mc)
		if err != nil {
			_ = req.Error("internal", err.Error(), nil)
			return
		}
		_ = req.Respond(b)
	}
}
