package natsstore

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go"

	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/natskit"
)

// Scoper returns an es.Store scoped to a workspace. postgres.Store.Workspace
// satisfies it. It is also the sharding/residency routing seam (ADR 0009): an
// implementation may route different workspaces to different physical backends
// or regional cells.
type Scoper func(workspace string) es.Store

// wsKey carries the authenticated workspace in context. An auth middleware
// (natskit.Middleware) sets it via WithWorkspace; handlers prefer it over the
// client-supplied field, so production never trusts a client's workspace
// claim (ADR 0009). Left unset in tests, handlers fall back to the request.
type wsKey struct{}

// WithWorkspace returns ctx carrying an authenticated workspace. Call it from
// an identity middleware after verifying the caller's token.
func WithWorkspace(ctx context.Context, workspace string) context.Context {
	return context.WithValue(ctx, wsKey{}, workspace)
}

// WorkspaceFrom returns the authenticated workspace in ctx, if any.
func WorkspaceFrom(ctx context.Context) (string, bool) {
	ws, ok := ctx.Value(wsKey{}).(string)
	return ws, ok && ws != ""
}

// Server serves an es.Store over NATS as a natskit micro service, so it
// self-registers for discovery, versioning, stats, and health
// (`nats micro ls/info/stats`). It holds no keys — in the zero-knowledge
// deployment the client encrypts, so the Server only sees ciphertext.
type Server struct {
	scope      Scoper
	prefix     string
	version    string
	middleware []natskit.Middleware
}

// ServerOption configures a Server.
type ServerOption func(*Server)

// WithServerPrefix overrides the subject prefix (default "svc.eslite").
func WithServerPrefix(p string) ServerOption { return func(s *Server) { s.prefix = p } }

// WithServerVersion sets the service version (semver; default "0.1.0").
func WithServerVersion(v string) ServerOption { return func(s *Server) { s.version = v } }

// WithMiddleware installs natskit middleware on every endpoint — e.g. an
// identity middleware that verifies the caller's token and calls WithWorkspace.
func WithMiddleware(mw ...natskit.Middleware) ServerOption {
	return func(s *Server) { s.middleware = append(s.middleware, mw...) }
}

// NewServer builds a Server over a workspace Scoper (e.g. postgres.Store.Workspace).
func NewServer(scope Scoper, opts ...ServerOption) *Server {
	s := &Server{scope: scope, prefix: DefaultPrefix, version: "0.1.0"}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Serve registers the micro service and its endpoints, then blocks until ctx
// is cancelled.
func (s *Server) Serve(ctx context.Context, nc *nats.Conn) error {
	svc, err := natskit.NewService(nc, natskit.ServiceConfig{
		Name:        "eslite",
		Version:     s.version,
		Description: "es-lite eventstore — es.Store over NATS",
		Middleware:  s.middleware,
	})
	if err != nil {
		return err
	}
	endpoints := []struct {
		method  string
		handler natskit.SvcHandler
	}{
		{mAppend, s.handleAppend},
		{mReadStream, s.handleReadStream},
		{mReadStreamAsOf, s.handleReadStream}, // dispatched by AsOf on the request
		{mReadAll, s.handleReadAll},
		{mCurrentVersion, s.handleCurrentVersion},
	}
	for _, e := range endpoints {
		// Subscribe on a workspace wildcard: subjects are
		// <prefix>.<ws>.<method> (architecture ADR 0005), and the handler
		// derives the workspace from the actual subject.
		if err := svc.Endpoint(e.method, s.prefix+".*."+e.method, e.handler); err != nil {
			_ = svc.Stop()
			return err
		}
	}
	<-ctx.Done()
	return svc.Stop()
}

// resolveWorkspace derives the workspace from the message SUBJECT (the token
// before the method), and cross-checks it against the authenticated workspace
// (architecture ADR 0005 §D). Auth is authoritative: when an authenticated ws
// is present in ctx (set by an identity/Tenant middleware, e.g. natsauthd), a
// subject that names a different ws is REJECTED fail-closed — never silently
// trusted, never silently fallen back. When no auth ws is present (pre-callout
// dev/tests), the subject ws is used; production must wire the auth middleware.
// The service never reads a workspace from the request body.
func (s *Server) resolveWorkspace(ctx context.Context, subject string) (string, error) {
	toks := strings.Split(subject, ".")
	if len(toks) < 3 {
		return "", fmt.Errorf("subject %q has no workspace token", subject)
	}
	subjectWS := toks[len(toks)-2]
	if subjectWS == "" || subjectWS == "*" || subjectWS == ">" {
		return "", fmt.Errorf("subject %q has an invalid workspace token", subject)
	}
	if authWS, ok := WorkspaceFrom(ctx); ok {
		if authWS != subjectWS {
			return "", fmt.Errorf("workspace mismatch: subject=%q auth=%q (fail-closed)", subjectWS, authWS)
		}
		return authWS, nil // authenticated ws is authoritative
	}
	return subjectWS, nil
}

func (s *Server) handleAppend(ctx context.Context, m natskit.MsgContext) ([]byte, error) {
	var req appendReq
	if err := json.Unmarshal(m.Data, &req); err != nil {
		return json.Marshal(appendResp{ErrKind: "internal", ErrMsg: "bad request: " + err.Error()})
	}
	sid, err := es.NewStreamID(req.StreamType, req.StreamID)
	if err != nil {
		return json.Marshal(appendResp{ErrKind: "invalid_stream", ErrMsg: err.Error()})
	}
	ws, err := s.resolveWorkspace(ctx, m.Subject)
	if err != nil {
		return json.Marshal(appendResp{ErrKind: "workspace", ErrMsg: err.Error()})
	}
	res, err := s.scope(ws).Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: req.ExpectedVersion,
		Events:          fromEventDataWire(req.Events),
		Constraints:     fromConstraintsWire(req.Constraints),
		CommandID:       parseUUID(req.CommandID),
		CorrelationID:   parseUUID(req.CorrelationID),
		CausationID:     parseUUID(req.CausationID),
		Actor:           es.Actor{Type: req.ActorType, ID: req.ActorID},
		OccurredAt:      parseTS(req.OccurredAt),
	})
	kind, msg := errKind(err)
	resp := appendResp{ErrKind: kind, ErrMsg: msg}
	if err == nil {
		resp.FromVersion, resp.ToVersion = res.FromVersion, res.ToVersion
		resp.Envelopes = toEnvelopesWire(res.Envelopes)
	}
	return json.Marshal(resp)
}

func (s *Server) handleReadStream(ctx context.Context, m natskit.MsgContext) ([]byte, error) {
	var req readStreamReq
	if err := json.Unmarshal(m.Data, &req); err != nil {
		return json.Marshal(readResp{ErrKind: "internal", ErrMsg: err.Error()})
	}
	sid, err := es.NewStreamID(req.StreamType, req.StreamID)
	if err != nil {
		return json.Marshal(readResp{ErrKind: "invalid_stream", ErrMsg: err.Error()})
	}
	ws, err := s.resolveWorkspace(ctx, m.Subject)
	if err != nil {
		return json.Marshal(readResp{ErrKind: "workspace", ErrMsg: err.Error()})
	}
	store := s.scope(ws)
	var envs []es.Envelope
	if req.AsOf != "" {
		envs, err = store.ReadStreamAsOf(ctx, sid, parseTS(req.AsOf))
	} else {
		envs, err = store.ReadStream(ctx, sid, req.FromVersion, req.ToVersion)
	}
	kind, msg := errKind(err)
	resp := readResp{ErrKind: kind, ErrMsg: msg}
	if err == nil {
		resp.Envelopes = toEnvelopesWire(envs)
	}
	return json.Marshal(resp)
}

func (s *Server) handleReadAll(ctx context.Context, m natskit.MsgContext) ([]byte, error) {
	var req readAllReq
	if err := json.Unmarshal(m.Data, &req); err != nil {
		return json.Marshal(readResp{ErrKind: "internal", ErrMsg: err.Error()})
	}
	ws, err := s.resolveWorkspace(ctx, m.Subject)
	if err != nil {
		return json.Marshal(readResp{ErrKind: "workspace", ErrMsg: err.Error()})
	}
	envs, err := s.scope(ws).ReadAll(ctx, req.FromPosition, req.Limit)
	kind, msg := errKind(err)
	resp := readResp{ErrKind: kind, ErrMsg: msg}
	if err == nil {
		resp.Envelopes = toEnvelopesWire(envs)
	}
	return json.Marshal(resp)
}

func (s *Server) handleCurrentVersion(ctx context.Context, m natskit.MsgContext) ([]byte, error) {
	var req currentVersionReq
	if err := json.Unmarshal(m.Data, &req); err != nil {
		return json.Marshal(versionResp{ErrKind: "internal", ErrMsg: err.Error()})
	}
	sid, err := es.NewStreamID(req.StreamType, req.StreamID)
	if err != nil {
		return json.Marshal(versionResp{ErrKind: "invalid_stream", ErrMsg: err.Error()})
	}
	ws, err := s.resolveWorkspace(ctx, m.Subject)
	if err != nil {
		return json.Marshal(versionResp{ErrKind: "workspace", ErrMsg: err.Error()})
	}
	v, err := s.scope(ws).CurrentStreamVersion(ctx, sid)
	kind, msg := errKind(err)
	return json.Marshal(versionResp{ErrKind: kind, ErrMsg: msg, Version: v})
}
