package natsstore

import (
	"context"
	"encoding/json"
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

// The authoritative workspace comes from natskit's identity system
// (natskit.Identity / WorkspaceFrom), set by the Tenant middleware the Server
// installs — see Serve. es-lite no longer carries its own workspace context
// key; the cross-check is natskit.RequireSubjectWorkspace (architecture ADR 0005).

// Server serves an es.Store over NATS as a natskit micro service, so it
// self-registers for discovery, versioning, stats, and health
// (`nats micro ls/info/stats`). It holds no keys — in the zero-knowledge
// deployment the client encrypts, so the Server only sees ciphertext.
type Server struct {
	scope      Scoper
	prefix     string
	version    string
	middleware []natskit.Middleware
	identity   natskit.IdentityExtractor // how the authoritative workspace is derived
	deks       WrappedDEKStore           // optional; exposes the wrapped-DEK RPCs (ADR 0014 C)
}

// WrappedDEKStore persists one OPAQUE wrapped DEK per workspace (architecture ADR 0014 C).
// es-lited's backing store (postgres.Store) satisfies it; the Server never unwraps — it can't,
// holding no KEK — so serving the wrapped DEK keeps es-lited zero-knowledge.
type WrappedDEKStore interface {
	LoadWrappedDEK(ctx context.Context, workspaceID string) (wrapped []byte, ok bool, err error)
	SaveWrappedDEK(ctx context.Context, workspaceID string, wrapped []byte, kekVersion int) error
	DeleteWrappedDEK(ctx context.Context, workspaceID string) error
}

// WithWrappedDEKStore serves the wrapped-DEK load/save/delete RPCs backed by d. Omit it (the
// default) and those endpoints are simply not registered.
func WithWrappedDEKStore(d WrappedDEKStore) ServerOption { return func(s *Server) { s.deks = d } }

// ServerOption configures a Server.
type ServerOption func(*Server)

// WithServerPrefix overrides the subject prefix (default "svc.eslite").
func WithServerPrefix(p string) ServerOption { return func(s *Server) { s.prefix = p } }

// WithServerVersion sets the service version (semver; default "0.1.0").
func WithServerVersion(v string) ServerOption { return func(s *Server) { s.version = v } }

// WithMiddleware installs additional natskit middleware on every endpoint
// (e.g. observability, PDP authorization, audit). It runs AFTER the identity/
// tenant middleware the Server installs.
func WithMiddleware(mw ...natskit.Middleware) ServerOption {
	return func(s *Server) { s.middleware = append(s.middleware, mw...) }
}

// WithIdentity sets how the authoritative workspace (and principal) is derived
// for each request — a natskit.IdentityExtractor. The default reads the
// workspace from the subject's ws token: trusted when the connection's creds
// are subject-scoped per workspace by natsauthd (the standard deployment). For
// edge/callout-stamped-header deployments, pass
// natskit.HeaderIdentity("X-Workspace", …) — the Server always additionally
// enforces subject==identity via RequireSubjectWorkspace (ADR 0005 §E).
func WithIdentity(extract natskit.IdentityExtractor) ServerOption {
	return func(s *Server) { s.identity = extract }
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
	// Subjects are <prefix>.<ws>.<method>; the ws token sits right after the
	// prefix (architecture ADR 0005). Identity (authoritative workspace) is set
	// by Tenant, then RequireSubjectWorkspace enforces subject==identity
	// fail-closed. These run BEFORE any user middleware.
	wsIndex := wsTokenIndex(s.prefix)
	extract := s.identity
	if extract == nil {
		extract = func(m natskit.MsgContext) (natskit.Identity, error) {
			return natskit.Identity{Workspace: natskit.SubjectToken(m.Subject, wsIndex)}, nil
		}
	}
	mw := append([]natskit.Middleware{
		natskit.Tenant(extract),
		natskit.RequireSubjectWorkspace(wsIndex),
	}, s.middleware...)

	// es-lited is zero-knowledge (clients encrypt event payloads client-side), so the
	// transport codec is Nop — the bus is protected by mTLS + account isolation, and
	// the sensitive bytes are already ciphertext before they arrive.
	conn, err := natskit.Wrap(nc, natskit.NopCodec{})
	if err != nil {
		return err
	}
	svc, err := conn.Service(natskit.ServiceConfig{
		Name:        "eslite",
		Version:     s.version,
		Description: "es-lite eventstore — es.Store over NATS",
		Middleware:  mw,
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
		{mLookupClaim, s.handleLookupClaim},
	}
	if s.deks != nil { // wrapped-DEK RPCs only when a store is wired (ADR 0014 C)
		endpoints = append(endpoints,
			struct {
				method  string
				handler natskit.SvcHandler
			}{mLoadWrappedDEK, s.handleLoadWrappedDEK},
			struct {
				method  string
				handler natskit.SvcHandler
			}{mSaveWrappedDEK, s.handleSaveWrappedDEK},
			struct {
				method  string
				handler natskit.SvcHandler
			}{mDeleteWrappedDEK, s.handleDeleteWrappedDEK},
		)
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

// wsTokenIndex is the 0-based subject index of the ws token, i.e. the token
// right after the prefix in <prefix>.<ws>.<method>. Prefix "svc.eslite.eu1"
// (3 tokens) ⇒ ws at index 3.
func wsTokenIndex(prefix string) int { return strings.Count(prefix, ".") + 1 }

func (s *Server) handleAppend(ctx context.Context, m natskit.MsgContext) ([]byte, error) {
	var req appendReq
	if err := json.Unmarshal(m.Data, &req); err != nil {
		return json.Marshal(appendResp{ErrKind: "internal", ErrMsg: "bad request: " + err.Error()})
	}
	sid, err := es.NewStreamID(req.StreamType, req.StreamID)
	if err != nil {
		return json.Marshal(appendResp{ErrKind: "invalid_stream", ErrMsg: err.Error()})
	}
	res, err := s.scope(natskit.WorkspaceFrom(ctx)).Append(ctx, es.AppendParams{
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
	store := s.scope(natskit.WorkspaceFrom(ctx))
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
	envs, err := s.scope(natskit.WorkspaceFrom(ctx)).ReadAll(ctx, req.FromPosition, req.Limit)
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
	v, err := s.scope(natskit.WorkspaceFrom(ctx)).CurrentStreamVersion(ctx, sid)
	kind, msg := errKind(err)
	return json.Marshal(versionResp{ErrKind: kind, ErrMsg: msg, Version: v})
}

func (s *Server) handleLookupClaim(ctx context.Context, m natskit.MsgContext) ([]byte, error) {
	var req lookupClaimReq
	if err := json.Unmarshal(m.Data, &req); err != nil {
		return json.Marshal(lookupClaimResp{ErrKind: "internal", ErrMsg: err.Error()})
	}
	streamID, found, err := s.scope(natskit.WorkspaceFrom(ctx)).LookupClaim(ctx, req.Scope, req.Value, req.PII)
	kind, msg := errKind(err)
	return json.Marshal(lookupClaimResp{ErrKind: kind, ErrMsg: msg, StreamID: streamID, Found: found})
}

// handleLoadWrappedDEK returns the workspace's opaque wrapped DEK (ADR 0014 C). The workspace
// is the authenticated one (subject/identity), never client-supplied.
func (s *Server) handleLoadWrappedDEK(ctx context.Context, _ natskit.MsgContext) ([]byte, error) {
	wrapped, ok, err := s.deks.LoadWrappedDEK(ctx, natskit.WorkspaceFrom(ctx))
	kind, msg := errKind(err)
	return json.Marshal(loadWrappedDEKResp{Wrapped: wrapped, Found: ok, ErrKind: kind, ErrMsg: msg})
}

// handleSaveWrappedDEK persists the workspace's wrapped DEK (insert-if-absent, per the store).
func (s *Server) handleSaveWrappedDEK(ctx context.Context, m natskit.MsgContext) ([]byte, error) {
	var req saveWrappedDEKReq
	if err := json.Unmarshal(m.Data, &req); err != nil {
		return json.Marshal(errResp{ErrKind: "internal", ErrMsg: err.Error()})
	}
	err := s.deks.SaveWrappedDEK(ctx, natskit.WorkspaceFrom(ctx), req.Wrapped, req.KEKVersion)
	kind, msg := errKind(err)
	return json.Marshal(errResp{ErrKind: kind, ErrMsg: msg})
}

// handleDeleteWrappedDEK drops the workspace's wrapped DEK (crypto-shred by locality).
func (s *Server) handleDeleteWrappedDEK(ctx context.Context, _ natskit.MsgContext) ([]byte, error) {
	err := s.deks.DeleteWrappedDEK(ctx, natskit.WorkspaceFrom(ctx))
	kind, msg := errKind(err)
	return json.Marshal(errResp{ErrKind: kind, ErrMsg: msg})
}
