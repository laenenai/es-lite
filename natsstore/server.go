package natsstore

import (
	"context"
	"encoding/json"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/laenenai/es-lite/es"
)

// Scoper returns an es.Store scoped to a workspace. postgres.Store.Workspace
// satisfies it. It is also the sharding seam: an implementation may route
// different workspaces to different physical backends (ADR 0009). The Server
// derives the workspace from the request today; production overrides it from
// the caller's authenticated JWT (never trust a client-supplied workspace).
type Scoper func(workspace string) es.Store

// Server serves an es.Store over NATS request/reply. It moves opaque
// envelopes and holds no keys — in the zero-knowledge deployment the client
// encrypts before calling, so the Server only ever sees ciphertext.
type Server struct {
	scope   Scoper
	prefix  string
	queue   string
	timeout time.Duration
}

// ServerOption configures a Server.
type ServerOption func(*Server)

// WithServerPrefix overrides the subject prefix (default "svc.eslite").
func WithServerPrefix(p string) ServerOption { return func(s *Server) { s.prefix = p } }

// WithServerQueue overrides the queue group (default "eslite").
func WithServerQueue(q string) ServerOption { return func(s *Server) { s.queue = q } }

// NewServer builds a Server over a workspace Scoper (e.g. postgres.Store.Workspace).
func NewServer(scope Scoper, opts ...ServerOption) *Server {
	s := &Server{scope: scope, prefix: DefaultPrefix, queue: DefaultQueue, timeout: 30 * time.Second}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Serve subscribes to the request subjects (queue group) and blocks until ctx
// is cancelled.
func (s *Server) Serve(ctx context.Context, nc *nats.Conn) error {
	handlers := map[string]nats.MsgHandler{
		mAppend:         s.handleAppend,
		mReadStream:     s.handleReadStream,
		mReadStreamAsOf: s.handleReadStream, // dispatched by AsOf on the request
		mReadAll:        s.handleReadAll,
		mCurrentVersion: s.handleCurrentVersion,
	}
	subs := make([]*nats.Subscription, 0, len(handlers))
	for m, h := range handlers {
		sub, err := nc.QueueSubscribe(s.prefix+"."+m, s.queue, h)
		if err != nil {
			return err
		}
		subs = append(subs, sub)
	}
	if err := nc.Flush(); err != nil {
		return err
	}
	<-ctx.Done()
	for _, sub := range subs {
		_ = sub.Unsubscribe()
	}
	return ctx.Err()
}

func (s *Server) reqCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), s.timeout)
}

func respond(m *nats.Msg, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	_ = m.Respond(b)
}

func (s *Server) handleAppend(m *nats.Msg) {
	var req appendReq
	if err := json.Unmarshal(m.Data, &req); err != nil {
		respond(m, appendResp{ErrKind: "internal", ErrMsg: "bad request: " + err.Error()})
		return
	}
	sid, err := es.NewStreamID(req.StreamType, req.StreamID)
	if err != nil {
		respond(m, appendResp{ErrKind: "invalid_stream", ErrMsg: err.Error()})
		return
	}
	ctx, cancel := s.reqCtx()
	defer cancel()
	res, err := s.scope(req.Workspace).Append(ctx, es.AppendParams{
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
	respond(m, resp)
}

func (s *Server) handleReadStream(m *nats.Msg) {
	var req readStreamReq
	if err := json.Unmarshal(m.Data, &req); err != nil {
		respond(m, readResp{ErrKind: "internal", ErrMsg: err.Error()})
		return
	}
	sid, err := es.NewStreamID(req.StreamType, req.StreamID)
	if err != nil {
		respond(m, readResp{ErrKind: "invalid_stream", ErrMsg: err.Error()})
		return
	}
	ctx, cancel := s.reqCtx()
	defer cancel()
	store := s.scope(req.Workspace)
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
	respond(m, resp)
}

func (s *Server) handleReadAll(m *nats.Msg) {
	var req readAllReq
	if err := json.Unmarshal(m.Data, &req); err != nil {
		respond(m, readResp{ErrKind: "internal", ErrMsg: err.Error()})
		return
	}
	ctx, cancel := s.reqCtx()
	defer cancel()
	envs, err := s.scope(req.Workspace).ReadAll(ctx, req.FromPosition, req.Limit)
	kind, msg := errKind(err)
	resp := readResp{ErrKind: kind, ErrMsg: msg}
	if err == nil {
		resp.Envelopes = toEnvelopesWire(envs)
	}
	respond(m, resp)
}

func (s *Server) handleCurrentVersion(m *nats.Msg) {
	var req currentVersionReq
	if err := json.Unmarshal(m.Data, &req); err != nil {
		respond(m, versionResp{ErrKind: "internal", ErrMsg: err.Error()})
		return
	}
	sid, err := es.NewStreamID(req.StreamType, req.StreamID)
	if err != nil {
		respond(m, versionResp{ErrKind: "invalid_stream", ErrMsg: err.Error()})
		return
	}
	ctx, cancel := s.reqCtx()
	defer cancel()
	v, err := s.scope(req.Workspace).CurrentStreamVersion(ctx, sid)
	kind, msg := errKind(err)
	respond(m, versionResp{ErrKind: kind, ErrMsg: msg, Version: v})
}
