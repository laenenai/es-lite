package natsstore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/laenenai/es-lite/es"
)

// Client turns NATS into an es.Store backend. Client.Workspace(ws) returns an
// es.Store that RPCs the Server. Wrap it in cryptostore for the zero-knowledge
// deployment, then hand it to aggregate.NewRuntime unchanged.
type Client struct {
	nc      *nats.Conn
	prefix  string
	timeout time.Duration
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithClientPrefix overrides the subject prefix (default "svc.eslite").
func WithClientPrefix(p string) ClientOption { return func(c *Client) { c.prefix = p } }

// WithClientTimeout sets the per-request timeout (default 5s).
func WithClientTimeout(d time.Duration) ClientOption { return func(c *Client) { c.timeout = d } }

// NewClient wraps a NATS connection.
func NewClient(nc *nats.Conn, opts ...ClientOption) *Client {
	c := &Client{nc: nc, prefix: DefaultPrefix, timeout: defaultTimeout}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Workspace returns an es.Store scoped to a workspace.
func (c *Client) Workspace(workspace string) es.Store {
	return &clientStore{c: c, ws: workspace}
}

type clientStore struct {
	c  *Client
	ws string
}

var _ es.Store = (*clientStore)(nil)

func (cs *clientStore) request(ctx context.Context, method string, req, resp any) error {
	b, err := json.Marshal(req)
	if err != nil {
		return err
	}
	if cs.c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cs.c.timeout)
		defer cancel()
	}
	msg, err := cs.c.nc.RequestWithContext(ctx, cs.c.prefix+"."+method, b)
	if err != nil {
		return fmt.Errorf("natsstore: request %s: %w", method, err)
	}
	return json.Unmarshal(msg.Data, resp)
}

func (cs *clientStore) Append(ctx context.Context, p es.AppendParams) (es.AppendResult, error) {
	req := appendReq{
		Workspace:       cs.ws,
		StreamType:      p.StreamID.Type,
		StreamID:        p.StreamID.ID,
		ExpectedVersion: p.ExpectedVersion,
		Events:          toEventDataWire(p.Events),
		Constraints:     toConstraintsWire(p.Constraints),
		CommandID:       uuidStr(p.CommandID),
		CorrelationID:   uuidStr(p.CorrelationID),
		CausationID:     uuidStr(p.CausationID),
		ActorType:       p.Actor.Type,
		ActorID:         p.Actor.ID,
		OccurredAt:      fmtTS(p.OccurredAt),
	}
	var resp appendResp
	if err := cs.request(ctx, mAppend, req, &resp); err != nil {
		return es.AppendResult{}, err
	}
	if resp.ErrKind != "" {
		return es.AppendResult{}, errFromKind(resp.ErrKind, resp.ErrMsg)
	}
	envs, err := fromEnvelopesWire(resp.Envelopes)
	if err != nil {
		return es.AppendResult{}, err
	}
	return es.AppendResult{FromVersion: resp.FromVersion, ToVersion: resp.ToVersion, Envelopes: envs}, nil
}

func (cs *clientStore) ReadStream(ctx context.Context, sid es.StreamID, fromVersion, toVersion uint64) ([]es.Envelope, error) {
	return cs.read(ctx, mReadStream, readStreamReq{
		Workspace: cs.ws, StreamType: sid.Type, StreamID: sid.ID,
		FromVersion: fromVersion, ToVersion: toVersion,
	})
}

func (cs *clientStore) ReadStreamAsOf(ctx context.Context, sid es.StreamID, asOf time.Time) ([]es.Envelope, error) {
	return cs.read(ctx, mReadStreamAsOf, readStreamReq{
		Workspace: cs.ws, StreamType: sid.Type, StreamID: sid.ID, AsOf: fmtTS(asOf),
	})
}

func (cs *clientStore) ReadAll(ctx context.Context, fromPosition uint64, limit int) ([]es.Envelope, error) {
	var resp readResp
	if err := cs.request(ctx, mReadAll, readAllReq{Workspace: cs.ws, FromPosition: fromPosition, Limit: limit}, &resp); err != nil {
		return nil, err
	}
	if resp.ErrKind != "" {
		return nil, errFromKind(resp.ErrKind, resp.ErrMsg)
	}
	return fromEnvelopesWire(resp.Envelopes)
}

func (cs *clientStore) CurrentStreamVersion(ctx context.Context, sid es.StreamID) (uint64, error) {
	var resp versionResp
	if err := cs.request(ctx, mCurrentVersion, currentVersionReq{Workspace: cs.ws, StreamType: sid.Type, StreamID: sid.ID}, &resp); err != nil {
		return 0, err
	}
	if resp.ErrKind != "" {
		return 0, errFromKind(resp.ErrKind, resp.ErrMsg)
	}
	return resp.Version, nil
}

func (cs *clientStore) read(ctx context.Context, method string, req readStreamReq) ([]es.Envelope, error) {
	var resp readResp
	if err := cs.request(ctx, method, req, &resp); err != nil {
		return nil, err
	}
	if resp.ErrKind != "" {
		return nil, errFromKind(resp.ErrKind, resp.ErrMsg)
	}
	return fromEnvelopesWire(resp.Envelopes)
}
