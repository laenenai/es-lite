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

// The Client also serves as a shred.WrappedDEKStore against a WithWrappedDEKStore-enabled
// es-lited (architecture ADR 0014 C): a client-side Shredder persists the workspace's OPAQUE
// wrapped DEK next to its ciphertext, and es-lited never unwraps it.

// LoadWrappedDEK fetches a workspace's wrapped DEK (ok=false if none yet).
func (c *Client) LoadWrappedDEK(ctx context.Context, workspace string) ([]byte, bool, error) {
	var resp loadWrappedDEKResp
	if err := (&clientStore{c: c, ws: workspace}).request(ctx, mLoadWrappedDEK, struct{}{}, &resp); err != nil {
		return nil, false, err
	}
	if resp.ErrKind != "" {
		return nil, false, errFromKind(resp.ErrKind, resp.ErrMsg)
	}
	return resp.Wrapped, resp.Found, nil
}

// SaveWrappedDEK persists a workspace's wrapped DEK (insert-if-absent server-side).
func (c *Client) SaveWrappedDEK(ctx context.Context, workspace string, wrapped []byte, kekVersion int) error {
	var resp errResp
	if err := (&clientStore{c: c, ws: workspace}).request(ctx, mSaveWrappedDEK, saveWrappedDEKReq{Wrapped: wrapped, KEKVersion: kekVersion}, &resp); err != nil {
		return err
	}
	if resp.ErrKind != "" {
		return errFromKind(resp.ErrKind, resp.ErrMsg)
	}
	return nil
}

// DeleteWrappedDEK drops a workspace's wrapped DEK.
func (c *Client) DeleteWrappedDEK(ctx context.Context, workspace string) error {
	var resp errResp
	if err := (&clientStore{c: c, ws: workspace}).request(ctx, mDeleteWrappedDEK, struct{}{}, &resp); err != nil {
		return err
	}
	if resp.ErrKind != "" {
		return errFromKind(resp.ErrKind, resp.ErrMsg)
	}
	return nil
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
	// Subject carries the workspace: <prefix>.<ws>.<method> (architecture
	// ADR 0005). The server derives the workspace from the subject.
	msg, err := cs.c.nc.RequestWithContext(ctx, cs.c.prefix+"."+cs.ws+"."+method, b)
	if err != nil {
		return fmt.Errorf("natsstore: request %s: %w", method, err)
	}
	// A NATS Micro error (e.g. the fail-closed identity/tenant middleware
	// rejecting the request) rides in headers with no body.
	if e := msg.Header.Get("Nats-Service-Error"); e != "" {
		return fmt.Errorf("natsstore: %s (%s)", e, msg.Header.Get("Nats-Service-Error-Code"))
	}
	return json.Unmarshal(msg.Data, resp)
}

func (cs *clientStore) Append(ctx context.Context, p es.AppendParams) (es.AppendResult, error) {
	req := appendReq{
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
		StreamType: sid.Type, StreamID: sid.ID,
		FromVersion: fromVersion, ToVersion: toVersion,
	})
}

func (cs *clientStore) ReadStreamAsOf(ctx context.Context, sid es.StreamID, asOf time.Time) ([]es.Envelope, error) {
	return cs.read(ctx, mReadStreamAsOf, readStreamReq{
		StreamType: sid.Type, StreamID: sid.ID, AsOf: fmtTS(asOf),
	})
}

func (cs *clientStore) ReadAll(ctx context.Context, fromPosition uint64, limit int) ([]es.Envelope, error) {
	var resp readResp
	if err := cs.request(ctx, mReadAll, readAllReq{FromPosition: fromPosition, Limit: limit}, &resp); err != nil {
		return nil, err
	}
	if resp.ErrKind != "" {
		return nil, errFromKind(resp.ErrKind, resp.ErrMsg)
	}
	return fromEnvelopesWire(resp.Envelopes)
}

func (cs *clientStore) CurrentStreamVersion(ctx context.Context, sid es.StreamID) (uint64, error) {
	var resp versionResp
	if err := cs.request(ctx, mCurrentVersion, currentVersionReq{StreamType: sid.Type, StreamID: sid.ID}, &resp); err != nil {
		return 0, err
	}
	if resp.ErrKind != "" {
		return 0, errFromKind(resp.ErrKind, resp.ErrMsg)
	}
	return resp.Version, nil
}

func (cs *clientStore) LookupClaim(ctx context.Context, scope, value string, pii bool) (string, bool, error) {
	var resp lookupClaimResp
	if err := cs.request(ctx, mLookupClaim, lookupClaimReq{Scope: scope, Value: value, PII: pii}, &resp); err != nil {
		return "", false, err
	}
	if resp.ErrKind != "" {
		return "", false, errFromKind(resp.ErrKind, resp.ErrMsg)
	}
	return resp.StreamID, resp.Found, nil
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
