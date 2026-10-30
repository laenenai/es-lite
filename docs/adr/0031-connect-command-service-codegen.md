# ADR 0031: Connect Command Service Codegen

- **Status:** Proposed
- **Date:** 2026-10-30

## Context

`contracts` serves commands over Connect (design §9.11, convention 31): plain
proto `service` definitions in `ont/api/<service>/v1`, `protoc-gen-connect-go`
run in consuming repos, every command response carrying
`WriteResult { stream_id, version, global_position }` plus the
clearance-filtered post-append state, and a stale `expected_version` surfacing
as a Connect `failed_precondition` error with a `VersionConflict` detail
(`expected_version`, `current_version`, `current_state`).

Every command handler in that design does the same seven steps: validate →
edge-authorise → stamp the acting principal from the authenticated identity
(never from the client, §3.5) → set `Meta.OccurredAt` from `decided_at` →
`Runtime.Handle` → build the response from `Result` → map errors. §3.5 also
makes two of those steps security-relevant: a handler that forgets to
overwrite `ActingPrincipal` accepts an impersonated principal.

What es-lite provides today: `aggregate.Runtime.Handle(ctx, sid, cmd, meta)
(Result[S,E], error)` whose `Result` already holds `State`, `ToVersion` and
`Envelopes` (the last one's `GlobalPosition`); typed sentinels `es.ErrConflict`,
`es.ErrTerminal`, `es.ErrConstraintViolated`; and a plugin that walks message
options. It has no HTTP/Connect surface and no notion of a principal beyond
`es.Actor{Type, ID}`.

## Decision

### 1. The service proto is hand-written in `contracts`; es-lite generates the Go handler

Generating a `.proto` from a `.proto` is rejected: buf would need a two-pass
build, the generated file would have to be committed and linted, and §9.11
already places services in `ont/api/<service>/v1` as authored contracts. So:

- **`contracts` writes** `ont/api/party/v1/space_command_service.proto`:
  ```
  service SpaceCommandService {
    option (es.v1.command_service) = { aggregate: "ont.domain.party.space.v1alpha1.Space" };
    rpc CreateSpace(CreateSpaceRequest) returns (CreateSpaceResponse);
    rpc AddMember(AddMemberRequest)   returns (AddMemberResponse);
    …
  }
  message AddMemberRequest  { string space_id = 1 [(es.v1.stream_id) = true];
                              ont.domain.party.space.v1alpha1.AddMember command = 2; }
  message AddMemberResponse { ont.api.common.v1.WriteResult result = 1;
                              ont.domain.party.space.v1alpha1.Space state = 2; }
  ```
  `protoc-gen-go` and `protoc-gen-connect-go` produce the message types and
  the `SpaceCommandServiceHandler` interface as usual.
- **`protoc-gen-es-lite` generates** `space_command_service_es.pb.go` next to
  them: a struct implementing that Connect handler interface, plus the
  hooks the application implements. Nothing is generated for a service without
  `(es.v1.command_service)`.

New options in `proto/es/v1/options.proto`:

```
message CommandService { string aggregate = 1; }   // full name of the State message
extend google.protobuf.ServiceOptions { CommandService command_service = 50020; }
extend google.protobuf.FieldOptions {
  bool stream_id        = 50021;   // request field holding the stream's ID slug
  bool acting_principal = 50022;   // command field the handler overwrites from the identity
  bool decided_at       = 50023;   // ADR 0032
  bool expected_version = 50024;   // optional uint64 command field for VersionConflict.expected_version
}
```

Per RPC the plugin **requires** (generation error otherwise): exactly one
request field whose message type is a variant of the aggregate package's
`(es.v1.sum_type) = "Command"` container; exactly one `(es.v1.stream_id)`
string field; a response with exactly one field of the State type and one
field named `result` whose type the plugin treats opaquely except for three
fields it sets by name (`stream_id`, `version`, `global_position`). The stream
type comes from the State message's `(es.v1.aggregate).stream_type` (ADR 0030).

### 2. The generated handler

```go
type SpaceCommandServiceDeps struct {
    Runtime   *aggregate.Runtime[*spacev1.Space, spacev1.Command, spacev1.Event]
    Authorize SpaceCommandAuthorizer        // app-implemented edge authorisation
    Identity  cmdsvc.IdentitySource         // app-implemented
    Validate  cmdsvc.Validator              // default: protovalidate
    Filter    func(ctx, *spacev1.Space) *spacev1.Space   // optional clearance filter; identity by default
}
func NewSpaceCommandServiceHandler(d SpaceCommandServiceDeps) partyv1connect.SpaceCommandServiceHandler
type SpaceCommandAuthorizer interface {
    AuthorizeAddMember(ctx context.Context, sid es.StreamID, cmd *spacev1.AddMember) error
    …one method per RPC
}
```

Each generated RPC method, in order:

1. `d.Validate.Validate(req.Msg)` → `connect.CodeInvalidArgument` with the
   protovalidate violations as the error detail.
2. `sid, err := es.NewStreamID("space", req.Msg.GetSpaceId())` →
   `CodeInvalidArgument` on `es.ErrInvalidStreamID`.
3. For a command with an `(es.v1.acting_principal)` field, the generated
   code calls `d.Identity.Principal(ctx)` and **assigns** the result to that
   field, discarding whatever the client sent; the generated
   `IdentitySource` is typed with the field's Go type
   (`*casecommonv1alpha1.ActingPrincipal`) so no reflection is involved. The
   same call yields the `es.Actor` for `Meta`. A command whose aggregate
   package declares any `acting_principal` field but whose RPC request lacks
   an identity source is a generation error — the hook is mandatory.
4. `d.Authorize.AuthorizeAddMember(ctx, sid, cmd)` → `CodePermissionDenied`
   (or the app's own `connect.Error`, passed through).
5. `meta := es.Meta{Actor: actor, CorrelationID: from the
   `Correlation-Id` request header if present, CommandID: from an
   `Idempotency-Key` header if present}`; `OccurredAt` is left zero so ADR
   0032's `DecidedAt` rule fills it from the command.
6. `res, err := d.Runtime.Handle(ctx, sid, cmd, meta)`.
7. Response: `result.stream_id = sid.Canonical()`, `result.version =
   res.ToVersion`, `result.global_position = last envelope's GlobalPosition`
   (0 for a zero-event no-op, which is legal per §3.7), `state =
   d.Filter(ctx, res.State)`.

Error mapping, in a small runtime package `cmdsvc` the generated code imports:

| runtime error | Connect code | detail |
|---|---|---|
| `es.ErrConflict` | `FailedPrecondition` | `VersionConflict{expected_version, current_version, current_state}` |
| `es.ErrTerminal` | `FailedPrecondition` | none; message names the stream |
| `es.ErrConstraintViolated` | `AlreadyExists` | scope in message |
| `es.ErrInvalidStreamID` | `InvalidArgument` | |
| decider rejection (any other error) | `FailedPrecondition` by default; the app may register an `ErrorMapper` for domain sentinels | |

For the conflict detail the generated code reloads via `d.Runtime.Load`
(current version and state) after `ErrConflict`, filters the state, and
attaches the detail with `connect.NewErrorDetail`. `expected_version` is the
version the command carried when the request message has a field tagged
`(es.v1.expected_version)`, otherwise the version `Handle` loaded. The
`VersionConflict` message type itself is es-lite's (`es/v1/cmdsvc.proto`,
`es.v1.VersionConflict{uint64 expected_version; uint64 current_version;
google.protobuf.Any current_state}`) so the mapper is generic; `contracts`'
`ont/api/common/v1.VersionConflict` either aliases it by import or is dropped.

### 3. What stays application-written

The `Authorizer` (cross-aggregate checks such as "submitter is an enabled
member" are edge authorisation by §3.5), the `IdentitySource` (IAM subject →
`ActingPrincipal` + `es.Actor`), the clearance `Filter`, and a domain
`ErrorMapper`. The app also wires the Connect mux
(`partyv1connect.NewSpaceCommandServiceHandler(spacev1es.NewSpaceCommandServiceHandler(deps))`).
Nothing in the command path is hand-written per RPC.

### 4. Dependencies

`cmdsvc` imports `connectrpc.com/connect` and
`github.com/bufbuild/protovalidate-go`. To keep the core module's dependency
graph lite, `cmdsvc` is a **nested Go module** (`cmdsvc/go.mod`, versioned with
the repo); the plugin itself gains no new dependency.

## Consequences

### Positive

- §3.5's two security rules — principal never from the client, `OccurredAt =
  decided_at` — are enforced by generated code, not by review.
- One `Result` → `WriteResult` mapping and one conflict mapping, so every
  service in `contracts` returns the §9.11 shapes identically.
- The app's authorisation hook is per-RPC and typed, so a missing rule is a
  compile error, not a silent allow.

### Negative

- A second plugin output per service proto and a new nested module; the
  `gen/` of the consuming repo now contains Connect stubs, es-lite handlers and
  message types for the same file.
- The response shape convention (one State field, a `result` field) is a
  structural contract between the two repos; the plugin enforces it, but it is
  stricter than §9.11 spells out.
- Conflict responses cost one extra `Load` — acceptable, conflicts are the
  rare path and the UI needs the current state anyway.

## Alternatives Considered

### Generate the service `.proto` from the `Commands` container

Rejected (above): two-pass buf build, committed generated protos, and it would
fix request/response naming es-lite has no standing to dictate in `api/`.

### A generic, reflection-based Connect handler (one endpoint, `Any` command)

Rejected: loses per-RPC typing for authorisation and validation, conflicts
with buf's `RPC_REQUEST_STANDARD_NAME` lint in `api/`, and makes the principal
field assignment reflective.

### Naming convention instead of `(es.v1.acting_principal)` / `(es.v1.stream_id)`

Rejected: convention 29 puts tooling concerns in options; a field named
`acting_principal` that is *not* meant to be overwritten would be silently
clobbered under a naming rule.
