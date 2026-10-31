package main

import (
	"fmt"
	"path"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	esv1 "github.com/laenenai/es-lite/gen/es/v1"
)

// Connect command service codegen (ADR 0031). For a service carrying
// (es.v1.command_service) the plugin emits a struct implementing the
// protoc-gen-connect-go handler interface, with the application-written
// hooks (Authorizer, Identity, Validate, Filter, Errors) in a Deps struct.

const (
	connectPkgPath   = protogen.GoImportPath("connectrpc.com/connect")
	cmdsvcPkgPath    = protogen.GoImportPath("github.com/laenenai/es-lite/cmdsvc")
	aggregatePkgPath = protogen.GoImportPath("github.com/laenenai/es-lite/aggregate")
	protoMsgPkgPath  = protogen.GoImportPath("google.golang.org/protobuf/proto")
)

// svcMethod is one validated RPC of a command service.
type svcMethod struct {
	m         *protogen.Method
	streamID  *protogen.Field // request field tagged (es.v1.stream_id)
	command   *protogen.Field // request field holding the Command variant
	variant   *protogen.Message
	principal *protogen.Field // (es.v1.acting_principal) on the variant, or nil
	expected  *protogen.Field // (es.v1.expected_version) on the variant, or nil
	result    *protogen.Field // response field named "result"
	state     *protogen.Field // response field of the State type
}

// svcSpec is one validated command service.
type svcSpec struct {
	svc       *protogen.Service
	agg       *aggregateInfo // Command container + State
	events    *sumInfo       // the package's Event container (runtime type param)
	methods   []svcMethod
	principal *protogen.Message // the one acting-principal type across methods, or nil
}

func collectServices(f *protogen.File, r *resolver) ([]svcSpec, error) {
	var out []svcSpec
	for _, svc := range f.Services {
		opts := svc.Desc.Options()
		if opts == nil || !proto.HasExtension(opts, esv1.E_CommandService) {
			continue
		}
		cs, _ := proto.GetExtension(opts, esv1.E_CommandService).(*esv1.CommandService)
		where := fmt.Sprintf("%s: (es.v1.command_service)", svc.Desc.FullName())
		spec, err := resolveService(where, svc, cs.GetAggregate(), r)
		if err != nil {
			return nil, err
		}
		out = append(out, spec)
	}
	return out, nil
}

func resolveService(where string, svc *protogen.Service, aggregate string, r *resolver) (svcSpec, error) {
	state, ok := r.msgs[aggregate]
	if !ok || !hasAggregate(state) {
		return svcSpec{}, fmt.Errorf("%s: aggregate %q is not a message carrying (es.v1.aggregate) in this generation run", where, aggregate)
	}
	pkg := state.Desc.ParentFile().Package()
	cmds, ok := r.sums[string(pkg)+"/"+commandSumType]
	if !ok {
		return svcSpec{}, fmt.Errorf("%s: package %s has no (es.v1.sum_type) = %q container", where, pkg, commandSumType)
	}
	events, ok := r.sums[string(pkg)+"/"+eventSumType]
	if !ok {
		return svcSpec{}, fmt.Errorf("%s: package %s has no (es.v1.sum_type) = %q container", where, pkg, eventSumType)
	}
	aggOpt, _ := proto.GetExtension(state.Desc.Options(), esv1.E_Aggregate).(*esv1.Aggregate)
	if aggOpt.GetStreamType() == "" {
		return svcSpec{}, fmt.Errorf("%s: %s: (es.v1.aggregate).stream_type is empty", where, aggregate)
	}
	spec := svcSpec{
		svc:    svc,
		agg:    &aggregateInfo{pkg: pkg, state: state, streamType: aggOpt.GetStreamType(), sum: cmds},
		events: events,
	}
	for _, m := range svc.Methods {
		mw := fmt.Sprintf("%s.%s", svc.Desc.FullName(), m.Desc.Name())
		if m.Desc.IsStreamingClient() || m.Desc.IsStreamingServer() {
			return svcSpec{}, fmt.Errorf("%s: command RPCs must be unary", mw)
		}
		sm, err := resolveMethod(mw, m, spec.agg, cmds, r)
		if err != nil {
			return svcSpec{}, err
		}
		if sm.principal != nil {
			if spec.principal != nil && spec.principal != sm.principal.Message {
				return svcSpec{}, fmt.Errorf("%s: acting principal type %s differs from %s used by another RPC; one identity source per service",
					mw, sm.principal.Message.Desc.FullName(), spec.principal.Desc.FullName())
			}
			spec.principal = sm.principal.Message
		}
		spec.methods = append(spec.methods, sm)
	}
	// ADR 0031 §2 step 3: if the aggregate package declares acting_principal
	// anywhere, every RPC whose command carries it must be served from the
	// identity source — which is what the typed interface guarantees; what
	// must fail is a command that declares it yet has no RPC source. All
	// RPC commands with the field get it set, so the remaining case is a
	// service that lacks any principal-carrying RPC while the package has
	// such commands: allowed (those commands are simply not served here).
	return spec, nil
}

func resolveMethod(where string, m *protogen.Method, agg *aggregateInfo, cmds *sumInfo, r *resolver) (svcMethod, error) {
	sm := svcMethod{m: m}
	for _, fld := range m.Input.Fields {
		opts := fld.Desc.Options()
		if opts != nil && proto.HasExtension(opts, esv1.E_StreamId) {
			if on, _ := proto.GetExtension(opts, esv1.E_StreamId).(bool); on {
				if fld.Desc.Kind() != protoreflect.StringKind || fld.Desc.IsList() {
					return sm, fmt.Errorf("%s: (es.v1.stream_id) must be a singular string field", where)
				}
				if sm.streamID != nil {
					return sm, fmt.Errorf("%s: two (es.v1.stream_id) fields", where)
				}
				sm.streamID = fld
				continue
			}
		}
		if fld.Message != nil && cmds.variants[string(fld.Message.Desc.FullName())] {
			if sm.command != nil {
				return sm, fmt.Errorf("%s: request carries two command fields (%s, %s)", where, sm.command.Desc.Name(), fld.Desc.Name())
			}
			sm.command, sm.variant = fld, fld.Message
		}
	}
	if sm.streamID == nil {
		return sm, fmt.Errorf("%s: request needs exactly one string field tagged (es.v1.stream_id)", where)
	}
	if sm.command == nil {
		return sm, fmt.Errorf("%s: request needs exactly one field whose type is a variant of %s's Command container", where, agg.pkg)
	}
	for _, fld := range sm.variant.Fields {
		opts := fld.Desc.Options()
		if opts == nil {
			continue
		}
		if on, _ := proto.GetExtension(opts, esv1.E_ActingPrincipal).(bool); on {
			if fld.Message == nil || fld.Desc.IsList() || fld.Desc.IsMap() {
				return sm, fmt.Errorf("%s: (es.v1.acting_principal) must be a singular message field", sm.variant.Desc.FullName())
			}
			if sm.principal != nil {
				return sm, fmt.Errorf("%s: two (es.v1.acting_principal) fields", sm.variant.Desc.FullName())
			}
			sm.principal = fld
		}
		if on, _ := proto.GetExtension(opts, esv1.E_ExpectedVersion).(bool); on {
			if fld.Desc.Kind() != protoreflect.Uint64Kind || fld.Desc.IsList() {
				return sm, fmt.Errorf("%s: (es.v1.expected_version) must be a singular uint64 field", sm.variant.Desc.FullName())
			}
			if sm.expected != nil {
				return sm, fmt.Errorf("%s: two (es.v1.expected_version) fields", sm.variant.Desc.FullName())
			}
			sm.expected = fld
		}
	}
	for _, fld := range m.Output.Fields {
		switch {
		case fld.Desc.Name() == "result":
			if fld.Message == nil {
				return sm, fmt.Errorf("%s: response field `result` must be a message", where)
			}
			sm.result = fld
		case fld.Message != nil && fld.Message == agg.state:
			if sm.state != nil {
				return sm, fmt.Errorf("%s: response carries two %s fields", where, agg.state.Desc.FullName())
			}
			sm.state = fld
		}
	}
	if sm.result == nil {
		return sm, fmt.Errorf("%s: response needs a message field named `result` (with stream_id, version, global_position)", where)
	}
	if sm.state == nil {
		return sm, fmt.Errorf("%s: response needs exactly one field of type %s", where, agg.state.Desc.FullName())
	}
	return sm, nil
}

// connectIdent returns an identifier in the protoc-gen-connect-go package
// of f: <go import path>/<go package name>connect.
func connectIdent(f *protogen.File, name string) protogen.GoIdent {
	return protogen.GoIdent{
		GoName:       name,
		GoImportPath: protogen.GoImportPath(path.Join(string(f.GoImportPath), string(f.GoPackageName)+"connect")),
	}
}

// esPackageName is the sibling package the service handlers go into,
// mirroring connect-go's <pkg>connect: <pkg>es.
func esPackageName(f *protogen.File) string { return string(f.GoPackageName) + "es" }

// writeServiceFile emits <dir>/<pkg>es/<base>_es.pb.go with every command
// service of f. It is a separate package because countersvcv1connect imports
// countersvcv1 (the messages), so a handler implementing the connect
// interface cannot live in countersvcv1 itself.
func writeServiceFile(gen *protogen.Plugin, f *protogen.File, svcs []svcSpec) {
	dir, base := path.Split(f.GeneratedFilenamePrefix)
	pkg := esPackageName(f)
	importPath := protogen.GoImportPath(path.Join(string(f.GoImportPath), pkg))
	g := gen.NewGeneratedFile(path.Join(dir, pkg, base+"_es.pb.go"), importPath)
	g.P("// Code generated by protoc-gen-es-lite. DO NOT EDIT.")
	g.P("//")
	g.P("// Source: ", f.Desc.Path())
	g.P()
	g.P("// Package ", pkg, " holds the generated Connect command-service handlers")
	g.P("// (ADR 0031) for ", f.Desc.Path(), ". Mount with the ", string(f.GoPackageName)+"connect", " constructors.")
	g.P("package ", pkg)
	g.P()
	for _, s := range svcs {
		writeService(g, f, s)
	}
}

// writeService emits, for service CounterCommandService:
//
//	type CounterCommandServiceAuthorizer interface { Authorize<RPC>(ctx, sid, cmd) error ... }
//	type CounterCommandServiceIdentity interface { Principal(ctx) (*P, es.Actor, error) }  // or Actor(ctx)
//	type CounterCommandServiceDeps struct { Runtime, Authorize, Identity, Validate, Filter, Errors }
//	func NewCounterCommandServiceHandler(d Deps) countersvcv1connect.CounterCommandServiceHandler
func writeService(g *protogen.GeneratedFile, f *protogen.File, s svcSpec) {
	name := s.svc.GoName
	ctx := g.QualifiedGoIdent(contextPkgPath.Ident("Context"))
	streamID := g.QualifiedGoIdent(esPkgPath.Ident("StreamID"))
	actor := g.QualifiedGoIdent(esPkgPath.Ident("Actor"))
	meta := g.QualifiedGoIdent(esPkgPath.Ident("Meta"))
	newStreamID := g.QualifiedGoIdent(esPkgPath.Ident("NewStreamID"))
	runtime := g.QualifiedGoIdent(aggregatePkgPath.Ident("Runtime"))
	validator := g.QualifiedGoIdent(cmdsvcPkgPath.Ident("Validator"))
	errorMapper := g.QualifiedGoIdent(cmdsvcPkgPath.Ident("ErrorMapper"))
	protoMsg := g.QualifiedGoIdent(protoMsgPkgPath.Ident("Message"))
	state := "*" + g.QualifiedGoIdent(s.agg.state.GoIdent)
	cmdIface := g.QualifiedGoIdent(s.agg.ifaceIdent())
	evIface := g.QualifiedGoIdent(protogen.GoIdent{GoName: s.events.iface, GoImportPath: s.events.file.GoImportPath})
	handlerIface := g.QualifiedGoIdent(connectIdent(f, name+"Handler"))
	impl := lowerFirst(name) + "Handler"

	g.P("// ", name, "Authorizer is edge authorisation (contracts §3.5), one method per RPC,")
	g.P("// called after validation and principal injection and before Handle. A nil")
	g.P("// return allows; a *connect.Error is passed through; any other error maps to")
	g.P("// permission_denied.")
	g.P("type ", name, "Authorizer interface {")
	for _, m := range s.methods {
		g.P("	Authorize", m.m.GoName, "(ctx ", ctx, ", sid ", streamID, ", cmd *", g.QualifiedGoIdent(m.variant.GoIdent), ") error")
	}
	g.P("}")
	g.P()

	g.P("// ", name, "Identity derives the caller's identity from ctx (the authenticated")
	g.P("// request). The principal is assigned to every command's (es.v1.acting_principal)")
	g.P("// field, discarding what the client sent; the Actor goes on the envelope.")
	g.P("type ", name, "Identity interface {")
	if s.principal != nil {
		g.P("	Principal(ctx ", ctx, ") (*", g.QualifiedGoIdent(s.principal.GoIdent), ", ", actor, ", error)")
	} else {
		g.P("	Actor(ctx ", ctx, ") (", actor, ", error)")
	}
	g.P("}")
	g.P()

	g.P("// ", name, "Deps is what the application wires into the generated handler.")
	g.P("type ", name, "Deps struct {")
	g.P("	Runtime   *", runtime, "[", state, ", ", cmdIface, ", ", evIface, "]")
	g.P("	Authorize ", name, "Authorizer")
	g.P("	Identity  ", name, "Identity")
	g.P("	// Validate checks every request; nil selects cmdsvc.DefaultValidator (protovalidate).")
	g.P("	Validate ", validator)
	g.P("	// Filter shapes the returned state (e.g. clearance); nil returns it as is.")
	g.P("	Filter func(ctx ", ctx, ", state ", state, ") ", state)
	g.P("	// Errors maps domain errors (decider rejections) to Connect errors; nil")
	g.P("	// maps them to failed_precondition.")
	g.P("	Errors ", errorMapper)
	g.P("}")
	g.P()

	g.P("type ", impl, " struct {")
	g.P("	d ", name, "Deps")
	g.P("}")
	g.P()
	g.P("// New", name, "Handler returns the Connect handler for ", name, ". Mount it with")
	g.P("// ", string(f.GoPackageName)+"connect", ".New", name, "Handler.")
	g.P("func New", name, "Handler(d ", name, "Deps) ", handlerIface, " {")
	g.P("	if d.Validate == nil {")
	g.P("		d.Validate = ", g.QualifiedGoIdent(cmdsvcPkgPath.Ident("DefaultValidator")), "()")
	g.P("	}")
	g.P("	return &", impl, "{d: d}")
	g.P("}")
	g.P()
	g.P("func (h *", impl, ") filter(ctx ", ctx, ", state ", state, ") ", state, " {")
	g.P("	if h.d.Filter == nil {")
	g.P("		return state")
	g.P("	}")
	g.P("	return h.d.Filter(ctx, state)")
	g.P("}")
	g.P()
	g.P("// reload fetches the current version and (filtered) state for a VersionConflict detail.")
	g.P("func (h *", impl, ") reload(sid ", streamID, ") ", g.QualifiedGoIdent(cmdsvcPkgPath.Ident("Reload")), " {")
	g.P("	return func(ctx ", ctx, ") (uint64, ", protoMsg, ", error) {")
	g.P("		state, version, err := h.d.Runtime.Load(ctx, sid)")
	g.P("		if err != nil {")
	g.P("			return 0, nil, err")
	g.P("		}")
	g.P("		return version, h.filter(ctx, state), nil")
	g.P("	}")
	g.P("}")
	g.P()

	request := g.QualifiedGoIdent(connectPkgPath.Ident("Request"))
	response := g.QualifiedGoIdent(connectPkgPath.Ident("Response"))
	newResponse := g.QualifiedGoIdent(connectPkgPath.Ident("NewResponse"))
	for _, m := range s.methods {
		in := g.QualifiedGoIdent(m.m.Input.GoIdent)
		out := g.QualifiedGoIdent(m.m.Output.GoIdent)
		g.P("// ", m.m.GoName, " implements ", handlerIface, ".")
		g.P("func (h *", impl, ") ", m.m.GoName, "(ctx ", ctx, ", req *", request, "[", in, "]) (*", response, "[", out, "], error) {")
		// 1. validate
		g.P("	if err := h.d.Validate.Validate(req.Msg); err != nil {")
		g.P("		return nil, ", g.QualifiedGoIdent(cmdsvcPkgPath.Ident("InvalidArgument")), "(err)")
		g.P("	}")
		// 2. stream id
		g.P("	sid, err := ", newStreamID, "(", fmt.Sprintf("%q", s.agg.streamType), ", req.Msg.Get", m.streamID.GoName, "())")
		g.P("	if err != nil {")
		g.P("		return nil, ", g.QualifiedGoIdent(cmdsvcPkgPath.Ident("InvalidArgument")), "(err)")
		g.P("	}")
		g.P("	cmd := req.Msg.Get", m.command.GoName, "()")
		g.P("	if cmd == nil {")
		g.P("		return nil, ", g.QualifiedGoIdent(cmdsvcPkgPath.Ident("MissingCommand")), "(", fmt.Sprintf("%q", m.command.Desc.Name()), ")")
		g.P("	}")
		// 3. identity / principal
		if s.principal != nil {
			g.P("	principal, actor, err := h.d.Identity.Principal(ctx)")
		} else {
			g.P("	actor, err := h.d.Identity.Actor(ctx)")
		}
		g.P("	if err != nil {")
		g.P("		return nil, ", g.QualifiedGoIdent(cmdsvcPkgPath.Ident("Unauthenticated")), "(err)")
		g.P("	}")
		if m.principal != nil {
			g.P("	cmd.", m.principal.GoName, " = principal // never from the client (§3.5)")
		} else if s.principal != nil {
			g.P("	_ = principal // this command carries no (es.v1.acting_principal) field")
		}
		// 4. authorise
		g.P("	if err := h.d.Authorize.Authorize", m.m.GoName, "(ctx, sid, cmd); err != nil {")
		g.P("		return nil, ", g.QualifiedGoIdent(cmdsvcPkgPath.Ident("PermissionDenied")), "(err)")
		g.P("	}")
		// 5. meta; 6. handle
		g.P("	meta := ", g.QualifiedGoIdent(cmdsvcPkgPath.Ident("MetaFrom")), "(req.Header(), actor)")
		expected := "uint64(0)"
		if m.expected != nil {
			expected = "cmd.Get" + m.expected.GoName + "()"
		}
		g.P("	expected := ", expected)
		g.P("	res, err := h.d.Runtime.HandleAt(ctx, sid, cmd, meta, expected)")
		g.P("	if err != nil {")
		g.P("		return nil, ", g.QualifiedGoIdent(cmdsvcPkgPath.Ident("MapError")), "(ctx, err, h.d.Errors, h.reload(sid), expected)")
		g.P("	}")
		// 7. response
		g.P("	resp := &", out, "{}")
		g.P("	resp.", m.result.GoName, " = &", g.QualifiedGoIdent(m.result.Message.GoIdent), "{}")
		g.P("	if err := ", g.QualifiedGoIdent(cmdsvcPkgPath.Ident("SetWriteResult")), "(resp.", m.result.GoName, ", sid, res.ToVersion, ", g.QualifiedGoIdent(cmdsvcPkgPath.Ident("LastGlobalPosition")), "(res.Envelopes)); err != nil {")
		g.P("		return nil, ", g.QualifiedGoIdent(cmdsvcPkgPath.Ident("Internal")), "(err)")
		g.P("	}")
		g.P("	resp.", m.state.GoName, " = h.filter(ctx, res.State)")
		g.P("	return ", newResponse, "(resp), nil")
		g.P("}")
		g.P()
	}
	_ = meta
}
