package main

import (
	"fmt"
	"strings"
	"unicode"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"

	esv1 "github.com/laenenai/es-lite/gen/es/v1"
)

const (
	contextPkgPath    = protogen.GoImportPath("context")
	projectionPkgPath = protogen.GoImportPath("github.com/laenenai/es-lite/projection")
	reactionPkgPath   = protogen.GoImportPath("github.com/laenenai/es-lite/reaction")
)

// consumed is one `consumes` entry, resolved.
type consumed struct {
	method  string // handler method name: On<Event>, or On<StreamType><Event> when the spec spans aggregates
	typeURL string
	msg     *protogen.Message
	agg     *aggregateInfo
}

// projSpec is one (es.v1.projection) message, validated (ADR 0030 §2, §4).
type projSpec struct {
	msg      *protogen.Message
	base     string // Go name prefix for generated identifiers
	spec     *esv1.ProjectionSpec
	consumes []consumed
	aggs     *aggregateSet
}

// specNames guards name uniqueness across one generation run (§4).
var specNames = map[string]string{}

func collectProjections(f *protogen.File, r *resolver) ([]projSpec, error) {
	var out []projSpec
	for _, m := range allMessages(f.Messages) {
		opts := m.Desc.Options()
		if opts == nil || !proto.HasExtension(opts, esv1.E_Projection) {
			continue
		}
		spec, _ := proto.GetExtension(opts, esv1.E_Projection).(*esv1.ProjectionSpec)
		where := fmt.Sprintf("%s: (es.v1.projection)", m.Desc.FullName())
		switch spec.GetKind() {
		case esv1.ProjectionKind_PROJECTION_KIND_READ_MODEL, esv1.ProjectionKind_PROJECTION_KIND_INDEX:
			if spec.GetRebuild() != esv1.RebuildMode_REBUILD_MODE_FROM_LOG {
				return nil, fmt.Errorf("%s: kind %s requires rebuild = REBUILD_MODE_FROM_LOG", where, spec.GetKind())
			}
		case esv1.ProjectionKind_PROJECTION_KIND_REACTION:
			return nil, fmt.Errorf("%s: kind REACTION must be declared with (es.v1.reaction)", where)
		default:
			return nil, fmt.Errorf("%s: kind must be set", where)
		}
		p := projSpec{msg: m, base: m.GoIdent.GoName, spec: spec, aggs: newAggregateSet()}
		var err error
		if p.consumes, err = resolveConsumes(where, spec.GetName(), spec.GetConsumes(), spec.GetFetch(), r, p.aggs); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, nil
}

// resolveConsumes validates the shared parts of a projection/reaction spec:
// a unique name, a set fetch mode, and every consumed type an Event variant.
func resolveConsumes(where, name string, consumes []string, fetch esv1.FetchMode, r *resolver, aggs *aggregateSet) ([]consumed, error) {
	if name == "" {
		return nil, fmt.Errorf("%s: name is required", where)
	}
	if prev, dup := specNames[name]; dup && prev != where {
		return nil, fmt.Errorf("%s: name %q is already used by %s", where, name, prev)
	}
	specNames[name] = where
	if fetch == esv1.FetchMode_FETCH_MODE_UNSPECIFIED {
		return nil, fmt.Errorf("%s: fetch must be set", where)
	}
	if len(consumes) == 0 {
		return nil, fmt.Errorf("%s: consumes must list at least one event type", where)
	}
	var out []consumed
	for _, c := range consumes {
		m, agg, err := r.variant(where, c, eventSumType)
		if err != nil {
			return nil, err
		}
		aggs.add(agg)
		out = append(out, consumed{typeURL: c, msg: m, agg: agg})
	}
	// Handler method names: On<Event> when every consumed event comes from
	// one aggregate; On<StreamType><Event> for every method when the spec
	// spans aggregates, since different aggregates legitimately share event
	// names (DerogationGranted on a case and on a review). The rule applies
	// to the whole spec so names stay predictable; adding a second aggregate
	// to a spec is a deliberate change to that spec.
	qualify := aggs.len() > 1
	seen := map[string]string{}
	for i := range out {
		name := "On" + out[i].msg.GoIdent.GoName
		if qualify {
			name = "On" + exportSnake(out[i].agg.streamType) + out[i].msg.GoIdent.GoName
		}
		if prev, ok := seen[name]; ok {
			return nil, fmt.Errorf("%s: %q and %q would both generate %s; consumed event names must be unique within an aggregate", where, prev, out[i].typeURL, name)
		}
		seen[name] = out[i].typeURL
		out[i].method = name
	}
	return out, nil
}

// exportSnake turns a stream type ("unit_review") into an exported Go name
// fragment ("UnitReview").
func exportSnake(s string) string {
	var b strings.Builder
	up := true
	for _, r := range s {
		if r == '_' || r == '-' {
			up = true
			continue
		}
		if up {
			b.WriteRune(unicode.ToUpper(r))
			up = false
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func (p projSpec) fetchesState() bool {
	return p.spec.GetFetch() == esv1.FetchMode_FETCH_MODE_STATE_AS_OF_EVENT_VERSION
}

func (p projSpec) catalogue() catalogueEntry {
	e := catalogueEntry{
		kind: p.spec.GetKind().String(), name: p.spec.GetName(), owner: p.spec.GetOwnerContext(),
		fetch: p.spec.GetFetch().String(), rebuild: p.spec.GetRebuild().String(), message: string(p.msg.Desc.FullName()),
	}
	for _, c := range p.consumes {
		e.consumes = append(e.consumes, c.typeURL)
	}
	return e
}

// writeProjection emits, for base "CounterView":
//
//	const CounterViewName = "counter.view"
//	func CounterViewSubjects(workspace string) []string
//	type CounterViewHandler interface { On<Event>(ctx, env[, ev, state]) error ... }
//	type CounterView<State>Loader interface { LoadAsOfVersion(...) }   (STATE_AS_OF only)
//	type CounterViewDeps struct { <State> <Loader>; <State>Codec es.Codec[<Event>]; Marker projection.Marker }
//	func NewCounterViewRunner(h CounterViewHandler, deps CounterViewDeps) *projection.Runner
func writeProjection(g *protogen.GeneratedFile, p projSpec) {
	base := p.base
	writeSpecHeader(g, base, "projection", p.spec.GetName(), p.consumes, p.aggs)
	writeHandlerIface(g, base, p.consumes, p.fetchesState(), "")
	writeDeps(g, base, p.aggs, p.fetchesState(), nil)

	projRunner := g.QualifiedGoIdent(projectionPkgPath.Ident("Runner"))
	g.P("// New", base, "Runner wires h into a projection.Runner: marker handling, live")
	g.P("// consumption (Run over any projection.Feed) and Rebuild from the log share")
	g.P("// one dispatch path (ADR 0005, ADR 0030).")
	g.P("func New", base, "Runner(h ", base, "Handler, deps ", base, "Deps) *", projRunner, " {")
	writeDispatch(g, base, p.consumes, p.fetchesState(), nil)
	g.P("	return &", projRunner, "{Name: ", base, "Name, Marker: deps.Marker, Dispatch: dispatch, Filter: ", lowerFirst(base), "Consumes}")
	g.P("}")
	g.P()
}

// writeSpecHeader emits the Name constant, the Subjects function and the
// consumed-type filter shared by projections and reactions.
func writeSpecHeader(g *protogen.GeneratedFile, base, kind, name string, consumes []consumed, aggs *aggregateSet) {
	envelope := g.QualifiedGoIdent(esPkgPath.Ident("Envelope"))
	g.P("// ", base, "Name is the ", kind, "'s stable name: its marker key and durable name.")
	g.P("const ", base, "Name = ", fmt.Sprintf("%q", name))
	g.P()
	g.P("// ", base, "Subjects returns the evt.<workspace>.<aggregate>.<event> filter subject")
	g.P("// of every consumed event type; workspace may be \"*\" for all workspaces")
	g.P("// (natsjs.ConsumerConfig.FilterSubjects).")
	g.P("func ", base, "Subjects(workspace string) []string {")
	g.P("	if workspace == \"\" {")
	g.P("		workspace = \"default\"")
	g.P("	}")
	g.P("	return []string{")
	for _, c := range consumes {
		g.P("		\"evt.\" + workspace + ", fmt.Sprintf("%q", eventSubjectSuffix(c.agg, c.typeURL)), ",")
	}
	g.P("	}")
	g.P("}")
	g.P()
	g.P("// ", lowerFirst(base), "Consumes reports whether env is one of the consumed types.")
	g.P("func ", lowerFirst(base), "Consumes(env ", envelope, ") bool {")
	g.P("	switch env.TypeURL {")
	for i, c := range consumes {
		sep := ","
		if i == len(consumes)-1 {
			sep = ":"
		}
		if i == 0 {
			g.P("	case ", fmt.Sprintf("%q", c.typeURL), sep)
			continue
		}
		g.P("		", fmt.Sprintf("%q", c.typeURL), sep)
	}
	g.P("		return true")
	g.P("	}")
	g.P("	return false")
	g.P("}")
	g.P()
}

// writeHandlerIface emits On<Event> per consumed type. With state fetching
// the method takes the decoded event and the state as of the event's
// version; metadata-only takes the envelope alone. issuer, when set, is
// appended as a final parameter (reactions).
func writeHandlerIface(g *protogen.GeneratedFile, base string, consumes []consumed, withState bool, issuer string) {
	ctx := g.QualifiedGoIdent(contextPkgPath.Ident("Context"))
	envelope := g.QualifiedGoIdent(esPkgPath.Ident("Envelope"))
	g.P("// ", base, "Handler is the application-written part: one method per consumed")
	g.P("// event, typed by the event's Go type. Delivery is at-least-once; the")
	g.P("// runner skips positions at or below the marker, so a method sees each")
	g.P("// position at most once after a clean save, but must still tolerate a")
	g.P("// redelivery after a crash between apply and save.")
	g.P("type ", base, "Handler interface {")
	for _, c := range consumes {
		params := "ctx " + ctx + ", env " + envelope
		if withState {
			params += ", ev *" + g.QualifiedGoIdent(c.msg.GoIdent) + ", state *" + g.QualifiedGoIdent(c.agg.state.GoIdent)
		}
		if issuer != "" {
			params += ", issue *" + issuer
		}
		g.P("	", c.method, "(", params, ") error")
	}
	g.P("}")
	g.P()
}

// writeDeps emits the loader seams and the Deps struct. routers, when
// non-nil, adds a reaction.Dispatcher per issuing aggregate and the Actor.
func writeDeps(g *protogen.GeneratedFile, base string, aggs *aggregateSet, withState bool, routers *aggregateSet) {
	ctx := g.QualifiedGoIdent(contextPkgPath.Ident("Context"))
	streamID := g.QualifiedGoIdent(esPkgPath.Ident("StreamID"))
	codec := g.QualifiedGoIdent(esPkgPath.Ident("Codec"))
	marker := g.QualifiedGoIdent(projectionPkgPath.Ident("Marker"))
	if withState {
		for _, a := range aggs.sorted() {
			g.P("// ", base, depName(a), "Loader fetches ", depName(a), " state as of a stream version.")
			g.P("// *aggregate.Runtime[*", g.QualifiedGoIdent(a.state.GoIdent), ", ...] satisfies it with no adapter.")
			g.P("type ", base, depName(a), "Loader interface {")
			g.P("	LoadAsOfVersion(ctx ", ctx, ", sid ", streamID, ", v uint64) (*", g.QualifiedGoIdent(a.state.GoIdent), ", error)")
			g.P("}")
			g.P()
		}
	}
	g.P("// ", base, "Deps is what the application wires into the runner.")
	g.P("type ", base, "Deps struct {")
	if withState {
		for _, a := range aggs.sorted() {
			g.P("	// ", depName(a), " loads state as of the event's version (required).")
			g.P("	", depName(a), " ", base, depName(a), "Loader")
			g.P("	// ", depName(a), "Codec decodes ", a.pkg, " events; nil selects the generated codec.")
			g.P("	// Supply an unsealing codec when payloads are sealed (ADR 0025).")
			g.P("	", depName(a), "Codec ", codec, "[", g.QualifiedGoIdent(a.ifaceIdent()), "]")
		}
	}
	if routers != nil {
		dispatcher := g.QualifiedGoIdent(reactionPkgPath.Ident("Dispatcher"))
		actor := g.QualifiedGoIdent(esPkgPath.Ident("Actor"))
		for _, a := range routers.sorted() {
			g.P("	// ", depName(a), "Router delivers issued ", a.pkg, " commands (reaction.RuntimeDispatcher")
			g.P("	// over the embedded runtime, or a command gate client).")
			g.P("	", depName(a), "Router ", dispatcher, "[", g.QualifiedGoIdent(a.ifaceIdent()), "]")
		}
		g.P("	// Actor is the system principal issued commands are attributed to.")
		g.P("	Actor ", actor)
	}
	g.P("	// Marker persists the highest applied global_position (ADR 0005); nil is")
	g.P("	// an in-memory marker, for tests only.")
	g.P("	Marker ", marker)
	g.P("}")
	g.P()
}

// writeDispatch emits `dispatch := func(ctx, env) error { switch env.TypeURL
// {...} }` inside a runner constructor. issuer, when non-nil, is the
// reaction's issuing set: the handler gets an issuer and its issues are
// executed after it returns.
func writeDispatch(g *protogen.GeneratedFile, base string, consumes []consumed, withState bool, issuer *aggregateSet) {
	ctx := g.QualifiedGoIdent(contextPkgPath.Ident("Context"))
	envelope := g.QualifiedGoIdent(esPkgPath.Ident("Envelope"))
	encoded := g.QualifiedGoIdent(esPkgPath.Ident("EncodedEvent"))
	errorf := g.QualifiedGoIdent(fmtPkgPath.Ident("Errorf"))

	if withState {
		aggs := newAggregateSet()
		for _, c := range consumes {
			aggs.add(c.agg)
		}
		for _, a := range aggs.sorted() {
			v := lowerFirst(depName(a)) + "Codec"
			g.P("	", v, " := deps.", depName(a), "Codec")
			g.P("	if ", v, " == nil {")
			g.P("		", v, " = ", g.QualifiedGoIdent(a.codecIdent()), "{}")
			g.P("	}")
		}
	}
	g.P("	dispatch := func(ctx ", ctx, ", env ", envelope, ") error {")
	if issuer != nil {
		g.P("		issue := &", base, "Issuer{}")
	}
	g.P("		switch env.TypeURL {")
	for _, c := range consumes {
		g.P("		case ", fmt.Sprintf("%q", c.typeURL), ":")
		args := "ctx, env"
		if withState {
			v := lowerFirst(depName(c.agg)) + "Codec"
			g.P("			decoded, err := ", v, ".Decode(", encoded, "{TypeURL: env.TypeURL, SchemaVersion: env.SchemaVersion, Payload: env.Payload})")
			g.P("			if err != nil {")
			g.P("				return ", errorf, "(\"", base, ": decode %s v%d: %w\", env.StreamID, env.Version, err)")
			g.P("			}")
			g.P("			ev, ok := decoded.(*", g.QualifiedGoIdent(c.msg.GoIdent), ")")
			g.P("			if !ok {")
			g.P("				return ", errorf, "(\"", base, ": %s decoded as %T\", env.TypeURL, decoded)")
			g.P("			}")
			g.P("			state, err := deps.", depName(c.agg), ".LoadAsOfVersion(ctx, env.StreamID, env.Version)")
			g.P("			if err != nil {")
			g.P("				return ", errorf, "(\"", base, ": load %s v%d: %w\", env.StreamID, env.Version, err)")
			g.P("			}")
			args += ", ev, state"
		}
		if issuer != nil {
			args += ", issue"
		}
		if issuer == nil {
			g.P("			return h.", c.method, "(", args, ")")
		} else {
			g.P("			if err := h.", c.method, "(", args, "); err != nil {")
			g.P("				return err")
			g.P("			}")
		}
	}
	if issuer == nil {
		g.P("		default:")
		g.P("			return nil // not consumed; filtered before dispatch")
		g.P("		}")
	} else {
		g.P("		default:")
		g.P("			return nil // not consumed; filtered before dispatch")
		g.P("		}")
		execute := g.QualifiedGoIdent(reactionPkgPath.Ident("Execute"))
		metaFor := g.QualifiedGoIdent(reactionPkgPath.Ident("MetaFor"))
		for _, a := range issuer.sorted() {
			g.P("		if err := ", execute, "(ctx, deps.", depName(a), "Router, issue.", lowerFirst(depName(a)), ", ", metaFor, "(env, deps.Actor)); err != nil {")
			g.P("			return err")
			g.P("		}")
		}
		g.P("		return nil")
	}
	g.P("	}")
}
