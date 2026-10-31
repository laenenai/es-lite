package main

import (
	"fmt"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"

	esv1 "github.com/laenenai/es-lite/gen/es/v1"
)

// issued is one `issues` entry, resolved.
type issued struct {
	method  string // issuer method name: <Command>, or <StreamType><Command> when issues span aggregates
	typeURL string
	msg     *protogen.Message
	agg     *aggregateInfo
}

// reactSpec is one (es.v1.reaction) message, validated (ADR 0030 §3).
type reactSpec struct {
	msg      *protogen.Message
	base     string
	spec     *esv1.ReactionSpec
	consumes []consumed
	issues   []issued
	aggs     *aggregateSet // consumed
	routers  *aggregateSet // issuing
}

func collectReactions(f *protogen.File, r *resolver) ([]reactSpec, error) {
	var out []reactSpec
	for _, m := range allMessages(f.Messages) {
		opts := m.Desc.Options()
		if opts == nil || !proto.HasExtension(opts, esv1.E_Reaction) {
			continue
		}
		if proto.HasExtension(opts, esv1.E_Projection) {
			return nil, fmt.Errorf("%s: carries both (es.v1.projection) and (es.v1.reaction)", m.Desc.FullName())
		}
		spec, _ := proto.GetExtension(opts, esv1.E_Reaction).(*esv1.ReactionSpec)
		where := fmt.Sprintf("%s: (es.v1.reaction)", m.Desc.FullName())
		rc := reactSpec{msg: m, base: m.GoIdent.GoName, spec: spec, aggs: newAggregateSet(), routers: newAggregateSet()}
		var err error
		if rc.consumes, err = resolveConsumes(where, spec.GetName(), spec.GetConsumes(), spec.GetFetch(), r, rc.aggs); err != nil {
			return nil, err
		}
		for _, c := range spec.GetIssues() {
			cm, agg, err := r.variant(where, c, commandSumType)
			if err != nil {
				return nil, err
			}
			rc.routers.add(agg)
			rc.issues = append(rc.issues, issued{typeURL: c, msg: cm, agg: agg})
		}
		// Issuer method names follow the handler rule: qualified by stream
		// type only when the issued commands span aggregates.
		qualify := rc.routers.len() > 1
		seen := map[string]string{}
		for i := range rc.issues {
			name := rc.issues[i].msg.GoIdent.GoName
			if qualify {
				name = exportSnake(rc.issues[i].agg.streamType) + name
			}
			if prev, ok := seen[name]; ok {
				return nil, fmt.Errorf("%s: %q and %q would both generate issuer method %s; issued command names must be unique within an aggregate", where, prev, rc.issues[i].typeURL, name)
			}
			seen[name] = rc.issues[i].typeURL
			rc.issues[i].method = name
		}
		out = append(out, rc)
	}
	return out, nil
}

func (rc reactSpec) fetchesState() bool {
	return rc.spec.GetFetch() == esv1.FetchMode_FETCH_MODE_STATE_AS_OF_EVENT_VERSION
}

func (rc reactSpec) catalogue() catalogueEntry {
	e := catalogueEntry{
		kind: "REACTION", name: rc.spec.GetName(), owner: rc.spec.GetOwnerContext(),
		fetch: rc.spec.GetFetch().String(), message: string(rc.msg.Desc.FullName()),
	}
	for _, c := range rc.consumes {
		e.consumes = append(e.consumes, c.typeURL)
	}
	for _, i := range rc.issues {
		e.issues = append(e.issues, i.typeURL)
	}
	return e
}

// writeReaction emits, for base "ResetOnClosed": the Name/Subjects/filter
// header, the Handler (with a trailing *ResetOnClosedIssuer parameter), the
// Issuer with one method per issued command, loader seams, Deps (loaders,
// codecs, one reaction.Dispatcher per issuing aggregate, Actor, Marker) and
// NewResetOnClosedRunner returning a *reaction.Runner (no Rebuild).
func writeReaction(g *protogen.GeneratedFile, rc reactSpec) {
	base := rc.base
	issuerName := base + "Issuer"
	streamID := g.QualifiedGoIdent(esPkgPath.Ident("StreamID"))
	issue := g.QualifiedGoIdent(reactionPkgPath.Ident("Issue"))

	writeSpecHeader(g, base, "reaction", rc.spec.GetName(), rc.consumes, rc.aggs)
	writeHandlerIface(g, base, rc.consumes, rc.fetchesState(), issuerName)

	g.P("// ", issuerName, " collects the commands a handler issues for one event; the")
	g.P("// runner delivers them after the handler returns (reaction.Execute: conflicts")
	g.P("// retried, a terminal target counts as done).")
	g.P("type ", issuerName, " struct {")
	for _, a := range rc.routers.sorted() {
		g.P("	", lowerFirst(depName(a)), " []", issue, "[", g.QualifiedGoIdent(a.ifaceIdent()), "]")
	}
	g.P("}")
	g.P()
	for _, i := range rc.issues {
		field := lowerFirst(depName(i.agg))
		g.P("// ", i.method, " issues a ", i.typeURL, " to sid.")
		g.P("func (i *", issuerName, ") ", i.method, "(sid ", streamID, ", cmd *", g.QualifiedGoIdent(i.msg.GoIdent), ") {")
		g.P("	i.", field, " = append(i.", field, ", ", issue, "[", g.QualifiedGoIdent(i.agg.ifaceIdent()), "]{StreamID: sid, Command: cmd})")
		g.P("}")
		g.P()
	}

	writeDeps(g, base, rc.aggs, rc.fetchesState(), rc.routers)

	runner := g.QualifiedGoIdent(reactionPkgPath.Ident("Runner"))
	newRunner := g.QualifiedGoIdent(reactionPkgPath.Ident("NewRunner"))
	g.P("// New", base, "Runner wires h into a reaction.Runner: marker handling and live")
	g.P("// consumption (Run over any projection.Feed). There is no Rebuild — replaying")
	g.P("// a reaction re-issues its commands (ADR 0030 §2).")
	g.P("func New", base, "Runner(h ", base, "Handler, deps ", base, "Deps) *", runner, " {")
	writeDispatch(g, base, rc.consumes, rc.fetchesState(), rc.routers)
	g.P("	return ", newRunner, "(", base, "Name, deps.Marker, dispatch, ", lowerFirst(base), "Consumes)")
	g.P("}")
	g.P()
}
