package main

import (
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	esv1 "github.com/laenenai/es-lite/gen/es/v1"
)

// Sum-type names the projection/reaction generators resolve against.
const eventSumType = "Event"

// resolver indexes every file in the request — imports included, since a
// read model consumes events declared in another package — so a `consumes`
// or `issues` entry can be checked and typed (ADR 0030 §2):
//
//	full type name -> message -> its package's Event/Command container
//	               -> the package's one (es.v1.aggregate) State message.
type resolver struct {
	msgs       map[string]*protogen.Message // by full name, nested included
	sums       map[string]*sumInfo          // by "<package>/<iface>"
	aggregates map[protoreflect.FullName][]*protogen.Message
	commands   map[string]bool // every Command variant, for (es.v1.decided_at)
}

// sumInfo is one (es.v1.sum_type) container: where it lives and what it holds.
type sumInfo struct {
	file     *protogen.File
	iface    string
	variants map[string]bool
}

// aggregateInfo is the resolved State of a consumed/issued type's package.
type aggregateInfo struct {
	pkg        protoreflect.FullName
	state      *protogen.Message
	streamType string
	sum        *sumInfo // the Event or Command container, per the lookup
}

// codecIdent is the generated codec type of the container, e.g. counterv1.EventCodec.
func (a *aggregateInfo) codecIdent() protogen.GoIdent {
	return protogen.GoIdent{GoName: a.sum.iface + "Codec", GoImportPath: a.sum.file.GoImportPath}
}

// ifaceIdent is the generated sum-type interface, e.g. counterv1.Event.
func (a *aggregateInfo) ifaceIdent() protogen.GoIdent {
	return protogen.GoIdent{GoName: a.sum.iface, GoImportPath: a.sum.file.GoImportPath}
}

func newResolver(gen *protogen.Plugin) (*resolver, error) {
	r := &resolver{
		msgs:       map[string]*protogen.Message{},
		sums:       map[string]*sumInfo{},
		aggregates: map[protoreflect.FullName][]*protogen.Message{},
		commands:   map[string]bool{},
	}
	for _, f := range gen.Files {
		pkg := f.Desc.Package()
		for _, m := range allMessages(f.Messages) {
			r.msgs[string(m.Desc.FullName())] = m
			if hasAggregate(m) {
				r.aggregates[pkg] = append(r.aggregates[pkg], m)
			}
		}
		for _, m := range f.Messages {
			name, err := sumTypeName(m)
			if err != nil {
				return nil, err
			}
			if name == "" || len(m.Oneofs) != 1 {
				continue
			}
			key := string(pkg) + "/" + name
			if _, dup := r.sums[key]; dup {
				return nil, fmt.Errorf("%s: a second (es.v1.sum_type) = %q container in package %s", m.Desc.FullName(), name, pkg)
			}
			si := &sumInfo{file: f, iface: name, variants: map[string]bool{}}
			for _, field := range m.Oneofs[0].Fields {
				if field.Message != nil {
					si.variants[string(field.Message.Desc.FullName())] = true
					if name == commandSumType {
						r.commands[string(field.Message.Desc.FullName())] = true
					}
				}
			}
			r.sums[key] = si
		}
	}
	return r, nil
}

// variant resolves fullName to a variant of its package's `iface` container
// and that package's aggregate. where names the option being resolved, for
// error messages.
func (r *resolver) variant(where, fullName, iface string) (*protogen.Message, *aggregateInfo, error) {
	m, ok := r.msgs[fullName]
	if !ok {
		return nil, nil, fmt.Errorf("%s: %q does not resolve to a message in this generation run (its file must be in the same buf generate input: use `strategy: all` for protoc-gen-es-lite)", where, fullName)
	}
	pkg := m.Desc.ParentFile().Package()
	sum, ok := r.sums[string(pkg)+"/"+iface]
	if !ok || !sum.variants[fullName] {
		return nil, nil, fmt.Errorf("%s: %q is not a variant of an (es.v1.sum_type) = %q container in package %s", where, fullName, iface, pkg)
	}
	aggs := r.aggregates[pkg]
	if len(aggs) != 1 {
		return nil, nil, fmt.Errorf("%s: package %s must have exactly one (es.v1.aggregate) message, found %d", where, pkg, len(aggs))
	}
	agg, _ := proto.GetExtension(aggs[0].Desc.Options(), esv1.E_Aggregate).(*esv1.Aggregate)
	if agg.GetStreamType() == "" {
		return nil, nil, fmt.Errorf("%s: %s: (es.v1.aggregate).stream_type is empty", where, aggs[0].Desc.FullName())
	}
	return m, &aggregateInfo{pkg: pkg, state: aggs[0], streamType: agg.GetStreamType(), sum: sum}, nil
}

func hasAggregate(m *protogen.Message) bool {
	opts := m.Desc.Options()
	return opts != nil && proto.HasExtension(opts, esv1.E_Aggregate)
}

// aggregateSet is the ordered set of aggregates a spec touches, keyed by
// package, so deps fields and codecs are emitted once per package.
type aggregateSet struct {
	byPkg map[protoreflect.FullName]*aggregateInfo
}

func newAggregateSet() *aggregateSet {
	return &aggregateSet{byPkg: map[protoreflect.FullName]*aggregateInfo{}}
}

func (s *aggregateSet) add(a *aggregateInfo) { s.byPkg[a.pkg] = a }

// sorted returns the aggregates in package order for deterministic output.
func (s *aggregateSet) sorted() []*aggregateInfo {
	out := make([]*aggregateInfo, 0, len(s.byPkg))
	for _, a := range s.byPkg {
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].pkg < out[j].pkg })
	return out
}

// depName is the Deps field prefix for an aggregate: its State Go name.
func depName(a *aggregateInfo) string { return a.state.GoIdent.GoName }

// subjectToken mirrors natsjs.subjectToken so generated subjects need no
// natsjs import: anything outside [A-Za-z0-9_-] becomes '_'.
func subjectToken(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// eventSubjectSuffix is ".<stream_type>.<event>" of evt.<ws>.<stream_type>.<event>.
func eventSubjectSuffix(a *aggregateInfo, typeURL string) string {
	short := typeURL
	if i := strings.LastIndexByte(typeURL, '.'); i >= 0 {
		short = typeURL[i+1:]
	}
	return "." + subjectToken(a.streamType) + "." + subjectToken(strings.ToLower(short))
}
