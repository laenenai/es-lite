// protoc-gen-es-lite is es-lite's code generator. For every message
// annotated with (es.v1.sum_type), it emits — into that message's own Go
// package — a sealed interface over the oneof's variants, a marker method
// on each variant, and an es.Codec implementation. This is the file the
// hand-written counter_es.go previously stood in for.
//
// It is a standard protoc/buf plugin: it reads a CodeGeneratorRequest on
// stdin and writes a CodeGeneratorResponse on stdout. Wire it via
// buf.gen.yaml (`local: protoc-gen-es-lite`).
package main

import (
	"fmt"
	"unicode"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/pluginpb"

	esv1 "github.com/laenenai/es-lite/gen/es/v1"
)

// Import paths referenced by generated code.
const (
	protoPkgPath = protogen.GoImportPath("google.golang.org/protobuf/proto")
	esPkgPath    = protogen.GoImportPath("github.com/laenenai/es-lite/es")
	fmtPkgPath   = protogen.GoImportPath("fmt")
	timePkgPath  = protogen.GoImportPath("time")
)

// timestampFullName is the only field type (es.v1.decided_at) may annotate.
const timestampFullName = "google.protobuf.Timestamp"

// commandSumType is the sum-type name whose variants may carry
// (es.v1.decided_at): commands, never events or value types (ADR 0032).
const commandSumType = "Command"

func main() {
	protogen.Options{}.Run(run)
}

// run is the plugin body, separated from main so tests can drive it with a
// hand-built CodeGeneratorRequest.
func run(gen *protogen.Plugin) error {
	// The generated code uses proto3; advertise support so buf does
	// not warn on optional fields.
	gen.SupportedFeatures = uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL)
	cmds, err := commandVariants(gen)
	if err != nil {
		return err
	}
	for _, f := range gen.Files {
		if !f.Generate {
			continue
		}
		if err := generateFile(gen, f, cmds); err != nil {
			return err
		}
	}
	return nil
}

// commandVariants collects, across every file in the request (imports
// included, so a container may live in another file of the package), the
// variants of each (es.v1.sum_type) = "Command" container. It is the set a
// (es.v1.decided_at) field may appear in.
func commandVariants(gen *protogen.Plugin) (map[string]bool, error) {
	out := map[string]bool{}
	for _, f := range gen.Files {
		for _, m := range f.Messages {
			name, err := sumTypeName(m)
			if err != nil {
				return nil, err
			}
			if name != commandSumType || len(m.Oneofs) != 1 {
				continue
			}
			for _, field := range m.Oneofs[0].Fields {
				if field.Message != nil {
					out[string(field.Message.Desc.FullName())] = true
				}
			}
		}
	}
	return out, nil
}

// sumType is one annotated container: its generated interface name, the
// variant messages of its oneof, any non-default schema versions (keyed by
// full proto type name) declared via (es.v1.schema_version), and the
// (es.v1.decided_at) field of each variant that declares one (ADR 0032).
type sumType struct {
	iface      string
	variants   []*protogen.Message
	nonDefault map[string]uint32
	decidedAt  map[string]*protogen.Field
}

func generateFile(gen *protogen.Plugin, f *protogen.File, cmds map[string]bool) error {
	// (es.v1.decided_at) is only meaningful on a Command variant; reject it
	// anywhere else in this file before looking at containers.
	for _, m := range allMessages(f.Messages) {
		field, err := decidedAtField(m)
		if err != nil {
			return err
		}
		if field != nil && !cmds[string(m.Desc.FullName())] {
			return fmt.Errorf("%s.%s: (es.v1.decided_at) is only allowed on a variant of an (es.v1.sum_type) = %q container",
				m.Desc.FullName(), field.Desc.Name(), commandSumType)
		}
	}

	var sums []sumType
	for _, m := range f.Messages {
		name, err := sumTypeName(m)
		if err != nil {
			return err
		}
		if name == "" {
			continue
		}
		if len(m.Oneofs) != 1 {
			return fmt.Errorf("%s: (es.v1.sum_type) requires exactly one oneof, found %d", m.Desc.FullName(), len(m.Oneofs))
		}
		st := sumType{iface: name}
		for _, field := range m.Oneofs[0].Fields {
			if field.Message == nil {
				return fmt.Errorf("%s: sum-type variant %q must be a message", m.Desc.FullName(), field.Desc.Name())
			}
			st.variants = append(st.variants, field.Message)
			if v := schemaVersionOf(field.Message); v != 1 {
				if st.nonDefault == nil {
					st.nonDefault = map[string]uint32{}
				}
				st.nonDefault[string(field.Message.Desc.FullName())] = v
			}
			if name == commandSumType {
				// Already validated above (type and placement); only the
				// lookup remains.
				if da, _ := decidedAtField(field.Message); da != nil {
					if st.decidedAt == nil {
						st.decidedAt = map[string]*protogen.Field{}
					}
					st.decidedAt[string(field.Message.Desc.FullName())] = da
				}
			}
		}
		sums = append(sums, st)
	}
	if len(sums) == 0 {
		return nil // nothing to generate for this file
	}

	g := gen.NewGeneratedFile(f.GeneratedFilenamePrefix+"_es.pb.go", f.GoImportPath)
	g.P("// Code generated by protoc-gen-es-lite. DO NOT EDIT.")
	g.P("//")
	g.P("// Source: ", f.Desc.Path())
	g.P()
	g.P("package ", f.GoPackageName)
	g.P()

	for _, st := range sums {
		writeSumType(g, st)
	}
	return nil
}

func writeSumType(g *protogen.GeneratedFile, st sumType) {
	proto := g.QualifiedGoIdent(protoPkgPath.Ident("Message"))
	marshal := g.QualifiedGoIdent(protoPkgPath.Ident("Marshal"))
	unmarshal := g.QualifiedGoIdent(protoPkgPath.Ident("Unmarshal"))
	encodedEvent := g.QualifiedGoIdent(esPkgPath.Ident("EncodedEvent"))
	codec := g.QualifiedGoIdent(esPkgPath.Ident("Codec"))
	unknownErr := g.QualifiedGoIdent(esPkgPath.Ident("ErrUnknownEventType"))
	errorf := g.QualifiedGoIdent(fmtPkgPath.Ident("Errorf"))

	iface := st.iface
	marker := "is" + iface

	// Sealed interface + marker methods.
	g.P("// ", iface, " is the sealed sum type over the annotated oneof's variants.")
	g.P("type ", iface, " interface{ ", marker, "() }")
	g.P()
	for _, v := range st.variants {
		g.P("func (*", g.QualifiedGoIdent(v.GoIdent), ") ", marker, "() {}")
	}
	g.P()

	// DecisionTime accessors (ADR 0032): one per variant that tags a
	// Timestamp field with (es.v1.decided_at). The method is not named
	// DecidedAt because protoc-gen-go already emits a struct field of that
	// name for a field called decided_at. An unset field yields the zero
	// time, not the Unix epoch timestamppb would report, so the runtime can
	// tell "missing" from "1970".
	for _, v := range st.variants {
		field, ok := st.decidedAt[string(v.Desc.FullName())]
		if !ok {
			continue
		}
		timeType := g.QualifiedGoIdent(timePkgPath.Ident("Time"))
		decidedAt := g.QualifiedGoIdent(esPkgPath.Ident("DecidedAt"))
		g.P("// DecisionTime returns the command's decision time from its (es.v1.decided_at)")
		g.P("// field, or the zero time when the field is unset.")
		g.P("func (x *", g.QualifiedGoIdent(v.GoIdent), ") DecisionTime() ", timeType, " {")
		g.P("	if ts := x.Get", field.GoName, "(); ts != nil {")
		g.P("		return ts.AsTime()")
		g.P("	}")
		g.P("	return ", timeType, "{}")
		g.P("}")
		g.P()
		g.P("var _ ", decidedAt, " = (*", g.QualifiedGoIdent(v.GoIdent), ")(nil)")
		g.P()
	}

	// Codec.
	codecType := iface + "Codec"
	g.P("// ", codecType, " marshals each ", iface, " variant as canonical proto")
	g.P("// bytes tagged with its full proto type name.")
	g.P("type ", codecType, " struct{}")
	g.P()
	g.P("var _ ", codec, "[", iface, "] = ", codecType, "{}")
	g.P()

	// Encode.
	schemaFn := lowerFirst(iface) + "SchemaVersion"
	g.P("func (", codecType, ") Encode(v ", iface, ") (", encodedEvent, ", error) {")
	g.P("	m, ok := v.(", proto, ")")
	g.P("	if !ok {")
	g.P("		return ", encodedEvent, "{}, ", errorf, "(\"", iface, "Codec: %T is not a proto.Message\", v)")
	g.P("	}")
	g.P("	b, err := ", marshal, "(m)")
	g.P("	if err != nil {")
	g.P("		return ", encodedEvent, "{}, err")
	g.P("	}")
	g.P("	typeURL := string(m.ProtoReflect().Descriptor().FullName())")
	if len(st.nonDefault) == 0 {
		g.P("	return ", encodedEvent, "{TypeURL: typeURL, SchemaVersion: 1, Payload: b}, nil")
	} else {
		g.P("	return ", encodedEvent, "{TypeURL: typeURL, SchemaVersion: ", schemaFn, "(typeURL), Payload: b}, nil")
	}
	g.P("}")
	g.P()

	// Per-variant schema version lookup (only when some variant overrides
	// the default of 1). Cases are emitted in variant declaration order for
	// deterministic output.
	if len(st.nonDefault) > 0 {
		g.P("func ", schemaFn, "(typeURL string) uint32 {")
		g.P("	switch typeURL {")
		for _, v := range st.variants {
			name := string(v.Desc.FullName())
			if ver, ok := st.nonDefault[name]; ok {
				g.P("	case ", fmt.Sprintf("%q", name), ":")
				g.P("		return ", ver)
			}
		}
		g.P("	default:")
		g.P("		return 1")
		g.P("	}")
		g.P("}")
		g.P()
	}

	// Decode.
	g.P("func (", codecType, ") Decode(enc ", encodedEvent, ") (", iface, ", error) {")
	g.P("	var msg ", proto)
	g.P("	switch enc.TypeURL {")
	for _, v := range st.variants {
		g.P("	case ", fmt.Sprintf("%q", string(v.Desc.FullName())), ":")
		g.P("		msg = &", g.QualifiedGoIdent(v.GoIdent), "{}")
	}
	g.P("	default:")
	g.P("		return nil, ", errorf, "(\"%w: %q\", ", unknownErr, ", enc.TypeURL)")
	g.P("	}")
	g.P("	if err := ", unmarshal, "(enc.Payload, msg); err != nil {")
	g.P("		return nil, err")
	g.P("	}")
	g.P("	return msg.(", iface, "), nil")
	g.P("}")
	g.P()
}

// sumTypeName returns the (es.v1.sum_type) value on a message, or "" if
// unset.
func sumTypeName(m *protogen.Message) (string, error) {
	opts := m.Desc.Options()
	if opts == nil {
		return "", nil
	}
	if !proto.HasExtension(opts, esv1.E_SumType) {
		return "", nil
	}
	name, ok := proto.GetExtension(opts, esv1.E_SumType).(string)
	if !ok {
		return "", fmt.Errorf("%s: (es.v1.sum_type) is not a string", m.Desc.FullName())
	}
	return name, nil
}

// decidedAtField returns the one field of m tagged (es.v1.decided_at), or nil.
// It is an error for a message to tag more than one field or to tag a field
// that is not a google.protobuf.Timestamp.
func decidedAtField(m *protogen.Message) (*protogen.Field, error) {
	var found *protogen.Field
	for _, field := range m.Fields {
		opts := field.Desc.Options()
		if opts == nil || !proto.HasExtension(opts, esv1.E_DecidedAt) {
			continue
		}
		on, _ := proto.GetExtension(opts, esv1.E_DecidedAt).(bool)
		if !on {
			continue
		}
		if field.Message == nil || field.Message.Desc.FullName() != timestampFullName {
			return nil, fmt.Errorf("%s.%s: (es.v1.decided_at) requires a %s field", m.Desc.FullName(), field.Desc.Name(), timestampFullName)
		}
		if field.Desc.IsList() || field.Desc.IsMap() {
			return nil, fmt.Errorf("%s.%s: (es.v1.decided_at) must be a singular field", m.Desc.FullName(), field.Desc.Name())
		}
		if found != nil {
			return nil, fmt.Errorf("%s: (es.v1.decided_at) set on both %s and %s; only one field may carry it",
				m.Desc.FullName(), found.Desc.Name(), field.Desc.Name())
		}
		found = field
	}
	return found, nil
}

// allMessages flattens a message tree (nested messages included) in
// declaration order.
func allMessages(msgs []*protogen.Message) []*protogen.Message {
	var out []*protogen.Message
	for _, m := range msgs {
		out = append(out, m)
		out = append(out, allMessages(m.Messages)...)
	}
	return out
}

// schemaVersionOf returns the (es.v1.schema_version) on a variant message,
// defaulting to 1 when unset or zero.
func schemaVersionOf(m *protogen.Message) uint32 {
	opts := m.Desc.Options()
	if opts == nil || !proto.HasExtension(opts, esv1.E_SchemaVersion) {
		return 1
	}
	v, ok := proto.GetExtension(opts, esv1.E_SchemaVersion).(uint32)
	if !ok || v == 0 {
		return 1
	}
	return v
}

// lowerFirst lowercases the first rune, turning an exported interface name
// into an unexported helper name (e.g. "Event" -> "eventSchemaVersion").
func lowerFirst(s string) string {
	if s == "" {
		return s
	}
	r := []rune(s)
	r[0] = unicode.ToLower(r[0])
	return string(r)
}
