package main

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/pluginpb"

	es "github.com/laenenai/es-lite/es"
	counterv1 "github.com/laenenai/es-lite/gen/counter/v1"
	counterviewv1 "github.com/laenenai/es-lite/gen/counterview/v1"
	esv1 "github.com/laenenai/es-lite/gen/es/v1"
)

// The plugin is tested by feeding it hand-built file descriptors: the
// counter fixture covers the happy path through `task generate:check`;
// these tests cover the generation errors, which need malformed inputs no
// checked-in .proto should carry.

// fixture builds a CodeGeneratorRequest for one test .proto (the file to
// generate) plus the es/v1 options and timestamp descriptors it imports.
func fixture(t *testing.T, file *descriptorpb.FileDescriptorProto) *protogen.Plugin {
	t.Helper()
	req := &pluginpb.CodeGeneratorRequest{
		FileToGenerate: []string{file.GetName()},
		ProtoFile: []*descriptorpb.FileDescriptorProto{
			protodesc.ToFileDescriptorProto(descriptorpb.File_google_protobuf_descriptor_proto),
			protodesc.ToFileDescriptorProto(timestamppb.File_google_protobuf_timestamp_proto),
			protodesc.ToFileDescriptorProto(durationpb.File_google_protobuf_duration_proto),
			protodesc.ToFileDescriptorProto(esv1.File_es_v1_options_proto),
			file,
		},
	}
	gen, err := protogen.Options{}.New(req)
	if err != nil {
		t.Fatalf("protogen: %v", err)
	}
	return gen
}

// run executes the plugin body and returns the error (nil on success).
func runPlugin(t *testing.T, file *descriptorpb.FileDescriptorProto) error {
	t.Helper()
	return run(fixture(t, file), "")
}

func fileProto(name, pkg string) *descriptorpb.FileDescriptorProto {
	return &descriptorpb.FileDescriptorProto{
		Name:       proto.String(name),
		Package:    proto.String(pkg),
		Syntax:     proto.String("proto3"),
		Dependency: []string{"es/v1/options.proto", "google/protobuf/timestamp.proto"},
		Options:    &descriptorpb.FileOptions{GoPackage: proto.String("example.com/gen/" + pkg + ";" + strings.ReplaceAll(pkg, ".", ""))},
	}
}

func sumTypeOpts(name string) *descriptorpb.MessageOptions {
	o := &descriptorpb.MessageOptions{}
	proto.SetExtension(o, esv1.E_SumType, name)
	return o
}

func decidedAtOpts() *descriptorpb.FieldOptions {
	o := &descriptorpb.FieldOptions{}
	proto.SetExtension(o, esv1.E_DecidedAt, true)
	return o
}

func tsField(name string, num int32, opts *descriptorpb.FieldOptions) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:     proto.String(name),
		Number:   proto.Int32(num),
		Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
		TypeName: proto.String(".google.protobuf.Timestamp"),
		JsonName: proto.String(name),
		Options:  opts,
	}
}

func stringField(name string, num int32, opts *descriptorpb.FieldOptions) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:     proto.String(name),
		Number:   proto.Int32(num),
		Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		Type:     descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
		JsonName: proto.String(name),
		Options:  opts,
	}
}

// container returns an (es.v1.sum_type) message with one oneof over the
// named variant messages.
func container(name, iface string, variants ...string) *descriptorpb.DescriptorProto {
	m := &descriptorpb.DescriptorProto{
		Name:      proto.String(name),
		Options:   sumTypeOpts(iface),
		OneofDecl: []*descriptorpb.OneofDescriptorProto{{Name: proto.String("variant")}},
	}
	for i, v := range variants {
		m.Field = append(m.Field, &descriptorpb.FieldDescriptorProto{
			Name:       proto.String(strings.ToLower(v)),
			Number:     proto.Int32(int32(i + 1)),
			Label:      descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			Type:       descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
			TypeName:   proto.String("." + "t.v1." + v),
			JsonName:   proto.String(strings.ToLower(v)),
			OneofIndex: proto.Int32(0),
		})
	}
	return m
}

func msg(name string, fields ...*descriptorpb.FieldDescriptorProto) *descriptorpb.DescriptorProto {
	return &descriptorpb.DescriptorProto{Name: proto.String(name), Field: fields}
}

func TestDecidedAtOnCommandVariantGenerates(t *testing.T) {
	f := fileProto("t/v1/t.proto", "t.v1")
	f.MessageType = []*descriptorpb.DescriptorProto{
		msg("Do", tsField("decided_at", 1, decidedAtOpts())),
		container("Commands", "Command", "Do"),
	}
	gen := fixture(t, f)
	if err := run(gen, ""); err != nil {
		t.Fatalf("generate: %v", err)
	}
	resp := gen.Response()
	if len(resp.File) != 1 {
		t.Fatalf("generated %d files, want 1", len(resp.File))
	}
	if !strings.Contains(resp.File[0].GetContent(), "func (x *Do) DecisionTime() time.Time") {
		t.Fatalf("generated code lacks DecisionTime:\n%s", resp.File[0].GetContent())
	}
}

func TestDecidedAtRejectsNonTimestamp(t *testing.T) {
	f := fileProto("t/v1/t.proto", "t.v1")
	f.MessageType = []*descriptorpb.DescriptorProto{
		msg("Do", stringField("decided_at", 1, decidedAtOpts())),
		container("Commands", "Command", "Do"),
	}
	err := runPlugin(t, f)
	if err == nil || !strings.Contains(err.Error(), "requires a google.protobuf.Timestamp field") {
		t.Fatalf("error = %v, want timestamp-type rejection", err)
	}
}

func TestDecidedAtRejectsNonCommandVariant(t *testing.T) {
	f := fileProto("t/v1/t.proto", "t.v1")
	f.MessageType = []*descriptorpb.DescriptorProto{
		msg("Happened", tsField("decided_at", 1, decidedAtOpts())),
		container("Events", "Event", "Happened"),
	}
	err := runPlugin(t, f)
	if err == nil || !strings.Contains(err.Error(), `only allowed on a variant of an (es.v1.sum_type) = "Command" container`) {
		t.Fatalf("error = %v, want non-command rejection", err)
	}
}

func TestDecidedAtRejectsTwoFields(t *testing.T) {
	f := fileProto("t/v1/t.proto", "t.v1")
	f.MessageType = []*descriptorpb.DescriptorProto{
		msg("Do", tsField("decided_at", 1, decidedAtOpts()), tsField("also", 2, decidedAtOpts())),
		container("Commands", "Command", "Do"),
	}
	err := runPlugin(t, f)
	if err == nil || !strings.Contains(err.Error(), "only one field may carry it") {
		t.Fatalf("error = %v, want two-field rejection", err)
	}
}

// ---- ADR 0030: projection / reaction generation errors ------------------

func aggregateOpts(streamType string) *descriptorpb.MessageOptions {
	o := &descriptorpb.MessageOptions{}
	proto.SetExtension(o, esv1.E_Aggregate, &esv1.Aggregate{StreamType: streamType})
	return o
}

func projectionOpts(spec *esv1.ProjectionSpec) *descriptorpb.MessageOptions {
	o := &descriptorpb.MessageOptions{}
	proto.SetExtension(o, esv1.E_Projection, spec)
	return o
}

func reactionOpts(spec *esv1.ReactionSpec) *descriptorpb.MessageOptions {
	o := &descriptorpb.MessageOptions{}
	proto.SetExtension(o, esv1.E_Reaction, spec)
	return o
}

// domainFile is a minimal aggregate package: State (aggregate), Happened
// (event), Do (command), with Event and Command containers.
func domainFile(withAggregate bool) *descriptorpb.FileDescriptorProto {
	f := fileProto("t/v1/t.proto", "t.v1")
	state := msg("State")
	if withAggregate {
		state.Options = aggregateOpts("thing")
	}
	f.MessageType = []*descriptorpb.DescriptorProto{
		state,
		msg("Happened"), msg("Do"),
		container("Events", "Event", "Happened"),
		container("Commands", "Command", "Do"),
	}
	return f
}

// readModel puts the projection alone in its own file (t/v1/view.proto),
// with the domain package only imported: a projection that is alone in its
// file is the shape that caught the metadata-only fmt import (Task 17),
// because nothing else in the file can mask an unused import.
func readModel(spec *esv1.ProjectionSpec) *descriptorpb.FileDescriptorProto {
	f := fileProto("t/v1/view.proto", "t.v1")
	f.Dependency = append(f.Dependency, "t/v1/t.proto")
	f.MessageType = []*descriptorpb.DescriptorProto{{Name: proto.String("View"), Options: projectionOpts(spec)}}
	return f
}

func goodSpec() *esv1.ProjectionSpec {
	return &esv1.ProjectionSpec{
		Name: "t.view", Kind: esv1.ProjectionKind_PROJECTION_KIND_READ_MODEL,
		Consumes: []string{"t.v1.Happened"},
		Fetch:    esv1.FetchMode_FETCH_MODE_STATE_AS_OF_EVENT_VERSION,
		Rebuild:  esv1.RebuildMode_REBUILD_MODE_FROM_LOG,
	}
}

// runWith generates `file` with `extra` descriptors also in the request
// (imports), returning the error and the generated content.
func runWith(t *testing.T, file *descriptorpb.FileDescriptorProto, extra ...*descriptorpb.FileDescriptorProto) (string, error) {
	t.Helper()
	req := &pluginpb.CodeGeneratorRequest{
		FileToGenerate: []string{file.GetName()},
		ProtoFile: append([]*descriptorpb.FileDescriptorProto{
			protodesc.ToFileDescriptorProto(descriptorpb.File_google_protobuf_descriptor_proto),
			protodesc.ToFileDescriptorProto(timestamppb.File_google_protobuf_timestamp_proto),
			protodesc.ToFileDescriptorProto(durationpb.File_google_protobuf_duration_proto),
			protodesc.ToFileDescriptorProto(esv1.File_es_v1_options_proto),
		}, append(extra, file)...),
	}
	gen, err := protogen.Options{}.New(req)
	if err != nil {
		t.Fatalf("protogen: %v", err)
	}
	if err := run(gen, ""); err != nil {
		return "", err
	}
	var content string
	for _, f := range gen.Response().File {
		content += f.GetContent()
	}
	return content, nil
}

func TestProjectionGeneratesHandlerAndRunner(t *testing.T) {
	// Domain and read model in one file keeps the fixture small; the
	// counterview fixture under examples/ covers the cross-package case.
	f := domainFile(true)
	f.MessageType = append(f.MessageType, &descriptorpb.DescriptorProto{Name: proto.String("View"), Options: projectionOpts(goodSpec())})
	content, err := runWith(t, f)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	for _, want := range []string{
		`const ViewName = "t.view"`,
		`"evt." + workspace + ".thing.happened"`,
		"func ViewDefinition() projection.Definition",
		"return projection.Definition{Name: ViewName, Consumes: ViewConsumes()}",
		"OnHappened(ctx context.Context, env es.Envelope, ev *Happened, state *State) error",
		"type ViewStateLoader interface",
		"func NewViewRunner(h ViewHandler, deps ViewDeps) *projection.Runner",
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("generated code lacks %q:\n%s", want, content)
		}
	}
	assertAllImportsUsed(t, content)
}

// TestProjectionEmitsUpcasterSeam (F20): a state-fetching spec's Deps
// carries an optional per-aggregate es.Upcaster — nil decodes as stored —
// and dispatch runs it between decode and the variant assertion, so a
// stored v1 event can flow through regenerated code. Reactions share the
// same deps/dispatch emit; metadata-only specs decode nothing and are
// unchanged.
func TestProjectionEmitsUpcasterSeam(t *testing.T) {
	f := domainFile(true)
	f.MessageType = append(f.MessageType, &descriptorpb.DescriptorProto{Name: proto.String("View"), Options: projectionOpts(goodSpec())})
	content, err := runWith(t, f)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	for _, want := range []string{
		"StateUpcaster upgrades stored event shapes on read; nil decodes as stored.",
		"StateUpcaster es.Upcaster[Event]",
		"stateUpcaster := deps.StateUpcaster",
		"if stateUpcaster != nil {",
		"decoded, err = stateUpcaster.Upcast(env.TypeURL, env.SchemaVersion, decoded)",
		`return fmt.Errorf("View: upcast %s v%d: %w", env.StreamID, env.Version, err)`,
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("generated code lacks %q:\n%s", want, content)
		}
	}
	// The upcast must run before the variant assertion, so the handler is
	// handed the upgraded event, not the stored one.
	if i, j := strings.Index(content, "stateUpcaster.Upcast("), strings.Index(content, "decoded.(*Happened)"); i < 0 || j < 0 || i > j {
		t.Fatalf("upcast must run between decode and the variant assertion:\n%s", content)
	}
	assertAllImportsUsed(t, content)

	// The reaction's deps share the same seam.
	r := domainFile(true)
	r.MessageType = append(r.MessageType, &descriptorpb.DescriptorProto{Name: proto.String("React"), Options: reactionOpts(&esv1.ReactionSpec{
		Name: "t.react", Consumes: []string{"t.v1.Happened"}, Issues: []string{"t.v1.Do"},
		Fetch: esv1.FetchMode_FETCH_MODE_STATE_AS_OF_EVENT_VERSION,
	})})
	if content, err = runWith(t, r); err != nil {
		t.Fatalf("generate reaction: %v", err)
	}
	for _, want := range []string{
		"StateUpcaster es.Upcaster[Event]",
		"decoded, err = stateUpcaster.Upcast(env.TypeURL, env.SchemaVersion, decoded)",
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("generated reaction lacks %q:\n%s", want, content)
		}
	}

	// Metadata-only specs decode nothing: no seam.
	spec := goodSpec()
	spec.Fetch = esv1.FetchMode_FETCH_MODE_METADATA_ONLY
	if content, err = runWith(t, readModel(spec), domainFile(true)); err != nil {
		t.Fatalf("generate metadata-only: %v", err)
	}
	if strings.Contains(content, "Upcaster") {
		t.Fatalf("metadata-only projection must not emit an upcaster seam:\n%s", content)
	}
}

func TestProjectionErrors(t *testing.T) {
	cases := map[string]struct {
		mutate func(*esv1.ProjectionSpec)
		agg    bool
		want   string
	}{
		"fetch unset":       {func(s *esv1.ProjectionSpec) { s.Fetch = 0 }, true, "fetch must be set"},
		"kind unset":        {func(s *esv1.ProjectionSpec) { s.Kind = 0 }, true, "kind must be set"},
		"kind reaction":     {func(s *esv1.ProjectionSpec) { s.Kind = esv1.ProjectionKind_PROJECTION_KIND_REACTION }, true, "must be declared with (es.v1.reaction)"},
		"no rebuild":        {func(s *esv1.ProjectionSpec) { s.Rebuild = 0 }, true, "requires rebuild = REBUILD_MODE_FROM_LOG"},
		"empty consumes":    {func(s *esv1.ProjectionSpec) { s.Consumes = nil }, true, "at least one event type"},
		"unknown type":      {func(s *esv1.ProjectionSpec) { s.Consumes = []string{"t.v1.Nope"} }, true, "does not resolve"},
		"not an event":      {func(s *esv1.ProjectionSpec) { s.Consumes = []string{"t.v1.Do"} }, true, `not a variant of an (es.v1.sum_type) = "Event"`},
		"no aggregate":      {func(*esv1.ProjectionSpec) {}, false, "exactly one (es.v1.aggregate)"},
		"name missing":      {func(s *esv1.ProjectionSpec) { s.Name = "" }, true, "name is required"},
		"duplicate consume": {func(s *esv1.ProjectionSpec) { s.Consumes = []string{"t.v1.Happened", "t.v1.Happened"} }, true, "must be unique"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			spec := goodSpec()
			c.mutate(spec)
			f := domainFile(c.agg)
			f.MessageType = append(f.MessageType, &descriptorpb.DescriptorProto{Name: proto.String("View"), Options: projectionOpts(spec)})
			_, err := runWith(t, f)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error = %v, want %q", err, c.want)
			}
		})
	}
}

func TestDuplicateSpecNameRejected(t *testing.T) {
	f := domainFile(true)
	f.MessageType = append(f.MessageType,
		&descriptorpb.DescriptorProto{Name: proto.String("ViewA"), Options: projectionOpts(goodSpec())},
		&descriptorpb.DescriptorProto{Name: proto.String("ViewB"), Options: projectionOpts(goodSpec())},
	)
	_, err := runWith(t, f)
	if err == nil || !strings.Contains(err.Error(), "already used by") {
		t.Fatalf("error = %v, want duplicate-name rejection", err)
	}
}

// A metadata-only projection alone in its file must not register fmt: the
// dispatch has no state-decoding branch, so an unconditional errorf leaves
// the import unused and the generated file does not compile downstream.
func TestProjectionMetadataOnlyHasNoUnusedImports(t *testing.T) {
	spec := goodSpec()
	spec.Fetch = esv1.FetchMode_FETCH_MODE_METADATA_ONLY
	content, err := runWith(t, readModel(spec), domainFile(true))
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	for _, want := range []string{
		"OnHappened(ctx context.Context, env es.Envelope) error",
		"func NewViewRunner(h ViewHandler, deps ViewDeps) *projection.Runner",
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("generated code lacks %q:\n%s", want, content)
		}
	}
	if strings.Contains(content, "\"fmt\"") {
		t.Fatalf("metadata-only projection must not import fmt:\n%s", content)
	}
	assertAllImportsUsed(t, content)
}

// TestProjectionEventOnlyDecodesWithoutState (F38): a FETCH_MODE_EVENT_ONLY
// spec validates like the other modes — rebuild is still required for a read
// model — and its handlers receive the typed event, On<Event>(ctx, env,
// ev *E), while the dispatch decodes and upcasts but never fetches state:
// Deps carries the Codec and Upcaster seams and no loader, and the runner
// has no required deps. Reactions share the same emit, with the issuer as
// the trailing parameter.
func TestProjectionEventOnlyDecodesWithoutState(t *testing.T) {
	spec := goodSpec()
	spec.Fetch = esv1.FetchMode_FETCH_MODE_EVENT_ONLY
	// Alone in its own file, the metadata-only shape of Task 17: the
	// decode branch registers fmt, so nothing may be left unused either way.
	content, err := runWith(t, readModel(spec), domainFile(true))
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	for _, want := range []string{
		"OnHappened(ctx context.Context, env es.Envelope, ev *Happened) error",
		"StateCodec es.Codec[Event]",
		"StateUpcaster es.Upcaster[Event]",
		"stateCodec := deps.StateCodec",
		"if stateCodec == nil {",
		"decoded, err = stateUpcaster.Upcast(env.TypeURL, env.SchemaVersion, decoded)",
		`return fmt.Errorf("View: decode %s v%d: %w", env.StreamID, env.Version, err)`,
		"return h.OnHappened(ctx, env, ev)",
		"func NewViewRunner(h ViewHandler, deps ViewDeps) *projection.Runner",
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("generated code lacks %q:\n%s", want, content)
		}
	}
	// Decode-only: no loader seam, no loader field, no state fetch.
	if strings.Contains(content, "Loader") {
		t.Fatalf("event-only projection must not emit a state loader:\n%s", content)
	}
	if strings.Contains(content, "LoadAsOfVersion") {
		t.Fatalf("event-only projection must not fetch state:\n%s", content)
	}
	// The handler is handed the upgraded event: upcast runs between decode
	// and the variant assertion (Task 26's ordering, without the state load).
	if i, j := strings.Index(content, "stateUpcaster.Upcast("), strings.Index(content, "decoded.(*Happened)"); i < 0 || j < 0 || i > j {
		t.Fatalf("upcast must run between decode and the variant assertion:\n%s", content)
	}
	assertAllImportsUsed(t, content)

	// Rebuild is required like the other modes: the kind rule is
	// fetch-independent, so an EVENT_ONLY read model without FROM_LOG
	// is rejected.
	norebuild := goodSpec()
	norebuild.Fetch = esv1.FetchMode_FETCH_MODE_EVENT_ONLY
	norebuild.Rebuild = esv1.RebuildMode_REBUILD_MODE_UNSPECIFIED
	f := domainFile(true)
	f.MessageType = append(f.MessageType, &descriptorpb.DescriptorProto{Name: proto.String("View"), Options: projectionOpts(norebuild)})
	if _, err = runWith(t, f); err == nil || !strings.Contains(err.Error(), "requires rebuild = REBUILD_MODE_FROM_LOG") {
		t.Fatalf("error = %v, want rebuild-required rejection", err)
	}

	// Reactions share the emit: the typed event precedes the issuer and
	// no state fetch is emitted there either.
	r := domainFile(true)
	r.MessageType = append(r.MessageType, &descriptorpb.DescriptorProto{Name: proto.String("React"), Options: reactionOpts(&esv1.ReactionSpec{
		Name: "t.react", Consumes: []string{"t.v1.Happened"}, Issues: []string{"t.v1.Do"},
		Fetch: esv1.FetchMode_FETCH_MODE_EVENT_ONLY,
	})})
	if content, err = runWith(t, r); err != nil {
		t.Fatalf("generate reaction: %v", err)
	}
	for _, want := range []string{
		"OnHappened(ctx context.Context, env es.Envelope, ev *Happened, issue *ReactIssuer) error",
		"StateCodec es.Codec[Event]",
		"StateUpcaster es.Upcaster[Event]",
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("generated reaction lacks %q:\n%s", want, content)
		}
	}
	if strings.Contains(content, "LoadAsOfVersion") {
		t.Fatalf("event-only reaction must not fetch state:\n%s", content)
	}
}

// The exported Consumes list preserves the proto option's order, not any
// sorted order — a catalogue seeds and drift-checks its filter from it.
func TestSpecExportsConsumesInOptionOrder(t *testing.T) {
	f := domainFile(true)
	f.Dependency = append(f.Dependency, "u/v1/u.proto")
	f.MessageType = append(f.MessageType, &descriptorpb.DescriptorProto{Name: proto.String("View"), Options: projectionOpts(&esv1.ProjectionSpec{
		Name: "t.view", Kind: esv1.ProjectionKind_PROJECTION_KIND_READ_MODEL,
		// u before t: not alphabetical, so only option order explains it.
		Consumes: []string{"u.v1.Happened", "t.v1.Happened"},
		Fetch:    esv1.FetchMode_FETCH_MODE_METADATA_ONLY,
		Rebuild:  esv1.RebuildMode_REBUILD_MODE_FROM_LOG,
	})})
	content, err := runWith(t, f, secondDomainFile())
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	rest := strings.SplitN(content, "func ViewConsumes() []string", 2)
	if len(rest) != 2 {
		t.Fatalf("generated code lacks ViewConsumes:\n%s", content)
	}
	i := strings.Index(rest[1], `"u.v1.Happened"`)
	j := strings.Index(rest[1], `"t.v1.Happened"`)
	if i < 0 || j < 0 || j < i {
		t.Fatalf("ViewConsumes must list u.v1.Happened before t.v1.Happened:\n%s", content)
	}
}

// assertAllImportsUsed parses generated Go and fails when an imported
// package is never referenced in the file body: an unused import is a
// compile error in the consumer's tree, not in the plugin's own tests.
func assertAllImportsUsed(t *testing.T, content string) {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "generated.go", content, 0)
	if err != nil {
		t.Fatalf("generated code does not parse: %v\n%s", err, content)
	}
	used := map[string]bool{}
	ast.Inspect(file, func(n ast.Node) bool {
		if sel, ok := n.(*ast.SelectorExpr); ok {
			if id, ok := sel.X.(*ast.Ident); ok {
				used[id.Name] = true
			}
		}
		return true
	})
	for _, imp := range file.Imports {
		name := ""
		if imp.Name != nil {
			name = imp.Name.Name
		}
		if name == "_" || name == "." {
			continue
		}
		if name == "" {
			path := strings.Trim(imp.Path.Value, `"`)
			name = path[strings.LastIndex(path, "/")+1:]
		}
		if !used[name] {
			t.Fatalf("generated code imports %s as %s but never uses it:\n%s", imp.Path.Value, name, content)
		}
	}
}

func TestReactionGeneratesIssuerAndRejectsNonCommand(t *testing.T) {
	f := domainFile(true)
	f.MessageType = append(f.MessageType, &descriptorpb.DescriptorProto{Name: proto.String("React"), Options: reactionOpts(&esv1.ReactionSpec{
		Name: "t.react", Consumes: []string{"t.v1.Happened"}, Issues: []string{"t.v1.Do"},
		Fetch: esv1.FetchMode_FETCH_MODE_METADATA_ONLY,
	})})
	content, err := runWith(t, f)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	for _, want := range []string{
		"OnHappened(ctx context.Context, env es.Envelope, issue *ReactIssuer) error",
		"func (i *ReactIssuer) Do(sid es.StreamID, cmd *Do)",
		"StateRouter reaction.Dispatcher[Command]",
		"func NewReactRunner(h ReactHandler, deps ReactDeps) *reaction.Runner",
		"func ReactConsumes() []string",
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("generated code lacks %q:\n%s", want, content)
		}
	}

	bad := domainFile(true)
	bad.MessageType = append(bad.MessageType, &descriptorpb.DescriptorProto{Name: proto.String("React"), Options: reactionOpts(&esv1.ReactionSpec{
		Name: "t.react", Consumes: []string{"t.v1.Happened"}, Issues: []string{"t.v1.Happened"},
		Fetch: esv1.FetchMode_FETCH_MODE_METADATA_ONLY,
	})})
	if _, err := runWith(t, bad); err == nil || !strings.Contains(err.Error(), `not a variant of an (es.v1.sum_type) = "Command"`) {
		t.Fatalf("error = %v, want non-command rejection", err)
	}
}

func TestDocsCatalogue(t *testing.T) {
	f := domainFile(true)
	f.MessageType = append(f.MessageType, &descriptorpb.DescriptorProto{Name: proto.String("View"), Options: projectionOpts(goodSpec())})
	gen := fixture(t, f)
	if err := run(gen, "PROJECTIONS.md"); err != nil {
		t.Fatalf("generate: %v", err)
	}
	if gen.SupportedFeatures&uint64(pluginpb.CodeGeneratorResponse_FEATURE_SUPPORTS_EDITIONS) == 0 {
		t.Fatal("plugin must declare editions support")
	}
	var found bool
	for _, out := range gen.Response().File {
		if out.GetName() != "PROJECTIONS.md" {
			continue
		}
		found = true
		content := out.GetContent()
		// Readable, not one wide table: grouped by owner context and kind,
		// one heading per descriptor, short event names grouped by package,
		// humanised fetch/rebuild.
		for _, want := range []string{
			"## " + goodSpec().GetOwnerContext() + "\n",
			"### Read models\n",
			"#### `t.view` (`t.v1.View`)\n",
			"- consumes: t → `Happened`",
			"- fetch: state as of the event's version",
			"- rebuild: from the log",
		} {
			if !strings.Contains(content, want) {
				t.Fatalf("catalogue lacks %q:\n%s", want, content)
			}
		}
		if strings.Contains(content, "FETCH_MODE_") || strings.Contains(content, "| Owner |") {
			t.Fatalf("catalogue still carries raw enum names or the wide table:\n%s", content)
		}
	}
	if !found {
		t.Fatal("catalogue file not generated")
	}
}

// The catalogue renders the third fetch mode in words, like the other
// two — never the raw enum (F38).
func TestDocsCatalogueHumanisesEventOnly(t *testing.T) {
	f := domainFile(true)
	spec := goodSpec()
	spec.Fetch = esv1.FetchMode_FETCH_MODE_EVENT_ONLY
	f.MessageType = append(f.MessageType, &descriptorpb.DescriptorProto{Name: proto.String("View"), Options: projectionOpts(spec)})
	gen := fixture(t, f)
	if err := run(gen, "PROJECTIONS.md"); err != nil {
		t.Fatalf("generate: %v", err)
	}
	var found bool
	for _, out := range gen.Response().File {
		if out.GetName() != "PROJECTIONS.md" {
			continue
		}
		found = true
		if !strings.Contains(out.GetContent(), "- fetch: typed event only (no state)") {
			t.Fatalf("catalogue lacks the humanised event-only fetch:\n%s", out.GetContent())
		}
	}
	if !found {
		t.Fatal("catalogue file not generated")
	}
}

// ---- ADR 0031: command service generation --------------------------------

func serviceOpts(aggregate string) *descriptorpb.ServiceOptions {
	o := &descriptorpb.ServiceOptions{}
	proto.SetExtension(o, esv1.E_CommandService, &esv1.CommandService{Aggregate: aggregate})
	return o
}

func streamIDOpts() *descriptorpb.FieldOptions {
	o := &descriptorpb.FieldOptions{}
	proto.SetExtension(o, esv1.E_StreamId, true)
	return o
}

func msgField(name string, num int32, typeName string) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name: proto.String(name), Number: proto.Int32(num), JsonName: proto.String(name),
		Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
		TypeName: proto.String(typeName),
	}
}

func uint64Field(name string, num int32) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name: proto.String(name), Number: proto.Int32(num), JsonName: proto.String(name),
		Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		Type:  descriptorpb.FieldDescriptorProto_TYPE_UINT64.Enum(),
	}
}

// serviceFile is the domain file plus a one-RPC command service; mutate
// lets a test break one part of it.
func serviceFile(mutate func(req, resp *descriptorpb.DescriptorProto, svc *descriptorpb.ServiceDescriptorProto)) *descriptorpb.FileDescriptorProto {
	f := domainFile(true)
	req := msg("DoRequest", stringField("id", 1, streamIDOpts()), msgField("command", 2, ".t.v1.Do"))
	resp := msg("DoResponse",
		msgField("result", 1, ".t.v1.WriteResult"),
		msgField("state", 2, ".t.v1.State"))
	wr := msg("WriteResult", stringField("stream_id", 1, nil), uint64Field("version", 2), uint64Field("global_position", 3))
	svc := &descriptorpb.ServiceDescriptorProto{
		Name:    proto.String("ThingCommandService"),
		Options: serviceOpts("t.v1.State"),
		Method: []*descriptorpb.MethodDescriptorProto{{
			Name: proto.String("Do"), InputType: proto.String(".t.v1.DoRequest"), OutputType: proto.String(".t.v1.DoResponse"),
		}},
	}
	if mutate != nil {
		mutate(req, resp, svc)
	}
	f.MessageType = append(f.MessageType, req, resp, wr)
	f.Service = []*descriptorpb.ServiceDescriptorProto{svc}
	return f
}

func TestServiceGeneratesHandlerInSiblingPackage(t *testing.T) {
	f := serviceFile(nil)
	gen := fixture(t, f)
	if err := run(gen, ""); err != nil {
		t.Fatalf("generate: %v", err)
	}
	var svcFile string
	for _, out := range gen.Response().File {
		if strings.HasSuffix(out.GetName(), "tv1es/t_es.pb.go") {
			svcFile = out.GetContent()
		}
	}
	if svcFile == "" {
		t.Fatalf("service handler not generated into the tv1es package; files: %v", names(gen))
	}
	for _, want := range []string{
		"package tv1es",
		"type ThingCommandServiceAuthorizer interface",
		"AuthorizeDo(ctx context.Context, sid es.StreamID, cmd *",
		"Actor(ctx context.Context) (es.Actor, error)", // no acting_principal field: actor-only identity
		"func NewThingCommandServiceHandler(d ThingCommandServiceDeps) tv1connect.ThingCommandServiceHandler",
		`es.NewStreamID("thing", req.Msg.GetId())`,
		"h.d.Runtime.HandleAt(ctx, sid, cmd, meta, expected)",
		"cmdsvc.SetWriteResult(resp.Result, sid, res.ToVersion, cmdsvc.LastGlobalPosition(res.Envelopes))",
	} {
		if !strings.Contains(svcFile, want) {
			t.Fatalf("generated service lacks %q:\n%s", want, svcFile)
		}
	}
}

// The emitted handler carries the draft-compliant Idempotency-Key flow
// (design D6, finding F6; draft-ietf-httpapi-idempotency-key-header-07):
// Deps gains an optional Dedup, and a wired Dedup makes every RPC look the
// command up after Authorize — replay the stored response, reject key
// reuse, save fresh responses — inside one guarded block, so a nil Dedup
// (or a request without the header) runs today's plain path unchanged.
func TestServiceEmitsDedupFlow(t *testing.T) {
	gen := fixture(t, serviceFile(nil))
	if err := run(gen, ""); err != nil {
		t.Fatalf("generate: %v", err)
	}
	var svc string
	for _, out := range gen.Response().File {
		if strings.HasSuffix(out.GetName(), "tv1es/t_es.pb.go") {
			svc = out.GetContent()
		}
	}
	if svc == "" {
		t.Fatalf("service handler not generated; files: %v", names(gen))
	}
	for _, want := range []string{
		"Dedup es.Deduplicator",
		`if h.d.Dedup != nil && req.Header().Get(cmdsvc.HdrIdempotencyKey) != "" {`,
		"fp := cmdsvc.Fingerprint(req.Msg)",
		"rec, found, err := h.d.Dedup.Lookup(ctx, key)",
		"return nil, cmdsvc.IdempotencyKeyReused()",
		"if err := h.d.Dedup.Save(ctx, key, es.DedupRecord{Fingerprint: fp, Response: stored}); err != nil {",
	} {
		if !strings.Contains(svc, want) {
			t.Errorf("generated handler lacks %q:\n%s", want, svc)
		}
	}
	assertAllImportsUsed(t, svc)
}

func names(gen *protogen.Plugin) []string {
	var out []string
	for _, f := range gen.Response().File {
		out = append(out, f.GetName())
	}
	return out
}

func TestServiceErrors(t *testing.T) {
	cases := map[string]struct {
		mutate func(req, resp *descriptorpb.DescriptorProto, svc *descriptorpb.ServiceDescriptorProto)
		want   string
	}{
		"unknown aggregate": {func(_, _ *descriptorpb.DescriptorProto, svc *descriptorpb.ServiceDescriptorProto) {
			svc.Options = serviceOpts("t.v1.Nope")
		}, "is not a message carrying (es.v1.aggregate)"},
		"no stream id": {func(req, _ *descriptorpb.DescriptorProto, _ *descriptorpb.ServiceDescriptorProto) {
			req.Field[0].Options = nil
		}, "exactly one string field tagged (es.v1.stream_id)"},
		"no command": {func(req, _ *descriptorpb.DescriptorProto, _ *descriptorpb.ServiceDescriptorProto) {
			req.Field = req.Field[:1]
		}, "exactly one field whose type is a variant"},
		"two commands": {func(req, _ *descriptorpb.DescriptorProto, _ *descriptorpb.ServiceDescriptorProto) {
			req.Field = append(req.Field, msgField("again", 3, ".t.v1.Do"))
		}, "two command fields"},
		"no result": {func(_, resp *descriptorpb.DescriptorProto, _ *descriptorpb.ServiceDescriptorProto) {
			resp.Field = resp.Field[1:]
		}, "field named `result`"},
		"no state": {func(_, resp *descriptorpb.DescriptorProto, _ *descriptorpb.ServiceDescriptorProto) {
			resp.Field = resp.Field[:1]
		}, "exactly one field of type t.v1.State"},
		"streaming": {func(_, _ *descriptorpb.DescriptorProto, svc *descriptorpb.ServiceDescriptorProto) {
			svc.Method[0].ServerStreaming = proto.Bool(true)
		}, "must be unary"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			err := run(fixture(t, serviceFile(c.mutate)), "")
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error = %v, want %q", err, c.want)
			}
		})
	}
}

// An aggregate named Case (a real one: casework) yields the Go keyword
// `case` as an unexported identifier; the generator must escape it.
func TestLowerFirstEscapesGoKeywords(t *testing.T) {
	for in, want := range map[string]string{
		"Case": "case_", "Type": "type_", "Range": "range_", "Counter": "counter", "Event": "event", "": "",
	} {
		if got := lowerFirst(in); got != want {
			t.Errorf("lowerFirst(%q) = %q, want %q", in, got, want)
		}
	}
}

// secondDomainFile is a second aggregate package, u.v1 (stream type
// "other"), whose event and command carry the same names as t.v1's.
func secondDomainFile() *descriptorpb.FileDescriptorProto {
	f := fileProto("u/v1/u.proto", "u.v1")
	state := msg("Other")
	state.Options = aggregateOpts("other")
	events := container("Events", "Event", "Happened")
	commands := container("Commands", "Command", "Do")
	for _, c := range []*descriptorpb.DescriptorProto{events, commands} {
		for _, fld := range c.Field {
			fld.TypeName = proto.String(strings.Replace(fld.GetTypeName(), ".t.v1.", ".u.v1.", 1))
		}
	}
	f.MessageType = []*descriptorpb.DescriptorProto{state, msg("Happened"), msg("Do"), events, commands}
	return f
}

// A spec consuming the same event name from two aggregates qualifies every
// handler method by stream type; a single-aggregate spec does not. Issuer
// methods follow the same rule for issued commands.
func TestHandlerNamesQualifiedAcrossAggregates(t *testing.T) {
	f := domainFile(true)
	f.Dependency = append(f.Dependency, "u/v1/u.proto")
	f.MessageType = append(f.MessageType, &descriptorpb.DescriptorProto{Name: proto.String("React"), Options: reactionOpts(&esv1.ReactionSpec{
		Name: "t.react", Consumes: []string{"t.v1.Happened", "u.v1.Happened"}, Issues: []string{"t.v1.Do", "u.v1.Do"},
		Fetch: esv1.FetchMode_FETCH_MODE_METADATA_ONLY,
	})})
	content, err := runWith(t, f, secondDomainFile())
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	for _, want := range []string{
		"OnThingHappened(ctx context.Context, env es.Envelope, issue *ReactIssuer) error",
		"OnOtherHappened(ctx context.Context, env es.Envelope, issue *ReactIssuer) error",
		"func (i *ReactIssuer) ThingDo(sid es.StreamID, cmd *Do)",
		"func (i *ReactIssuer) OtherDo(sid es.StreamID, cmd *",
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("generated code lacks %q:\n%s", want, content)
		}
	}
	// Single-aggregate specs keep the short names.
	g := domainFile(true)
	g.MessageType = append(g.MessageType, &descriptorpb.DescriptorProto{Name: proto.String("React"), Options: reactionOpts(&esv1.ReactionSpec{
		Name: "t.react1", Consumes: []string{"t.v1.Happened"}, Issues: []string{"t.v1.Do"},
		Fetch: esv1.FetchMode_FETCH_MODE_METADATA_ONLY,
	})})
	content, err = runWith(t, g)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if !strings.Contains(content, "OnHappened(ctx") || !strings.Contains(content, "func (i *ReactIssuer) Do(sid") {
		t.Fatalf("single-aggregate spec must keep short names:\n%s", content)
	}
}

// ---- F32: construction-time dep validation, result shape, run state -----

// panicLoader satisfies the generated state-loader seam without touching a
// store: constructing a runner must not call it.
type panicLoader struct{}

func (panicLoader) LoadAsOfVersion(context.Context, es.StreamID, uint64) (*counterv1.Counter, error) {
	return nil, nil
}

// panicRouter satisfies the generated reaction.Dispatcher seam.
type panicRouter struct{}

func (panicRouter) Dispatch(context.Context, es.StreamID, counterv1.Command, es.Meta) error {
	return nil
}

// assertPanicsNaming runs construct and requires a panic whose message
// contains want — a missing-dep panic names the dep it wants.
func assertPanicsNaming(t *testing.T, want string, construct func()) {
	t.Helper()
	defer func() {
		t.Helper()
		r := recover()
		if r == nil {
			t.Errorf("no panic, want one naming %q", want)
			return
		}
		if msg := fmt.Sprint(r); !strings.Contains(msg, want) {
			t.Errorf("panic = %q, want it to name %q", msg, want)
		}
	}()
	construct()
}

// The generated constructors must reject a nil required dep at
// construction, naming it, instead of accepting it and misbehaving on
// first use (F32). The optional seams — Validate, Filter, Errors, Marker,
// Codec, Upcaster — keep their nil defaults. The runner guards are proven
// by constructing the checked-in counterview output (this generator's own
// product, kept fresh by `task generate:check`); the service handler is
// generated into the cmdsvc nested module, which this package cannot
// import (ADR 0031 §4), so its guards are asserted on the emitted source.
func TestHandlerPanicNamesMissingDeps(t *testing.T) {
	// Runners: each required dep nil in turn.
	assertPanicsNaming(t, "CounterViewDeps.Counter is required", func() {
		counterviewv1.NewCounterViewRunner(nil, counterviewv1.CounterViewDeps{})
	})
	assertPanicsNaming(t, "ResetOnClosedDeps.Counter is required", func() {
		counterviewv1.NewResetOnClosedRunner(nil, counterviewv1.ResetOnClosedDeps{CounterRouter: panicRouter{}})
	})
	assertPanicsNaming(t, "ResetOnClosedDeps.CounterRouter is required", func() {
		counterviewv1.NewResetOnClosedRunner(nil, counterviewv1.ResetOnClosedDeps{Counter: panicLoader{}})
	})
	// Fully wired: no panic — Codec, Upcaster, Actor and Marker stay nil.
	counterviewv1.NewCounterViewRunner(nil, counterviewv1.CounterViewDeps{Counter: panicLoader{}})
	counterviewv1.NewResetOnClosedRunner(nil, counterviewv1.ResetOnClosedDeps{Counter: panicLoader{}, CounterRouter: panicRouter{}})
	// A metadata-only runner has no required deps: empty Deps construct.
	counterviewv1.NewCounterActivityRunner(nil, counterviewv1.CounterActivityDeps{})

	// Service handler: Runtime, Authorize and Identity are required.
	content, err := runWith(t, serviceFile(nil))
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	for _, dep := range []string{"Runtime", "Authorize", "Identity"} {
		want := fmt.Sprintf("panic(%q)", "ThingCommandServiceDeps."+dep+" is required")
		if !strings.Contains(content, want) {
			t.Errorf("generated handler lacks %s:\n%s", want, content)
		}
	}
	for _, dep := range []string{"Validate", "Filter", "Errors"} {
		if strings.Contains(content, dep+" is required") {
			t.Errorf("generated handler must keep %s's nil default:\n%s", dep, content)
		}
	}
}

// resolveMethod must reject a `result` message (or response shape) that
// cannot carry what cmdsvc.SetWriteResult writes, naming the offending
// field (F32).
func TestResultShapeValidatedAtGeneration(t *testing.T) {
	cases := map[string]struct {
		mutate func(wr, resp *descriptorpb.DescriptorProto)
		want   string
	}{
		"no version": {func(wr, _ *descriptorpb.DescriptorProto) {
			wr.Field = append(wr.Field[:1], wr.Field[2:]...)
		}, `field named "version"`},
		"no stream_id": {func(wr, _ *descriptorpb.DescriptorProto) {
			wr.Field = wr.Field[1:]
		}, `field named "stream_id"`},
		"stream_id not a string": {func(wr, _ *descriptorpb.DescriptorProto) {
			wr.Field[0] = uint64Field("stream_id", 1)
		}, "stream_id must be a string field"},
		"version not uint64": {func(wr, _ *descriptorpb.DescriptorProto) {
			wr.Field[1] = stringField("version", 2, nil)
		}, "version must be a uint64 field"},
		"global_position repeated": {func(wr, _ *descriptorpb.DescriptorProto) {
			wr.Field[2].Label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()
		}, "global_position must be singular"},
		"result repeated": {func(_, resp *descriptorpb.DescriptorProto) {
			resp.Field[0].Label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()
		}, "`result` must be a singular"},
		"state repeated": {func(_, resp *descriptorpb.DescriptorProto) {
			resp.Field[1].Label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()
		}, `"state" must be a singular`},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			f := serviceFile(nil)
			var wr, resp *descriptorpb.DescriptorProto
			for _, m := range f.MessageType {
				switch m.GetName() {
				case "WriteResult":
					wr = m
				case "DoResponse":
					resp = m
				}
			}
			if wr == nil || resp == nil {
				t.Fatal("fixture lacks WriteResult or DoResponse")
			}
			c.mutate(wr, resp)
			err := runPlugin(t, f)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error = %v, want a generation error naming %s", err, c.want)
			}
		})
	}
}

// The spec-name uniqueness registry belongs to one generation run, not to
// the process: the plugin runs once per protoc invocation, but the test
// suite drives `run` many times in one process, so a fresh run must not
// see names a previous run declared (F32).
func TestSpecNamesNotGlobal(t *testing.T) {
	runOnce := func(msgName string) error {
		f := domainFile(true)
		f.MessageType = append(f.MessageType, &descriptorpb.DescriptorProto{
			Name:    proto.String(msgName),
			Options: projectionOpts(goodSpec()),
		})
		return runPlugin(t, f)
	}
	// Two runs, one process, the same spec name declared by different
	// messages: a per-run registry accepts both; a process-global one
	// rejects the second as a duplicate.
	if err := runOnce("View"); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if err := runOnce("View2"); err != nil {
		t.Fatalf("second run declaring the same spec name: %v", err)
	}
}
