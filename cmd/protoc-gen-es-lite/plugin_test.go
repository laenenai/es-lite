package main

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/pluginpb"

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

func readModel(spec *esv1.ProjectionSpec) *descriptorpb.FileDescriptorProto {
	f := fileProto("t/v1/t.proto", "t.v1")
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
	specNames = map[string]string{} // fresh per test: the real plugin runs once per process
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
		"OnHappened(ctx context.Context, env es.Envelope, ev *Happened, state *State) error",
		"type ViewStateLoader interface",
		"func NewViewRunner(h ViewHandler, deps ViewDeps) *projection.Runner",
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("generated code lacks %q:\n%s", want, content)
		}
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
	specNames = map[string]string{}
	gen := fixture(t, f)
	// fixture() lacks the domain file's dependencies only when extra files
	// are needed; here everything is in one file.
	if err := run(gen, "PROJECTIONS.md"); err != nil {
		t.Fatalf("generate: %v", err)
	}
	var found bool
	for _, out := range gen.Response().File {
		if out.GetName() == "PROJECTIONS.md" {
			found = true
			if !strings.Contains(out.GetContent(), "| `t.view` | PROJECTION_KIND_READ_MODEL | `t.v1.Happened` |") {
				t.Fatalf("catalogue content:\n%s", out.GetContent())
			}
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
	specNames = map[string]string{}
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
			specNames = map[string]string{}
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
