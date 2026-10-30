package main

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
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
	return run(fixture(t, file))
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
	if err := run(gen); err != nil {
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
