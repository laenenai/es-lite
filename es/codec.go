package es

// EncodedEvent is the wire form of a single event: a TypeURL that
// identifies the variant, its schema version, and the serialized bytes.
// It is the boundary between the typed aggregate world (sealed event
// interfaces) and the byte-oriented Store.
type EncodedEvent struct {
	TypeURL       string
	SchemaVersion uint32
	Payload       []byte
}

// Codec encodes and decodes an aggregate's event sum type E. By
// convention it is generated from the .proto (future codegen; for now
// hand-written per aggregate) and marshals each variant as canonical
// proto bytes tagged with the variant's full type name.
//
// Decode returns ErrUnknownEventType when it does not recognize the
// TypeURL — the signal for a version-skew deployment.
type Codec[E any] interface {
	Encode(event E) (EncodedEvent, error)
	Decode(enc EncodedEvent) (E, error)
}
