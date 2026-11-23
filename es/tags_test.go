package es_test

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/laenenai/es-lite/es"
)

func TestEnvelopeCarriesTags(t *testing.T) {
	withTags := goldenV1
	withTags.Tags = map[string]string{"k": "v"}

	if got, want := es.CanonicalEnvelope(withTags), es.CanonicalEnvelope(goldenV1); !bytes.Equal(got, want) {
		t.Fatalf("tags changed the canonical envelope:\n got %x\nwant %x", got, want)
	}
	if got, want := es.EnvelopeHash(withTags), es.EnvelopeHash(goldenV1); got != want {
		t.Fatalf("tags changed the event hash: got %x, want %x", got, want)
	}
	if got := es.CanonicalEnvelope(goldenV1); len(got) != 218 {
		t.Fatalf("golden canonical length = %d, want 218", len(got))
	}
}

func TestValidateTag(t *testing.T) {
	valid := []struct{ key, value string }{
		{"flowdoc.space", "space-1"},
		{"a", ""},
		{"a.b_c:d-e", "v"},
		{"k", strings.Repeat("v", es.MaxTagValueBytes)},
	}
	for _, tc := range valid {
		if err := es.ValidateTag(tc.key, tc.value); err != nil {
			t.Errorf("ValidateTag(%q, %q) = %v, want nil", tc.key, tc.value, err)
		}
	}

	invalid := []struct{ name, key, value string }{
		{"empty key", "", "v"},
		{"long key", strings.Repeat("k", es.MaxTagKeyBytes+1), "v"},
		{"uppercase", "Flowdoc.Space", "v"},
		{"slash", "space/1", "v"},
		{"space in key", "a b", "v"},
		{"long value", "k", strings.Repeat("v", es.MaxTagValueBytes+1)},
		{"nul", "k", "a\x00b"},
		{"invalid utf8", "k", string([]byte{0xff})},
	}
	for _, tc := range invalid {
		if err := es.ValidateTag(tc.key, tc.value); !errors.Is(err, es.ErrInvalidTag) {
			t.Errorf("%s: ValidateTag(%q, %q) = %v, want ErrInvalidTag", tc.name, tc.key, tc.value, err)
		}
	}
}

func TestMergedTags(t *testing.T) {
	got, err := es.MergedTags(
		map[string]string{"flowdoc.space": "s1", "k": "batch"},
		[]map[string]string{
			{"flowdoc.block": "b1"},
			{"k": "batch"},
			nil,
		},
	)
	if err != nil {
		t.Fatalf("MergedTags: %v", err)
	}
	want := []map[string]string{
		{"flowdoc.space": "s1", "k": "batch", "flowdoc.block": "b1"},
		{"flowdoc.space": "s1", "k": "batch"},
		{"flowdoc.space": "s1", "k": "batch"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("MergedTags = %#v, want %#v", got, want)
	}

	if _, err := es.MergedTags(
		map[string]string{"k": "batch"},
		[]map[string]string{{"k": "event"}},
	); !errors.Is(err, es.ErrConflictingTag) {
		t.Errorf("conflicting value: err = %v, want ErrConflictingTag", err)
	}

	tooMany := make(map[string]string, es.MaxTagsPerEvent+1)
	for i := 0; i <= es.MaxTagsPerEvent; i++ {
		tooMany[fmt.Sprintf("k%d", i)] = "v"
	}
	if _, err := es.MergedTags(tooMany, []map[string]string{{}}); !errors.Is(err, es.ErrTooManyTags) {
		t.Errorf("too many tags: err = %v, want ErrTooManyTags", err)
	}

	got, err = es.MergedTags(nil, []map[string]string{nil, {}})
	if err != nil {
		t.Fatalf("MergedTags(empty): %v", err)
	}
	if len(got) != 2 || got[0] != nil || got[1] != nil {
		t.Fatalf("MergedTags(empty) = %#v, want two nil maps", got)
	}
}

func TestTagsJSONRoundTrip(t *testing.T) {
	for _, in := range []map[string]string{nil, {}, {"b": "2", "a": "1"}} {
		raw, err := es.EncodeTags(in)
		if err != nil {
			t.Fatalf("EncodeTags(%#v): %v", in, err)
		}
		if len(in) == 0 {
			if raw != nil {
				t.Fatalf("EncodeTags(%#v) = %q, want nil", in, raw)
			}
			continue
		}
		if string(raw) != `{"a":"1","b":"2"}` {
			t.Fatalf("EncodeTags(%#v) = %q, want sorted JSON object", in, raw)
		}
		out, err := es.DecodeTags(raw)
		if err != nil {
			t.Fatalf("DecodeTags(%q): %v", raw, err)
		}
		if !reflect.DeepEqual(out, in) {
			t.Fatalf("DecodeTags(%q) = %#v, want %#v", raw, out, in)
		}
	}

	for _, raw := range [][]byte{nil, []byte("{}"), []byte("null")} {
		out, err := es.DecodeTags(raw)
		if err != nil || out != nil {
			t.Fatalf("DecodeTags(%q) = (%#v, %v), want (nil, nil)", raw, out, err)
		}
	}

	for _, raw := range [][]byte{
		[]byte(`[]`),
		[]byte(`{"k":1}`),
		[]byte(`{"Bad":"v"}`),
		[]byte(`{"k":"a\u0000b"}`),
	} {
		if _, err := es.DecodeTags(raw); !errors.Is(err, es.ErrInvalidTag) {
			t.Errorf("DecodeTags(%q) error = %v, want ErrInvalidTag", raw, err)
		}
	}
}

func TestTagQueryValidate(t *testing.T) {
	valid := es.TagQuery{Tags: map[string]string{"k": "v"}}
	if err := valid.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if got := valid.EffectiveLimit(); got != es.DefaultTagReadLimit {
		t.Fatalf("EffectiveLimit() = %d, want %d", got, es.DefaultTagReadLimit)
	}
	if got := (es.TagQuery{Tags: map[string]string{"k": "v"}, Limit: es.MaxTagReadLimit}).EffectiveLimit(); got != es.MaxTagReadLimit {
		t.Fatalf("EffectiveLimit() = %d, want %d", got, es.MaxTagReadLimit)
	}

	invalid := []es.TagQuery{
		{},
		{Tags: map[string]string{"k": "v"}, Limit: -1},
		{Tags: map[string]string{"k": "v"}, Limit: es.MaxTagReadLimit + 1},
		{Tags: map[string]string{"k": "v"}, FromPosition: 5, ToPosition: 4},
		{Tags: map[string]string{"Bad": "v"}},
	}
	for i, q := range invalid {
		if err := q.Validate(); !errors.Is(err, es.ErrInvalidTagQuery) {
			t.Errorf("case %d: Validate(%#v) = %v, want ErrInvalidTagQuery", i, q, err)
		}
	}
}
