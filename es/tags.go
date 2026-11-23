package es

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

// Event tags (design 2026-11-23). A tag is a flat string-to-string pair
// stored on the event (events.tags) and projected into the event_tags index.
// The vocabulary is deliberately small: a bounded key and value, a bounded
// count, and ASCII keys so a btree text index behaves predictably.
const (
	// MaxTagKeyBytes is the maximum tag-key length in bytes. The key
	// charset is ASCII, so this is also its maximum rune count.
	MaxTagKeyBytes = 128
	// MaxTagValueBytes is the maximum tag-value length in bytes.
	MaxTagValueBytes = 512
	// MaxTagsPerEvent is the maximum number of tags on one merged event:
	// the batch tags overlaid with the event's own tags.
	MaxTagsPerEvent = 32
	// DefaultTagReadLimit is the page size ReadByTags uses when the query
	// leaves Limit unset.
	DefaultTagReadLimit = 500
	// MaxTagReadLimit is the largest page ReadByTags accepts.
	MaxTagReadLimit = 1000
)

// tagKeyPattern is the tag-key charset: lowercase ASCII letters, digits,
// dot, underscore, colon and hyphen.
var tagKeyPattern = regexp.MustCompile(`^[a-z0-9._:-]+$`)

// TagQuery selects events carrying every tag in Tags. FromPosition is
// exclusive, ToPosition inclusive (0 = no upper bound), and Limit is the
// page size (0 = DefaultTagReadLimit). A query with no tags is invalid:
// "everything" is ReadAll, not a tag query.
type TagQuery struct {
	Tags         map[string]string
	FromPosition uint64
	ToPosition   uint64
	Limit        int
}

// Validate reports whether the query is well formed: at least one tag, a
// valid tag set, a limit in [0, MaxTagReadLimit], and an ordered range.
func (q TagQuery) Validate() error {
	if len(q.Tags) == 0 {
		return fmt.Errorf("%w: at least one tag is required", ErrInvalidTagQuery)
	}
	for k, v := range q.Tags {
		if err := ValidateTag(k, v); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidTagQuery, err)
		}
	}
	if q.Limit < 0 || q.Limit > MaxTagReadLimit {
		return fmt.Errorf("%w: limit %d outside [0,%d]", ErrInvalidTagQuery, q.Limit, MaxTagReadLimit)
	}
	if q.ToPosition != 0 && q.ToPosition < q.FromPosition {
		return fmt.Errorf("%w: to_position %d is before from_position %d",
			ErrInvalidTagQuery, q.ToPosition, q.FromPosition)
	}
	return nil
}

// EffectiveLimit returns Limit, or DefaultTagReadLimit when Limit is 0.
func (q TagQuery) EffectiveLimit() int {
	if q.Limit == 0 {
		return DefaultTagReadLimit
	}
	return q.Limit
}

// ValidateTag checks one tag pair against the vocabulary. The key must be
// 1-128 bytes matching [a-z0-9._:-]+; the value must be at most 512 bytes of
// valid UTF-8 with no NUL. The empty value is allowed.
func ValidateTag(key, value string) error {
	if len(key) == 0 || len(key) > MaxTagKeyBytes {
		return fmt.Errorf("%w: key length %d outside [1,%d]",
			ErrInvalidTag, len(key), MaxTagKeyBytes)
	}
	if !tagKeyPattern.MatchString(key) {
		return fmt.Errorf("%w: key %q must match %s", ErrInvalidTag, key, tagKeyPattern)
	}
	if len(value) > MaxTagValueBytes {
		return fmt.Errorf("%w: value length %d exceeds %d",
			ErrInvalidTag, len(value), MaxTagValueBytes)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("%w: value is not valid UTF-8", ErrInvalidTag)
	}
	if strings.IndexByte(value, 0) >= 0 {
		return fmt.Errorf("%w: value contains a NUL byte", ErrInvalidTag)
	}
	return nil
}

// MergedTags overlays each event's own tags onto the batch tags and returns
// one merged set per event, in input order. A nil entry means the event has
// no tags. A key present in both sets with different values is
// ErrConflictingTag; the same value simply dedupes. The merged set is capped
// at MaxTagsPerEvent. It does not depend on EventData, so the core tag
// vocabulary compiles before the append fields land.
func MergedTags(batch map[string]string, perEvent []map[string]string) ([]map[string]string, error) {
	if len(batch) > MaxTagsPerEvent {
		return nil, fmt.Errorf("%w: %d batch tags, max %d",
			ErrTooManyTags, len(batch), MaxTagsPerEvent)
	}
	for k, v := range batch {
		if err := ValidateTag(k, v); err != nil {
			return nil, err
		}
	}

	out := make([]map[string]string, len(perEvent))
	for i, event := range perEvent {
		for k, v := range event {
			if err := ValidateTag(k, v); err != nil {
				return nil, err
			}
		}
		if len(batch) == 0 && len(event) == 0 {
			continue
		}
		merged := make(map[string]string, len(batch)+len(event))
		for k, v := range batch {
			merged[k] = v
		}
		for k, v := range event {
			if existing, ok := merged[k]; ok && existing != v {
				return nil, fmt.Errorf("%w: %q is %q in the batch and %q on the event",
					ErrConflictingTag, k, existing, v)
			}
			merged[k] = v
		}
		if len(merged) > MaxTagsPerEvent {
			return nil, fmt.Errorf("%w: %d merged tags, max %d",
				ErrTooManyTags, len(merged), MaxTagsPerEvent)
		}
		out[i] = merged
	}
	return out, nil
}

// EncodeTags returns the JSON object stored in events.tags, or nil for an
// empty set. Go's json encoder sorts map keys, so the stored text is stable.
func EncodeTags(tags map[string]string) ([]byte, error) {
	if len(tags) == 0 {
		return nil, nil
	}
	if len(tags) > MaxTagsPerEvent {
		return nil, fmt.Errorf("%w: %d tags, max %d", ErrTooManyTags, len(tags), MaxTagsPerEvent)
	}
	for k, v := range tags {
		if err := ValidateTag(k, v); err != nil {
			return nil, err
		}
	}
	raw, err := json.Marshal(tags)
	if err != nil {
		return nil, fmt.Errorf("%w: encode: %v", ErrInvalidTag, err)
	}
	return raw, nil
}

// DecodeTags parses a stored tags column. A nil, empty, "null" or "{}" input
// reads as no tags. A non-object, a non-string value, an invalid key/value or
// an over-cap set is corrupt and fails loudly: the projection and the change
// feed depend on the tags being exactly what the append wrote.
func DecodeTags(raw []byte) (map[string]string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var tags map[string]string
	if err := json.Unmarshal(raw, &tags); err != nil {
		return nil, fmt.Errorf("%w: stored tags: %v", ErrInvalidTag, err)
	}
	if len(tags) == 0 {
		return nil, nil
	}
	if len(tags) > MaxTagsPerEvent {
		return nil, fmt.Errorf("%w: %d stored tags, max %d",
			ErrTooManyTags, len(tags), MaxTagsPerEvent)
	}
	for k, v := range tags {
		if err := ValidateTag(k, v); err != nil {
			return nil, fmt.Errorf("%w: stored tags: %v", ErrInvalidTag, err)
		}
	}
	return tags, nil
}

// TagReader is implemented by stores that can query events by tag. It is a
// capability interface, not a method on Store, so adding it cannot break a
// third-party store.
type TagReader interface {
	// ReadByTags returns events carrying every tag in q.Tags, ordered by
	// ascending global position. FromPosition is exclusive, ToPosition
	// inclusive. The caller paginates with the last returned
	// GlobalPosition. An empty result is (nil, nil), not ErrStreamNotFound.
	ReadByTags(ctx context.Context, q TagQuery) ([]Envelope, error)
}

// TagsRebuildReport summarizes a successful RebuildTags.
type TagsRebuildReport struct {
	Events, Tags int64
}

// TagsVerifyReport is the result of VerifyTags: whether the live event_tags
// projection equals a rebuild from events.tags, with the first divergence
// when it does not.
type TagsVerifyReport struct {
	OK              bool
	Events, Tags    int64
	FirstDivergence string
}

// TagRowMismatch is the first log_order batch whose recomputed tags hash
// differs from the stored tags_hash.
type TagRowMismatch struct {
	From             uint64
	ExpectedTagsHash []byte
	ActualTagsHash   []byte
}

// TagsHashVerifyReport is the result of TagAttester.VerifyTagHashes.
type TagsHashVerifyReport struct {
	OK          bool
	RowsChecked uint64
	Mismatch    *TagRowMismatch
}

// TagMaintainer is implemented by stores that can reconstruct the event_tags
// projection from events.tags. Like ClaimsMaintainer, it exists for restore,
// clone and verification; the append path maintains the live index.
type TagMaintainer interface {
	// RebuildTags replaces the live projection by re-projecting every
	// tagged event. It requires quiescence and refuses a stored tag value
	// that is not a string rather than stringifying it.
	RebuildTags(ctx context.Context) (TagsRebuildReport, error)

	// VerifyTags rebuilds into a temporary projection and compares it with
	// the live one, reporting the first divergence. It does not modify the
	// live index.
	VerifyTags(ctx context.Context) (TagsVerifyReport, error)
}

// TagAttester is implemented by stores that can recompute each batch's tags
// hash from the events and compare it with log_order.tags_hash.
type TagAttester interface {
	VerifyTagHashes(ctx context.Context, upto uint64) (*TagsHashVerifyReport, error)
}
