package es

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
)

// Token is a read-your-writes consistency token: a scope and a global log
// position. A write returns one; a reader passes it back to wait until the
// projection it reads has applied every event at or below Position in Scope.
//
// Scope names the workspace (or "*" for a store-wide reader); Position is the
// envelope's GlobalPosition. The zero Token means "no position" and is
// compatible with any scope.
type Token struct {
	Scope    string
	Position uint64
}

// Token wire format: one version byte (1), a big-endian u16 scope length, the
// scope bytes, then a big-endian u64 position, base64url-encoded without
// padding. "" is the zero token's wire form.
const (
	tokenVersion   byte = 1
	tokenHeaderLen      = 3 // version + u16 scope length
	tokenTailLen        = 8 // u64 position
)

// String returns the URL-safe wire form of the token. The zero token encodes
// as the empty string.
func (t Token) String() string {
	if t == (Token{}) {
		return ""
	}
	buf := make([]byte, tokenHeaderLen+len(t.Scope)+tokenTailLen)
	buf[0] = tokenVersion
	binary.BigEndian.PutUint16(buf[1:tokenHeaderLen], uint16(len(t.Scope)))
	copy(buf[tokenHeaderLen:], t.Scope)
	binary.BigEndian.PutUint64(buf[tokenHeaderLen+len(t.Scope):], t.Position)
	return base64.RawURLEncoding.EncodeToString(buf)
}

// ParseToken decodes a token's wire form. The empty string is the zero token;
// anything else that is not a well-formed version-1 token is ErrInvalidToken.
func ParseToken(s string) (Token, error) {
	if s == "" {
		return Token{}, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return Token{}, fmt.Errorf("%w: not base64url: %v", ErrInvalidToken, err)
	}
	if len(b) < tokenHeaderLen {
		return Token{}, fmt.Errorf("%w: truncated header", ErrInvalidToken)
	}
	if b[0] != tokenVersion {
		return Token{}, fmt.Errorf("%w: unsupported version %d", ErrInvalidToken, b[0])
	}
	scopeLen := int(binary.BigEndian.Uint16(b[1:tokenHeaderLen]))
	if len(b) != tokenHeaderLen+scopeLen+tokenTailLen {
		return Token{}, fmt.Errorf("%w: body length %d, want %d", ErrInvalidToken, len(b), tokenHeaderLen+scopeLen+tokenTailLen)
	}
	return Token{
		Scope:    string(b[tokenHeaderLen : tokenHeaderLen+scopeLen]),
		Position: binary.BigEndian.Uint64(b[tokenHeaderLen+scopeLen:]),
	}, nil
}

// TokenFor builds the token for an append (or a batch of them): the highest
// AppendResult.Position in results, or 0 when there are none, in scope.
func TokenFor(scope string, results ...AppendResult) Token {
	var pos uint64
	for _, res := range results {
		if res.Position > pos {
			pos = res.Position
		}
	}
	return Token{Scope: scope, Position: pos}
}

// MaxToken returns the further-along of two tokens. A zero token is
// compatible with any scope and yields the other token; two different
// non-empty scopes are ErrScopeMismatch — a token from one workspace can never
// be satisfied by another workspace's progress.
func MaxToken(a, b Token) (Token, error) {
	switch {
	case a == (Token{}):
		return b, nil
	case b == (Token{}):
		return a, nil
	case a.Scope != b.Scope:
		return Token{}, fmt.Errorf("%w: %q vs %q", ErrScopeMismatch, a.Scope, b.Scope)
	case b.Position > a.Position:
		return b, nil
	default:
		return a, nil
	}
}
