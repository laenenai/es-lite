package es_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/laenenai/es-lite/es"
)

func TestTokenRoundTrip(t *testing.T) {
	tok := es.Token{Scope: "ws_a", Position: 42}
	got, err := es.ParseToken(tok.String())
	if err != nil || got != tok {
		t.Fatalf("round trip = (%+v, %v), want %+v", got, err, tok)
	}
	if tok.String() == "" || strings.ContainsAny(tok.String(), "+/=") {
		t.Fatalf("not URL-safe: %q", tok.String())
	}
}

func TestParseTokenRejectsMalformed(t *testing.T) {
	for _, s := range []string{"!!!!", "AQ", "AQAAAAA"} { // not base64 / truncated / bad version body
		if _, err := es.ParseToken(s); !errors.Is(err, es.ErrInvalidToken) {
			t.Errorf("%q: %v, want ErrInvalidToken", s, err)
		}
	}
}

func TestZeroTokenAndMaxToken(t *testing.T) {
	if got, err := es.ParseToken(""); err != nil || got != (es.Token{}) {
		t.Fatalf("ParseToken(\"\") = (%+v, %v), want zero token", got, err)
	}
	a := es.Token{Scope: "ws", Position: 5}
	b := es.Token{Scope: "ws", Position: 9}
	if got, err := es.MaxToken(a, b); err != nil || got.Position != 9 {
		t.Fatalf("MaxToken(a, b) = (%+v, %v), want position 9", got, err)
	}
	if got, err := es.MaxToken(b, a); err != nil || got.Position != 9 {
		t.Fatalf("MaxToken(b, a) = (%+v, %v), want position 9", got, err)
	}
	if _, err := es.MaxToken(a, es.Token{Scope: "other", Position: 9}); !errors.Is(err, es.ErrScopeMismatch) {
		t.Fatalf("MaxToken across scopes: %v, want ErrScopeMismatch", err)
	}
}

func TestMaxTokenZero(t *testing.T) {
	real := es.Token{Scope: "ws", Position: 7}
	if got, err := es.MaxToken(es.Token{}, real); err != nil || got != real {
		t.Fatalf("MaxToken(zero, real) = (%+v, %v), want %+v", got, err, real)
	}
	if got, err := es.MaxToken(real, es.Token{}); err != nil || got != real {
		t.Fatalf("MaxToken(real, zero) = (%+v, %v), want %+v", got, err, real)
	}
	if got, err := es.MaxToken(es.Token{}, es.Token{}); err != nil || got != (es.Token{}) {
		t.Fatalf("MaxToken(zero, zero) = (%+v, %v), want zero token", got, err)
	}
}

func TestTokenFor(t *testing.T) {
	got := es.TokenFor("ws", es.AppendResult{Position: 3}, es.AppendResult{Position: 8})
	if got.Position != 8 || got.Scope != "ws" {
		t.Fatalf("TokenFor = %+v, want {ws 8}", got)
	}
	if none := es.TokenFor("ws"); none != (es.Token{Scope: "ws"}) {
		t.Fatalf("TokenFor with no results = %+v, want {ws 0}", none)
	}
}
