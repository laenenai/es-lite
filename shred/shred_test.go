package shred_test

import (
	"context"
	"errors"
	"testing"

	"github.com/laenenai/es-lite/keystore"
	kmem "github.com/laenenai/es-lite/keystore/memory"
	"github.com/laenenai/es-lite/shred"
)

func TestRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := shred.New(kmem.New(), shred.NewMemDEKStore())

	c, err := s.Cipher(ctx, "ws_1")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	plain := []byte("hello, workspace 1")
	blob, err := c.Encrypt(plain)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if string(blob) == string(plain) {
		t.Fatal("ciphertext equals plaintext")
	}
	got, err := c.Decrypt(blob)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if string(got) != string(plain) {
		t.Fatalf("round trip: got %q, want %q", got, plain)
	}
}

// A cold Shredder (fresh cache) sharing the same KeyStore + DEK store must
// decrypt via the unwrap path — the "process restart" case.
func TestUnwrapAfterColdCache(t *testing.T) {
	ctx := context.Background()
	ks := kmem.New()
	deks := shred.NewMemDEKStore()

	warm := shred.New(ks, deks)
	c, _ := warm.Cipher(ctx, "ws_1")
	blob, _ := c.Encrypt([]byte("persisted"))

	cold := shred.New(ks, deks) // empty cache, must unwrap the stored DEK
	c2, err := cold.Cipher(ctx, "ws_1")
	if err != nil {
		t.Fatalf("cold cipher: %v", err)
	}
	got, err := c2.Decrypt(blob)
	if err != nil {
		t.Fatalf("cold decrypt: %v", err)
	}
	if string(got) != "persisted" {
		t.Fatalf("got %q", got)
	}
}

func TestForgetShreds(t *testing.T) {
	ctx := context.Background()
	ks := kmem.New()
	deks := shred.NewMemDEKStore()
	s := shred.New(ks, deks)

	c, _ := s.Cipher(ctx, "ws_1")
	blob, _ := c.Encrypt([]byte("secret"))

	// Capture the real wrapped DEK before erasure.
	wrapped, ok, err := deks.LoadWrappedDEK(ctx, "ws_1")
	if err != nil || !ok {
		t.Fatalf("load wrapped dek: ok=%v err=%v", ok, err)
	}

	if err := s.Forget(ctx, "ws_1"); err != nil {
		t.Fatalf("forget: %v", err)
	}

	// 1. The destroyed KEK can no longer unwrap the genuine wrapped DEK:
	//    the payload is cryptographically erased, even from backups.
	if _, err := ks.UnwrapDEK(ctx, "ws_1", wrapped); !errors.Is(err, keystore.ErrShredded) {
		t.Fatalf("unwrap after forget: got %v, want ErrShredded", err)
	}

	// 2. A later write re-provisions the workspace under a brand-new key,
	//    but the pre-shred ciphertext stays permanently unreadable.
	cold := shred.New(ks, deks)
	c2, err := cold.Cipher(ctx, "ws_1") // mints a fresh KEK+DEK
	if err != nil {
		t.Fatalf("re-provision cipher: %v", err)
	}
	if _, err := c2.Decrypt(blob); err == nil {
		t.Fatal("old ciphertext still readable after shred")
	}
}

// Workspaces are isolated: ws_2's cipher cannot open ws_1's ciphertext.
func TestWorkspaceIsolation(t *testing.T) {
	ctx := context.Background()
	s := shred.New(kmem.New(), shred.NewMemDEKStore())

	c1, _ := s.Cipher(ctx, "ws_1")
	blob, _ := c1.Encrypt([]byte("ws1 data"))

	c2, _ := s.Cipher(ctx, "ws_2")
	if _, err := c2.Decrypt(blob); err == nil {
		t.Fatal("ws_2 decrypted ws_1 ciphertext")
	}
}
