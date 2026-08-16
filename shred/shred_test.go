package shred_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/laenenai/es-lite/internal/memdek"
	"github.com/laenenai/es-lite/keystore"
	kmem "github.com/laenenai/es-lite/keystore/memory"
	"github.com/laenenai/es-lite/shred"
)

func TestAADBinding(t *testing.T) {
	ctx := context.Background()
	s := shred.New(kmem.New(), memdek.New())
	c, err := s.Cipher(ctx, "ws_1")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	plain := []byte("bound to a stream")
	aadA := []byte("ws_1|user:alice")
	aadB := []byte("ws_1|user:mallory")

	blob, err := c.EncryptWithAAD(plain, aadA)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	// Same AAD opens.
	got, err := c.DecryptWithAAD(blob, aadA)
	if err != nil || string(got) != string(plain) {
		t.Fatalf("same-AAD decrypt: got %q err %v", got, err)
	}
	// Different AAD (replay into another stream) MUST fail.
	if _, err := c.DecryptWithAAD(blob, aadB); err == nil {
		t.Fatal("decrypt under different AAD succeeded — replay not prevented")
	}
	// Nil AAD (unbound) also fails against an AAD-bound ciphertext.
	if _, err := c.Decrypt(blob); err == nil {
		t.Fatal("decrypt with nil AAD opened an AAD-bound ciphertext")
	}
}

func TestRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := shred.New(kmem.New(), memdek.New())

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
	deks := memdek.New()

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
	deks := memdek.New()
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
	s := shred.New(kmem.New(), memdek.New())

	c1, _ := s.Cipher(ctx, "ws_1")
	blob, _ := c1.Encrypt([]byte("ws1 data"))

	c2, _ := s.Cipher(ctx, "ws_2")
	if _, err := c2.Decrypt(blob); err == nil {
		t.Fatal("ws_2 decrypted ws_1 ciphertext")
	}
}

// A first-write race must never lose data (2026-08-05 review C6). Many goroutines write a
// brand-new workspace at once; each mints a DEK, but SaveWrappedDEK is insert-if-absent so
// only one is persisted. Every writer must converge on the PERSISTED key — otherwise a
// writer that lost the race encrypts under a key that was never stored, and its ciphertext
// is permanently unrecoverable. We prove convergence by decrypting every writer's ciphertext
// with a COLD reader, which holds only the persisted DEK. Run with -race.
func TestFirstWriteRaceConverges(t *testing.T) {
	ctx := context.Background()
	ks := kmem.New()
	deks := memdek.New()
	s := shred.New(ks, deks)

	const n = 32
	var wg sync.WaitGroup
	blobs := make([][]byte, n)
	errs := make([]error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start // release all goroutines together to maximise first-write contention
			c, err := s.WriteCipher(ctx, "ws_race")
			if err != nil {
				errs[i] = err
				return
			}
			blobs[i], errs[i] = c.Encrypt([]byte("payload"))
		}(i)
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("writer %d: %v", i, err)
		}
	}

	// Cold reader: fresh cache, so it can only use the persisted DEK. If any writer had
	// used its own unpersisted DEK, its ciphertext fails to open here — that is the C6 bug.
	cold := shred.New(ks, deks)
	rc, err := cold.ReadCipher(ctx, "ws_race")
	if err != nil {
		t.Fatalf("cold read cipher: %v", err)
	}
	for i, blob := range blobs {
		got, err := rc.Decrypt(blob)
		if err != nil {
			t.Fatalf("writer %d ciphertext undecryptable under the persisted DEK (data loss): %v", i, err)
		}
		if string(got) != "payload" {
			t.Fatalf("writer %d: got %q", i, got)
		}
	}
}
