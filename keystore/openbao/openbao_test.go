package openbao_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/laenenai/es-lite/keystore"
	"github.com/laenenai/es-lite/keystore/openbao"
)

// Integration test against a real OpenBao/Vault. Skips unless OPENBAO_ADDR
// and OPENBAO_TOKEN are set. To run locally:
//
//	docker run -d --rm --name bao -p 8200:8200 --cap-add=IPC_LOCK \
//	  -e BAO_DEV_ROOT_TOKEN_ID=root openbao/openbao server -dev
//	OPENBAO_ADDR=http://127.0.0.1:8200 OPENBAO_TOKEN=root go test ./keystore/openbao/...
func TestTransitRoundTripAndShred(t *testing.T) {
	addr := os.Getenv("OPENBAO_ADDR")
	token := os.Getenv("OPENBAO_TOKEN")
	if addr == "" || token == "" {
		t.Skip("set OPENBAO_ADDR and OPENBAO_TOKEN to run the OpenBao integration test")
	}
	ctx := context.Background()
	enableTransit(t, addr, token)

	ks, err := openbao.New(openbao.Config{Address: addr, Token: token, KeyPrefix: "es-lite-test-"})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	ws := "ws_" + time.Now().UTC().Format("150405.000000000")

	dek, wrapped, ver, err := ks.GenerateDEK(ctx, ws)
	if err != nil {
		t.Fatalf("generate dek: %v", err)
	}
	if len(dek) != keystore.DEKSize {
		t.Fatalf("dek len = %d, want %d", len(dek), keystore.DEKSize)
	}
	if ver < 1 {
		t.Fatalf("kek version = %d", ver)
	}

	got, err := ks.UnwrapDEK(ctx, ws, wrapped)
	if err != nil {
		t.Fatalf("unwrap: %v", err)
	}
	if !bytes.Equal(got, dek) {
		t.Fatal("unwrapped dek != generated dek")
	}

	if err := ks.ForgetWorkspace(ctx, ws); err != nil {
		t.Fatalf("forget: %v", err)
	}
	if _, err := ks.UnwrapDEK(ctx, ws, wrapped); !errors.Is(err, keystore.ErrShredded) {
		t.Fatalf("unwrap after forget: got %v, want ErrShredded", err)
	}
}

// enableTransit mounts the transit engine if it is not already mounted.
func enableTransit(t *testing.T, addr, token string) {
	t.Helper()
	body := bytes.NewReader([]byte(`{"type":"transit"}`))
	req, _ := http.NewRequest(http.MethodPost, addr+"/v1/sys/mounts/transit", body)
	req.Header.Set("X-Vault-Token", token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("enable transit: %v", err)
	}
	defer resp.Body.Close()
	// 204 = mounted now; 400 = already mounted. Both are fine.
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("enable transit: unexpected status %d", resp.StatusCode)
	}
}
