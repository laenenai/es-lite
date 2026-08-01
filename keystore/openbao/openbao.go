// Package openbao implements keystore.KeyStore against OpenBao's (or
// Vault's) Transit secrets engine, using envelope encryption:
//
//   - the per-workspace Transit key is the KEK; its material never leaves
//     OpenBao;
//   - GenerateDEK asks Transit for a datakey (plaintext DEK + wrapped DEK);
//   - UnwrapDEK decrypts the wrapped DEK via Transit;
//   - ForgetWorkspace deletes the Transit key — the crypto-shred.
//
// It speaks the Transit HTTP API directly (net/http + JSON) rather than
// pulling the Vault SDK, keeping es-lite dependency-light. See ADR 0004
// (crypto-shredding) and ADR 0006 (this adapter).
package openbao

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/laenenai/es-lite/keystore"
)

// Config configures a Client.
type Config struct {
	// Address is the OpenBao base URL, e.g. "https://bao.internal:8200".
	Address string
	// Token authenticates to OpenBao (X-Vault-Token). In production this
	// comes from Kubernetes auth, not a static token.
	Token string
	// Mount is the Transit engine mount path. Default "transit".
	Mount string
	// KeyPrefix is prepended to the workspace id to form the Transit key
	// name (e.g. "es-lite-"). Optional.
	KeyPrefix string
	// HTTPClient overrides the default (10s timeout) client.
	HTTPClient *http.Client
}

// Client is an OpenBao Transit-backed keystore.KeyStore.
type Client struct {
	cfg  Config
	http *http.Client
}

var _ keystore.KeyStore = (*Client)(nil)

// New constructs a Client. Address and Token are required.
func New(cfg Config) (*Client, error) {
	if cfg.Address == "" {
		return nil, fmt.Errorf("openbao: Address is required")
	}
	if cfg.Token == "" {
		return nil, fmt.Errorf("openbao: Token is required")
	}
	if cfg.Mount == "" {
		cfg.Mount = "transit"
	}
	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: 10 * time.Second}
	}
	cfg.Address = strings.TrimRight(cfg.Address, "/")
	return &Client{cfg: cfg, http: hc}, nil
}

func (c *Client) keyName(workspaceID string) string { return c.cfg.KeyPrefix + workspaceID }

// GenerateDEK ensures the workspace Transit key exists, then requests a
// datakey. The returned wrapped bytes are the Transit ciphertext token
// ("vault:vN:...").
func (c *Client) GenerateDEK(ctx context.Context, workspaceID string) ([]byte, []byte, int, error) {
	if err := c.ensureKey(ctx, workspaceID); err != nil {
		return nil, nil, 0, err
	}
	var out struct {
		Data struct {
			Plaintext  string `json:"plaintext"`
			Ciphertext string `json:"ciphertext"`
		} `json:"data"`
	}
	path := fmt.Sprintf("/v1/%s/datakey/plaintext/%s", c.cfg.Mount, c.keyName(workspaceID))
	if _, err := c.do(ctx, http.MethodPost, path, map[string]any{"bits": 256}, &out); err != nil {
		return nil, nil, 0, fmt.Errorf("openbao: datakey: %w", err)
	}
	dek, err := base64.StdEncoding.DecodeString(out.Data.Plaintext)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("openbao: decode datakey plaintext: %w", err)
	}
	return dek, []byte(out.Data.Ciphertext), keyVersion(out.Data.Ciphertext), nil
}

// UnwrapDEK decrypts a wrapped DEK. A destroyed key yields ErrShredded.
func (c *Client) UnwrapDEK(ctx context.Context, workspaceID string, wrapped []byte) ([]byte, error) {
	var out struct {
		Data struct {
			Plaintext string `json:"plaintext"`
		} `json:"data"`
	}
	path := fmt.Sprintf("/v1/%s/decrypt/%s", c.cfg.Mount, c.keyName(workspaceID))
	status, err := c.do(ctx, http.MethodPost, path, map[string]any{"ciphertext": string(wrapped)}, &out)
	if err != nil {
		if isMissingKey(status, err) {
			return nil, fmt.Errorf("%w: %s", keystore.ErrShredded, workspaceID)
		}
		return nil, fmt.Errorf("openbao: decrypt: %w", err)
	}
	dek, err := base64.StdEncoding.DecodeString(out.Data.Plaintext)
	if err != nil {
		return nil, fmt.Errorf("openbao: decode plaintext: %w", err)
	}
	return dek, nil
}

// ForgetWorkspace deletes the workspace Transit key (crypto-shred).
// Idempotent: a 404 for an already-absent key is success.
func (c *Client) ForgetWorkspace(ctx context.Context, workspaceID string) error {
	path := fmt.Sprintf("/v1/%s/keys/%s", c.cfg.Mount, c.keyName(workspaceID))
	status, err := c.do(ctx, http.MethodDelete, path, nil, nil)
	if err != nil && status != http.StatusNotFound {
		return fmt.Errorf("openbao: delete key: %w", err)
	}
	return nil
}

// ensureKey creates the Transit key if absent and marks it deletable so
// ForgetWorkspace can later destroy it.
func (c *Client) ensureKey(ctx context.Context, workspaceID string) error {
	name := c.keyName(workspaceID)
	createPath := fmt.Sprintf("/v1/%s/keys/%s", c.cfg.Mount, name)
	// Creating an existing key is a no-op success in Transit; ignore the
	// benign "already exists" path by not treating 400 here as fatal only
	// when the config step below succeeds.
	if _, err := c.do(ctx, http.MethodPost, createPath, map[string]any{"type": "aes256-gcm96"}, nil); err != nil {
		// fall through: the key may already exist; the config call verifies.
	}
	cfgPath := fmt.Sprintf("/v1/%s/keys/%s/config", c.cfg.Mount, name)
	if _, err := c.do(ctx, http.MethodPost, cfgPath, map[string]any{"deletion_allowed": true}, nil); err != nil {
		return fmt.Errorf("openbao: enable deletion on key %q: %w", name, err)
	}
	return nil
}

// do issues one Transit request. It returns the HTTP status and, on a
// non-2xx, an error carrying OpenBao's error messages. out may be nil.
func (c *Client) do(ctx context.Context, method, path string, body any, out any) (int, error) {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.cfg.Address+path, reader)
	if err != nil {
		return 0, err
	}
	req.Header.Set("X-Vault-Token", c.cfg.Token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf("status %d: %s", resp.StatusCode, baoErrors(raw))
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return resp.StatusCode, fmt.Errorf("decode response: %w", err)
		}
	}
	return resp.StatusCode, nil
}

// baoErrors extracts the JSON {"errors":[...]} body OpenBao returns.
func baoErrors(raw []byte) string {
	var e struct {
		Errors []string `json:"errors"`
	}
	if json.Unmarshal(raw, &e) == nil && len(e.Errors) > 0 {
		return strings.Join(e.Errors, "; ")
	}
	return string(raw)
}

// isMissingKey reports whether an error/status indicates the Transit key
// is gone (i.e. the workspace was shredded), vs. a transient/other failure.
func isMissingKey(status int, err error) bool {
	if status == http.StatusNotFound {
		return true
	}
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "encryption key not found") ||
		strings.Contains(msg, "could not be found") ||
		strings.Contains(msg, "no existing key") ||
		strings.Contains(msg, "unknown key")
}

// keyVersion parses N from a "vault:vN:..." Transit ciphertext token.
func keyVersion(ciphertext string) int {
	parts := strings.SplitN(ciphertext, ":", 3)
	if len(parts) < 2 || !strings.HasPrefix(parts[1], "v") {
		return 1
	}
	if n, err := strconv.Atoi(parts[1][1:]); err == nil {
		return n
	}
	return 1
}
