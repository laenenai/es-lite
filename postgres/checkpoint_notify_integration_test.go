package postgres_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/laenenai/es-lite/postgres"
)

// TestCheckpointNotifyFiresOnSave pins the C2 shape for the marker
// channel: with WithCheckpointNotify set, a committed checkpoint save
// reaches a LISTENing connection with the channel and the scope as the
// payload. The notification is emitted in the same transaction as the
// upsert, so it can only describe a durable advance.
func TestCheckpointNotifyFiresOnSave(t *testing.T) {
	const channel = "es_lite_checkpoint_fires"
	ctx := context.Background()
	lc := listenConn(t, channel)
	s := openStoreWithOptions(t, postgres.WithCheckpointNotify(channel))
	checkpoints := postgres.NewCheckpointStore(s)
	const scope = "ws_checkpoint_fires"

	if err := checkpoints.Save(ctx, scope, "iam.tenant", "live", "", 7); err != nil {
		t.Fatalf("save: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	n, err := lc.WaitForNotification(waitCtx)
	if err != nil {
		t.Fatalf("wait for notification: %v", err)
	}
	if n.Channel != channel {
		t.Fatalf("notification channel = %q, want %q", n.Channel, channel)
	}
	if n.Payload != scope {
		t.Fatalf("notification payload = %q, want the scope %q", n.Payload, scope)
	}
}

// TestCheckpointNotifyOffDoesNotFire pins the default: opened without
// WithCheckpointNotify, a save commits and issues no notification. The
// option is opt-in and the default save path is unchanged.
func TestCheckpointNotifyOffDoesNotFire(t *testing.T) {
	const channel = "es_lite_checkpoint_off"
	ctx := context.Background()
	lc := listenConn(t, channel)
	s := openStore(t) // no WithCheckpointNotify
	checkpoints := postgres.NewCheckpointStore(s)

	if err := checkpoints.Save(ctx, "ws_checkpoint_off", "iam.tenant", "live", "", 7); err != nil {
		t.Fatalf("save: %v", err)
	}

	assertNoNotification(t, lc, 100*time.Millisecond)
}

// TestCheckpointResetDoesNotFire pins the boundary: Reset is a deliberate
// replay decision, not an advance. A wake for it would only cost every
// waiter a fenced read that shows no new data.
func TestCheckpointResetDoesNotFire(t *testing.T) {
	const channel = "es_lite_checkpoint_reset"
	ctx := context.Background()
	lc := listenConn(t, channel)
	s := openStoreWithOptions(t, postgres.WithCheckpointNotify(channel))
	checkpoints := postgres.NewCheckpointStore(s)

	if err := checkpoints.Reset(ctx, "ws_checkpoint_reset", "iam.tenant", "live", "", 3); err != nil {
		t.Fatalf("reset: %v", err)
	}

	assertNoNotification(t, lc, 100*time.Millisecond)
}

// TestCheckpointNotifyRejectsAnOversizedScope pins the payload bound and
// the transaction: an oversized scope fails the save, notifies nobody,
// and leaves no checkpoint row behind.
func TestCheckpointNotifyRejectsAnOversizedScope(t *testing.T) {
	const channel = "es_lite_checkpoint_oversized"
	ctx := context.Background()
	lc := listenConn(t, channel)
	s := openStoreWithOptions(t, postgres.WithCheckpointNotify(channel))
	checkpoints := postgres.NewCheckpointStore(s)
	scope := strings.Repeat("s", 513)

	err := checkpoints.Save(ctx, scope, "iam.tenant", "live", "", 7)
	if err == nil || !strings.Contains(err.Error(), "notify bound") {
		t.Fatalf("save = %v, want a notify-bound error", err)
	}
	assertNoNotification(t, lc, 100*time.Millisecond)

	_, found, err := checkpoints.Checkpoint(ctx, scope, "iam.tenant", "live")
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if found {
		t.Fatal("the failed save left a checkpoint row behind")
	}
}
