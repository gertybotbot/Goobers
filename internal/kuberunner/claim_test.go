package kuberunner

import (
	"context"
	"errors"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestBusinessClaimFencingIsMonotonicAndStaleWorkersFailClosed(t *testing.T) {
	now := time.Date(2026, 8, 19, 14, 0, 0, 0, time.UTC)
	c := fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build()
	store := &KubeClaimStore{Client: c, Namespace: testNamespace, Now: func() time.Time { return now }}
	key := ClaimKey{Gaggle: "web", Provider: "github", ExternalID: "42"}
	ctx := context.Background()

	first, err := store.Acquire(ctx, testNamespace, key, "run-1", "uid-1", time.Minute)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if first.Epoch != 1 {
		t.Fatalf("first epoch = %d, want 1", first.Epoch)
	}
	// Caller namespaces cannot split authority: the store writes every claim to
	// its one configured authority namespace.
	if _, err := store.Acquire(ctx, "another-run-namespace", key, "run-2", "uid-2", time.Minute); !errors.Is(err, ErrClaimHeld) {
		t.Fatalf("concurrent cross-namespace acquire error = %v, want ErrClaimHeld", err)
	}

	// A restarted controller for the same run occurrence recovers the same
	// epoch; it does not mint a second authority token.
	recovered, err := store.Acquire(ctx, testNamespace, key, "run-1", "uid-1", time.Minute)
	if err != nil {
		t.Fatalf("restart acquire: %v", err)
	}
	if recovered.Epoch != first.Epoch {
		t.Fatalf("restart epoch = %d, want unchanged %d", recovered.Epoch, first.Epoch)
	}

	now = now.Add(2 * time.Minute)
	second, err := store.Acquire(ctx, testNamespace, key, "run-2", "uid-2", time.Minute)
	if err != nil {
		t.Fatalf("take over expired claim: %v", err)
	}
	if second.Epoch != 2 {
		t.Fatalf("takeover epoch = %d, want 2", second.Epoch)
	}
	if err := store.AssertCurrent(ctx, testNamespace, first); !errors.Is(err, ErrClaimFenceLost) {
		t.Fatalf("stale assert error = %v, want ErrClaimFenceLost", err)
	}
	if _, err := store.Renew(ctx, testNamespace, first, time.Minute); !errors.Is(err, ErrClaimFenceLost) {
		t.Fatalf("stale renew error = %v, want ErrClaimFenceLost", err)
	}
	if err := store.Release(ctx, testNamespace, first); !errors.Is(err, ErrClaimFenceLost) {
		t.Fatalf("stale release error = %v, want ErrClaimFenceLost", err)
	}

	if err := store.Release(ctx, testNamespace, second); err != nil {
		t.Fatalf("release current claim: %v", err)
	}
	// Release is idempotent across a crash before claim.released is journalled.
	if err := store.Release(ctx, testNamespace, second); err != nil {
		t.Fatalf("repeat release: %v", err)
	}
	third, err := store.Acquire(ctx, testNamespace, key, "run-3", "uid-3", time.Minute)
	if err != nil {
		t.Fatalf("acquire released claim: %v", err)
	}
	if third.Epoch != 3 {
		t.Fatalf("post-release epoch = %d, want 3 (released epochs are retained)", third.Epoch)
	}
}
