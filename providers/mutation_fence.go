package providers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
)

// MutationFence is the worker-owned authorization seam checked immediately
// before a provider request that might mutate external state. Implementations
// normally re-read the run journal and assert the retained business-claim epoch;
// possession of a credential or of an earlier claim token is never sufficient.
type MutationFence interface {
	AuthorizeProviderMutation(context.Context, ProviderMutation) error
}

// ProviderMutation identifies one provider request at the authorization seam.
// IdempotencyKey is stable for the same attempt, method, endpoint, and canonical
// request body. Providers need not support that key natively; it lets a broker
// or test double deduplicate a restarted worker without inventing workflow
// authority outside the run journal.
type ProviderMutation struct {
	Provider       ProviderKind
	Method         string
	Endpoint       string
	IdempotencyKey string
}

type mutationFenceContext struct {
	fence   MutationFence
	binding string
}

type mutationFenceContextKey struct{}

// WithMutationFence binds a fence to provider calls made with ctx. binding must
// identify the executable attempt (including its fencing epoch); it is included
// in every deterministic mutation idempotency key.
func WithMutationFence(ctx context.Context, binding string, fence MutationFence) context.Context {
	if fence == nil {
		return ctx
	}
	return context.WithValue(ctx, mutationFenceContextKey{}, mutationFenceContext{fence: fence, binding: binding})
}

func authorizeProviderMutation(ctx context.Context, provider ProviderKind, method, endpoint string, body interface{}) error {
	if method == http.MethodGet || method == http.MethodHead || method == http.MethodOptions {
		return nil
	}
	bound, ok := ctx.Value(mutationFenceContextKey{}).(mutationFenceContext)
	if !ok || bound.fence == nil {
		// Existing local-runner provider paths do not attach this context and keep
		// their established behavior. The Kubernetes worker independently refuses
		// a mutation-capable executor unless it advertises this fenced path.
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("provider mutation fence: encode request identity: %w", err)
	}
	sum := sha256.Sum256(append([]byte(bound.binding+"\x00"+string(provider)+"\x00"+method+"\x00"+endpoint+"\x00"), encoded...))
	mutation := ProviderMutation{
		Provider:       provider,
		Method:         method,
		Endpoint:       endpoint,
		IdempotencyKey: "sha256:" + hex.EncodeToString(sum[:]),
	}
	if err := bound.fence.AuthorizeProviderMutation(ctx, mutation); err != nil {
		return fmt.Errorf("provider mutation fence: %w", err)
	}
	return nil
}
