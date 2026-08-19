package providers

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

type recordingMutationFence struct {
	err      error
	requests []ProviderMutation
}

func (f *recordingMutationFence) AuthorizeProviderMutation(_ context.Context, mutation ProviderMutation) error {
	f.requests = append(f.requests, mutation)
	return f.err
}

type countingHTTPClient struct{ calls int }

func (c *countingHTTPClient) Do(*http.Request) (*http.Response, error) {
	c.calls++
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`))}, nil
}

func TestProviderMutationFenceRejectsBeforeTheSideEffect(t *testing.T) {
	denied := errors.New("stale fencing epoch")
	fence := &recordingMutationFence{err: denied}
	client := &countingHTTPClient{}
	provider := NewGitHubProvider("secret", WithHTTPClient(client))
	ctx := WithMutationFence(context.Background(), "run/uid/build/1/fence-7", fence)

	_, err := provider.send(ctx, http.MethodPost, "https://api.github.test/repos/acme/web/issues", map[string]string{"title": "work"})
	if !errors.Is(err, denied) {
		t.Fatalf("send error = %v, want stale-fence refusal", err)
	}
	if client.calls != 0 {
		t.Fatalf("provider side effects = %d, want 0", client.calls)
	}
	if len(fence.requests) != 1 || fence.requests[0].Provider != ProviderGitHub || fence.requests[0].IdempotencyKey == "" {
		t.Fatalf("fence requests = %+v, want one addressed mutation", fence.requests)
	}
}

func TestProviderMutationFenceUsesStableAttemptBoundIdempotencyKeys(t *testing.T) {
	fence := &recordingMutationFence{}
	client := &countingHTTPClient{}
	provider := NewGiteaProvider("https://gitea.test", "secret", WithGiteaHTTPClient(client))
	ctx := WithMutationFence(context.Background(), "run/uid/build/1/fence-7", fence)
	body := map[string]string{"body": "hello"}

	for range 2 {
		resp, err := provider.send(ctx, http.MethodPost, "https://gitea.test/api/v1/repos/acme/web/issues/1/comments", body)
		if err != nil {
			t.Fatalf("send: %v", err)
		}
		_ = resp.Body.Close()
	}
	if len(fence.requests) != 2 || fence.requests[0].IdempotencyKey != fence.requests[1].IdempotencyKey {
		t.Fatalf("idempotency keys = %+v, want stable keys", fence.requests)
	}
	if client.calls != 2 {
		t.Fatalf("provider calls = %d, want 2 authorized calls", client.calls)
	}
}

func TestProviderMutationFenceDoesNotGateReadsAndHonorsCancellation(t *testing.T) {
	fence := &recordingMutationFence{}
	client := &countingHTTPClient{}
	provider := NewADOProvider("org", "project", "secret", func(p *ADOProvider) { p.Client = client })
	ctx := WithMutationFence(context.Background(), "attempt", fence)

	resp, err := provider.send(ctx, http.MethodGet, "https://dev.azure.test/read", nil, "")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	_ = resp.Body.Close()
	if len(fence.requests) != 0 || client.calls != 1 {
		t.Fatalf("read fence/calls = %d/%d, want 0/1", len(fence.requests), client.calls)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = provider.send(cancelled, http.MethodPatch, "https://dev.azure.test/write", map[string]string{"state": "done"}, "")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled mutation error = %v, want context.Canceled", err)
	}
	if len(fence.requests) != 0 || client.calls != 1 {
		t.Fatalf("cancelled mutation reached fence/effect: %d/%d", len(fence.requests), client.calls)
	}
}
