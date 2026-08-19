package kuberunner

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/goobers/goobers/api/v1alpha1"
	"github.com/goobers/goobers/internal/capability"
	"github.com/goobers/goobers/internal/invoke"
	"github.com/goobers/goobers/providers"
)

type workerAuthority struct {
	calls  int
	failAt int
	err    error
}

func (a *workerAuthority) AssertAttempt(context.Context, AttemptID) error {
	a.calls++
	if a.failAt > 0 && a.calls >= a.failAt {
		return a.err
	}
	return nil
}

type workerGoober struct {
	calls int
	got   apiv1.InvocationEnvelope
}

func (g *workerGoober) Invoke(_ context.Context, env apiv1.InvocationEnvelope) (apiv1.ResultEnvelope, error) {
	g.calls++
	g.got = env
	return apiv1.ResultEnvelope{Status: apiv1.ResultSuccess, Summary: "implemented"}, nil
}

func (g *workerGoober) Review(context.Context, apiv1.InvocationEnvelope) (apiv1.Verdict, error) {
	return apiv1.Verdict{Decision: apiv1.VerdictPass}, nil
}

type staticAgentResolver struct{ agent invoke.Goober }

func (r staticAgentResolver) Agent(context.Context, string) (invoke.Goober, error) {
	return r.agent, nil
}

type workerResults struct{ raw []byte }

func (r *workerResults) ReadResult(AttemptID) ([]byte, error) {
	if len(r.raw) == 0 {
		return nil, ErrResultMissing
	}
	return append([]byte(nil), r.raw...), nil
}

func (r *workerResults) PublishResult(attempt AttemptID, result apiv1.ResultEnvelope) error {
	raw, err := EncodeReceipt(attempt, result)
	if err != nil {
		return err
	}
	r.raw = raw
	return nil
}

func agentPlan(capabilities ...string) AttemptPlan {
	attempt := AttemptID{RunUID: "uid-1", RunID: "run-1", State: "implement", Attempt: 1, FenceEpoch: 4}
	return AttemptPlan{
		Schema:  AttemptPlanVersion,
		Attempt: attempt,
		Kind:    ExecutionAgentic,
		Invocation: apiv1.InvocationEnvelope{
			TaskID: "implement", WorkflowID: "ship", RunID: "run-1", Gaggle: "web",
			Goal: "implement", Workspace: AttemptWorkspace, Capabilities: capabilities,
		},
		Agentic: &AgenticExecution{Goober: "coder"},
	}
}

func TestWorkerExecutesAgentThroughExistingInvokeSeamAndPublishesDeterministically(t *testing.T) {
	plan := agentPlan(string(capability.AgentModel), string(capability.RepoRead))
	authority := &workerAuthority{}
	agent := &workerGoober{}
	results := &workerResults{}
	worker := Worker{
		Source: StaticAttemptPlanSource{Plan: plan}, Agents: staticAgentResolver{agent: agent},
		Authority: authority, Results: results, Publisher: results,
	}

	if err := worker.Execute(t.Context()); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if agent.calls != 1 || agent.got.TaskID != "implement" || agent.got.RunID != "run-1" {
		t.Fatalf("agent calls/source = %d/%+v", agent.calls, agent.got)
	}
	if authority.calls != 2 {
		t.Fatalf("authority assertions = %d, want pre-execution and pre-publish", authority.calls)
	}
	validated, err := ValidateResult(plan.Attempt, results.raw)
	if err != nil || validated.Envelope.Status != apiv1.ResultSuccess {
		t.Fatalf("published result = %+v, err=%v", validated, err)
	}

	// A restarted worker sees the immutable receipt and performs no second
	// agent invocation or authority-dependent side effect.
	if err := worker.Execute(t.Context()); err != nil {
		t.Fatalf("restart execute: %v", err)
	}
	if agent.calls != 1 || authority.calls != 2 {
		t.Fatalf("restart repeated work: agent=%d authority=%d", agent.calls, authority.calls)
	}
}

func TestWorkerRejectsDirectAgentMutationCapabilityBeforeInvocation(t *testing.T) {
	plan := agentPlan(string(capability.RepoPush))
	agent := &workerGoober{}
	results := &workerResults{}
	worker := Worker{
		Source: StaticAttemptPlanSource{Plan: plan}, Agents: staticAgentResolver{agent: agent},
		Authority: &workerAuthority{}, Results: results, Publisher: results,
	}
	if err := worker.Execute(t.Context()); !errors.Is(err, ErrUnsafeMutation) {
		t.Fatalf("execute error = %v, want ErrUnsafeMutation", err)
	}
	if agent.calls != 0 || len(results.raw) != 0 {
		t.Fatalf("unsafe agent invoked/published: calls=%d result=%d", agent.calls, len(results.raw))
	}
}

func TestWorkerRejectsStaleCompletion(t *testing.T) {
	plan := agentPlan(string(capability.AgentModel))
	authority := &workerAuthority{failAt: 2, err: ErrAttemptStale}
	agent := &workerGoober{}
	results := &workerResults{}
	worker := Worker{
		Source: StaticAttemptPlanSource{Plan: plan}, Agents: staticAgentResolver{agent: agent},
		Authority: authority, Results: results, Publisher: results,
	}
	if err := worker.Execute(t.Context()); !errors.Is(err, ErrAttemptStale) {
		t.Fatalf("execute error = %v, want ErrAttemptStale", err)
	}
	if agent.calls != 1 || len(results.raw) != 0 {
		t.Fatalf("stale completion committed: agent=%d result=%d", agent.calls, len(results.raw))
	}
}

type providerHTTPClient struct{ calls int }

func (c *providerHTTPClient) Do(*http.Request) (*http.Response, error) {
	c.calls++
	body := `{"number":7,"title":"created","state":"open","html_url":"https://github.test/acme/web/issues/7"}`
	return &http.Response{StatusCode: http.StatusCreated, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
}

type fencedProviderExecutor struct {
	provider *providers.GitHubProvider
}

func (f fencedProviderExecutor) UsesProviderMutationFence() {}

func (f fencedProviderExecutor) Run(ctx context.Context, _ apiv1.InvocationEnvelope, _ apiv1.DeterministicRun) (apiv1.ResultEnvelope, error) {
	_, err := f.provider.CreateWorkItem(ctx, providers.CreateWorkItemRequest{
		Repository: providers.RepositoryRef{Owner: "acme", Name: "web"}, Title: "created",
	})
	if err != nil {
		return apiv1.ResultEnvelope{}, err
	}
	return apiv1.ResultEnvelope{Status: apiv1.ResultSuccess}, nil
}

type staticProviderResolver struct{ executor invoke.Deterministic }

func (r staticProviderResolver) ProviderExecutor(context.Context, AttemptPlan) (invoke.Deterministic, error) {
	return r.executor, nil
}

func providerPlan() AttemptPlan {
	attempt := AttemptID{RunUID: "uid-1", RunID: "run-1", State: "create-issue", Attempt: 1, FenceEpoch: 9}
	return AttemptPlan{
		Schema: AttemptPlanVersion, Attempt: attempt, Kind: ExecutionProviderTask,
		Invocation: apiv1.InvocationEnvelope{
			TaskID: "create-issue", WorkflowID: "curate", RunID: "run-1", Gaggle: "web",
			Goal: "create issue", Workspace: AttemptWorkspace,
			Capabilities: []string{string(capability.GitHubIssuesWrite)},
		},
		Provider: &ProviderExecution{Run: apiv1.DeterministicRun{Command: []string{"trusted-provider-stage"}}},
	}
}

func TestWorkerExecutesProviderStageThroughFencedInvokeSeam(t *testing.T) {
	plan := providerPlan()
	httpClient := &providerHTTPClient{}
	executor := fencedProviderExecutor{provider: providers.NewGitHubProvider("secret", providers.WithHTTPClient(httpClient))}
	authority := &workerAuthority{}
	results := &workerResults{}
	worker := Worker{
		Source: StaticAttemptPlanSource{Plan: plan}, Providers: staticProviderResolver{executor: executor},
		Authority: authority, Results: results, Publisher: results,
	}
	if err := worker.Execute(t.Context()); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if httpClient.calls != 1 {
		t.Fatalf("provider side effects = %d, want 1", httpClient.calls)
	}
	if authority.calls != 3 {
		t.Fatalf("authority assertions = %d, want pre-execution, pre-mutation, and pre-publish", authority.calls)
	}
	if _, err := ValidateResult(plan.Attempt, results.raw); err != nil {
		t.Fatalf("published provider result: %v", err)
	}
}

func TestWorkerFencesProviderMutationImmediatelyBeforeSideEffect(t *testing.T) {
	plan := providerPlan()
	httpClient := &providerHTTPClient{}
	executor := fencedProviderExecutor{provider: providers.NewGitHubProvider("secret", providers.WithHTTPClient(httpClient))}
	authority := &workerAuthority{failAt: 2, err: ErrClaimFenceLost}
	results := &workerResults{}
	worker := Worker{
		Source: StaticAttemptPlanSource{Plan: plan}, Providers: staticProviderResolver{executor: executor},
		Authority: authority, Results: results, Publisher: results,
	}

	if err := worker.Execute(t.Context()); !errors.Is(err, ErrClaimFenceLost) {
		t.Fatalf("execute error = %v, want ErrClaimFenceLost", err)
	}
	if httpClient.calls != 0 {
		t.Fatalf("stale worker made %d provider mutations, want 0", httpClient.calls)
	}
	if len(results.raw) != 0 {
		t.Fatal("stale provider worker published a completion")
	}
}

func TestWorkerCancellationStopsProviderMutationBeforeSideEffect(t *testing.T) {
	plan := providerPlan()
	httpClient := &providerHTTPClient{}
	executor := fencedProviderExecutor{provider: providers.NewGitHubProvider("secret", providers.WithHTTPClient(httpClient))}
	worker := Worker{
		Source: StaticAttemptPlanSource{Plan: plan}, Providers: staticProviderResolver{executor: executor},
		Authority: &workerAuthority{}, Publisher: &workerResults{},
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := worker.Execute(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("execute error = %v, want context.Canceled", err)
	}
	if httpClient.calls != 0 {
		t.Fatalf("cancelled worker made %d provider mutations", httpClient.calls)
	}
}

func TestWorkerRequiresBusinessClaimForEveryProviderMutation(t *testing.T) {
	plan := providerPlan()
	plan.Attempt.FenceEpoch = 0
	worker := Worker{
		Source: StaticAttemptPlanSource{Plan: plan},
		Providers: staticProviderResolver{executor: deterministicFunc(func(context.Context, apiv1.InvocationEnvelope, apiv1.DeterministicRun) (apiv1.ResultEnvelope, error) {
			t.Fatal("claimless mutation executor was invoked")
			return apiv1.ResultEnvelope{}, nil
		})},
		Authority: &workerAuthority{}, Publisher: &workerResults{},
	}
	if err := worker.Execute(t.Context()); !errors.Is(err, ErrClaimFenceLost) {
		t.Fatalf("execute error = %v, want ErrClaimFenceLost", err)
	}
}

func TestWorkerRequiresFencedProviderExecutorForMutationCapabilities(t *testing.T) {
	plan := providerPlan()
	agent := &workerGoober{} // implements invoke.Goober, not invoke.Deterministic
	_ = agent
	unfenced := deterministicFunc(func(context.Context, apiv1.InvocationEnvelope, apiv1.DeterministicRun) (apiv1.ResultEnvelope, error) {
		t.Fatal("unfenced executor was invoked")
		return apiv1.ResultEnvelope{}, nil
	})
	worker := Worker{
		Source: StaticAttemptPlanSource{Plan: plan}, Providers: staticProviderResolver{executor: unfenced},
		Authority: &workerAuthority{}, Publisher: &workerResults{},
	}
	if err := worker.Execute(t.Context()); !errors.Is(err, ErrUnsafeMutation) {
		t.Fatalf("execute error = %v, want ErrUnsafeMutation", err)
	}
}

type deterministicFunc func(context.Context, apiv1.InvocationEnvelope, apiv1.DeterministicRun) (apiv1.ResultEnvelope, error)

func (f deterministicFunc) Run(ctx context.Context, env apiv1.InvocationEnvelope, run apiv1.DeterministicRun) (apiv1.ResultEnvelope, error) {
	return f(ctx, env, run)
}
