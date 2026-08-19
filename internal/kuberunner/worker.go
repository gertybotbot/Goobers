package kuberunner

import (
	"context"
	"errors"
	"fmt"

	apiv1 "github.com/goobers/goobers/api/v1alpha1"
	"github.com/goobers/goobers/internal/invoke"
	"github.com/goobers/goobers/providers"
)

var (
	ErrAttemptStale     = errors.New("kuberunner: executable attempt is no longer current")
	ErrAttemptCancelled = errors.New("kuberunner: executable attempt was cancelled")
	ErrUnsafeMutation   = errors.New("kuberunner: mutation-capable execution bypasses the fenced provider seam")
)

// AttemptPlanSource loads immutable source for the Job's one attempt.
type AttemptPlanSource interface {
	LoadAttemptPlan(context.Context) (AttemptPlan, error)
}

// StaticAttemptPlanSource is useful for an environment-decoded Job plan and
// focused embedders.
type StaticAttemptPlanSource struct{ Plan AttemptPlan }

func (s StaticAttemptPlanSource) LoadAttemptPlan(context.Context) (AttemptPlan, error) {
	if err := s.Plan.validate(); err != nil {
		return AttemptPlan{}, err
	}
	return s.Plan, nil
}

// AgentResolver supplies the existing invoke.Goober abstraction.
type AgentResolver interface {
	Agent(context.Context, string) (invoke.Goober, error)
}

// ProviderExecutorResolver supplies the existing invoke.Deterministic
// abstraction for trusted in-process provider stages.
type ProviderExecutorResolver interface {
	ProviderExecutor(context.Context, AttemptPlan) (invoke.Deterministic, error)
}

// FencedProviderExecutor is the explicit capability marker required before a
// mutation-capable provider plan may execute. The executor still implements the
// ordinary invoke.Deterministic contract; the marker states that every external
// provider call preserves the Worker context and therefore reaches
// providers.WithMutationFence before its HTTP side effect.
type FencedProviderExecutor interface {
	invoke.Deterministic
	UsesProviderMutationFence()
}

// AttemptAuthority reasserts that an attempt is the journal's current open
// occurrence and that its retained claim epoch is current.
type AttemptAuthority interface {
	AssertAttempt(context.Context, AttemptID) error
}

// ResultPublisher publishes one validated receipt. FSResultReader satisfies it.
type ResultPublisher interface {
	PublishResult(AttemptID, apiv1.ResultEnvelope) error
}

// Worker executes exactly one source-backed attempt. It never advances workflow
// state: its only durable output is a receipt that the controller independently
// validates and journals.
type Worker struct {
	Source    AttemptPlanSource
	Agents    AgentResolver
	Providers ProviderExecutorResolver
	Authority AttemptAuthority
	Results   ResultReader
	Publisher ResultPublisher
}

// Execute runs the attempt once. A valid already-published receipt makes a
// restart idempotent; no executor or provider is called again.
func (w *Worker) Execute(ctx context.Context) error {
	if w.Source == nil || w.Authority == nil || w.Publisher == nil {
		return fmt.Errorf("kuberunner: worker source, authority, and publisher are required")
	}
	plan, err := w.Source.LoadAttemptPlan(ctx)
	if err != nil {
		return fmt.Errorf("load attempt source: %w", err)
	}
	if w.Results != nil {
		if raw, readErr := w.Results.ReadResult(plan.Attempt); readErr == nil {
			if _, err := ValidateResult(plan.Attempt, raw); err != nil {
				return fmt.Errorf("existing attempt result is invalid: %w", err)
			}
			return nil
		} else if !errors.Is(readErr, ErrResultMissing) {
			return fmt.Errorf("read existing attempt result: %w", readErr)
		}
	}
	if planHasMutationCapabilities(plan) && plan.Attempt.FenceEpoch < 1 {
		return fmt.Errorf("%w: mutation-capable stage %q has no business-claim epoch", ErrClaimFenceLost, plan.Attempt.State)
	}
	if err := w.Authority.AssertAttempt(ctx, plan.Attempt); err != nil {
		return err
	}

	var result apiv1.ResultEnvelope
	switch plan.Kind {
	case ExecutionAgentic:
		// The capability audit requires model reasoning to remain agentic, but
		// mutation execution to move behind a trusted deterministic boundary.
		// Handing a raw push/write token to the harness would make per-mutation
		// fencing impossible, so fail closed rather than silently over-credential.
		if planHasMutationCapabilities(plan) {
			return fmt.Errorf("%w: agentic stage %q declares direct mutation capability", ErrUnsafeMutation, plan.Attempt.State)
		}
		if w.Agents == nil {
			return fmt.Errorf("kuberunner: no agent resolver configured")
		}
		agent, err := w.Agents.Agent(ctx, plan.Agentic.Goober)
		if err != nil {
			return fmt.Errorf("resolve agent %q: %w", plan.Agentic.Goober, err)
		}
		if plan.Agentic.Review {
			verdict, err := agent.Review(ctx, plan.Invocation)
			if err != nil {
				return fmt.Errorf("execute agentic review: %w", err)
			}
			result = resultFromVerdict(verdict)
		} else {
			result, err = agent.Invoke(ctx, plan.Invocation)
			if err != nil {
				return fmt.Errorf("execute agentic task: %w", err)
			}
		}

	case ExecutionProviderTask:
		if w.Providers == nil {
			return fmt.Errorf("kuberunner: no provider executor resolver configured")
		}
		executor, err := w.Providers.ProviderExecutor(ctx, plan)
		if err != nil {
			return fmt.Errorf("resolve provider executor: %w", err)
		}
		if planHasMutationCapabilities(plan) {
			if _, ok := executor.(FencedProviderExecutor); !ok {
				return fmt.Errorf("%w: provider stage %q", ErrUnsafeMutation, plan.Attempt.State)
			}
		}
		fence := attemptProviderFence{authority: w.Authority, attempt: plan.Attempt}
		providerCtx := providers.WithMutationFence(ctx, plan.Attempt.String(), fence)
		result, err = executor.Run(providerCtx, plan.Invocation, plan.Provider.Run)
		if err != nil {
			return fmt.Errorf("execute provider stage: %w", err)
		}

	default:
		return fmt.Errorf("kuberunner: unsupported execution kind %q", plan.Kind)
	}

	// Reject a stale completion at the producer as well as at the controller's
	// acceptance seam. The second check closes takeover/cancellation while the
	// executor was running; the controller performs a third check before append.
	if err := w.Authority.AssertAttempt(ctx, plan.Attempt); err != nil {
		return err
	}
	raw, err := EncodeReceipt(plan.Attempt, result)
	if err != nil {
		return err
	}
	if _, err := ValidateResult(plan.Attempt, raw); err != nil {
		return fmt.Errorf("executor produced invalid result: %w", err)
	}
	if err := w.Publisher.PublishResult(plan.Attempt, result); err != nil {
		return fmt.Errorf("publish attempt result: %w", err)
	}
	return nil
}

type attemptProviderFence struct {
	authority AttemptAuthority
	attempt   AttemptID
}

func (f attemptProviderFence) AuthorizeProviderMutation(ctx context.Context, _ providers.ProviderMutation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return f.authority.AssertAttempt(ctx, f.attempt)
}

// JournalAttemptAuthority is the production fence: journal occurrence first,
// then the retained claim. Neither status nor Job state participates.
type JournalAttemptAuthority struct {
	Journal   JournalStore
	Claims    ClaimStore
	Namespace string
}

func (a JournalAttemptAuthority) AssertAttempt(ctx context.Context, attempt AttemptID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.Journal == nil {
		return ErrAttemptStale
	}
	head, err := a.Journal.Head(attempt.RunID)
	if err != nil {
		return fmt.Errorf("assert attempt journal: %w", err)
	}
	if head.IsTerminal() {
		return ErrAttemptCancelled
	}
	if !head.HasOpenAttempt() || head.OpenStage != attempt.State || head.OpenAttempt != attempt.Attempt || head.Branch != attempt.Branch {
		return ErrAttemptStale
	}
	if attempt.FenceEpoch == 0 {
		if head.Claim != nil {
			return ErrAttemptStale
		}
		return nil
	}
	if head.Claim == nil || head.Claim.RunID != attempt.RunID || head.Claim.RunUID != attempt.RunUID || head.Claim.Epoch != attempt.FenceEpoch {
		return ErrClaimFenceLost
	}
	if a.Claims == nil {
		return ErrClaimFenceLost
	}
	return a.Claims.AssertCurrent(ctx, a.Namespace, *head.Claim)
}

func resultFromVerdict(verdict apiv1.Verdict) apiv1.ResultEnvelope {
	result := apiv1.ResultEnvelope{Summary: verdict.Summary}
	if result.Summary == "" {
		result.Summary = verdict.Rationale
	}
	switch verdict.Decision {
	case apiv1.VerdictPass:
		result.Status = apiv1.ResultSuccess
	case apiv1.VerdictFail, apiv1.VerdictNeedsChanges:
		result.Status = apiv1.ResultFailure
		result.Error = &apiv1.ErrorInfo{Code: "agentic_review_" + string(verdict.Decision), Message: result.Summary}
	default:
		result.Status = apiv1.ResultFailure
		result.Error = &apiv1.ErrorInfo{Code: "agentic_review_invalid", Message: "agentic reviewer returned an invalid decision"}
	}
	return result
}
