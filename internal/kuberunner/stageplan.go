package kuberunner

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	apiv1 "github.com/goobers/goobers/api/v1alpha1"
	"github.com/goobers/goobers/internal/capability"
	"github.com/goobers/goobers/internal/workflow"
)

// AttemptPlanVersion is the closed controller-to-worker source envelope. It is
// immutable attempt input, not workflow authority; the worker and controller
// still re-read the journal and retained claim before any mutation or result.
const AttemptPlanVersion = "goobers.dev/kuberunner/attempt-plan/v1alpha1"

const (
	AttemptPlanEnv     = "GOOBERS_ATTEMPT_PLAN"
	AttemptWorkspace   = "/goobers/workspace"
	AttemptWorkerEntry = "/usr/local/bin/goober-attempt"
)

// ExecutionKind selects the existing invoke abstraction a worker uses.
type ExecutionKind string

const (
	ExecutionAgentic      ExecutionKind = "agentic"
	ExecutionProviderTask ExecutionKind = "provider"
)

// AgenticExecution invokes invoke.Goober. Review distinguishes an agentic gate
// from an ordinary task.
type AgenticExecution struct {
	Goober string `json:"goober"`
	Review bool   `json:"review,omitempty"`
}

// ProviderExecution invokes invoke.Deterministic. Mutation-capable provider
// stages are admitted only when that executor advertises the fenced provider
// transport marker checked by Worker.
type ProviderExecution struct {
	Run apiv1.DeterministicRun `json:"run"`
}

// CredentialSecretBinding maps one capability to one scoped Kubernetes Secret
// key. Values never enter AttemptPlan or annotations.
type CredentialSecretBinding struct {
	Env        string
	SecretName string
	SecretKey  string
}

func (b CredentialSecretBinding) validate(capability string) error {
	if b.Env == "" || b.SecretName == "" || b.SecretKey == "" {
		return fmt.Errorf("kuberunner: incomplete credential binding for capability %q", capability)
	}
	return nil
}

// AttemptPlan is the pinned, source-backed execution input carried by exactly
// one Job. It contains no credential values. Secret selectors are wired into
// the Pod independently and only for capabilities declared here.
type AttemptPlan struct {
	Schema     string                   `json:"schema"`
	Attempt    AttemptID                `json:"attempt"`
	Kind       ExecutionKind            `json:"kind"`
	Invocation apiv1.InvocationEnvelope `json:"invocation"`
	Agentic    *AgenticExecution        `json:"agentic,omitempty"`
	Provider   *ProviderExecution       `json:"provider,omitempty"`
}

func (p AttemptPlan) validate() error {
	if p.Schema != AttemptPlanVersion {
		return fmt.Errorf("kuberunner: unknown attempt plan schema %q", p.Schema)
	}
	if p.Attempt.RunUID == "" || p.Attempt.RunID == "" || p.Attempt.State == "" || p.Attempt.Attempt < 1 {
		return fmt.Errorf("kuberunner: incomplete attempt identity")
	}
	if p.Invocation.RunID != p.Attempt.RunID || p.Invocation.TaskID != p.Attempt.State {
		return fmt.Errorf("kuberunner: attempt plan invocation is misaddressed")
	}
	for _, grant := range p.Invocation.Capabilities {
		if !capability.StageDeclarable(grant) {
			return fmt.Errorf("kuberunner: attempt plan contains unknown capability %q", grant)
		}
	}
	switch p.Kind {
	case ExecutionAgentic:
		if p.Agentic == nil || p.Provider != nil || p.Agentic.Goober == "" {
			return fmt.Errorf("kuberunner: invalid agentic execution plan")
		}
	case ExecutionProviderTask:
		if p.Provider == nil || p.Agentic != nil {
			return fmt.Errorf("kuberunner: invalid provider execution plan")
		}
	default:
		return fmt.Errorf("kuberunner: unknown execution kind %q", p.Kind)
	}
	return nil
}

// EncodeAttemptPlan produces the closed environment value mounted in a Job.
func EncodeAttemptPlan(plan AttemptPlan) (string, error) {
	if err := plan.validate(); err != nil {
		return "", err
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		return "", fmt.Errorf("kuberunner: encode attempt plan: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

// DecodeAttemptPlan rejects unknown fields before a worker trusts source bytes.
func DecodeAttemptPlan(encoded string) (AttemptPlan, error) {
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return AttemptPlan{}, fmt.Errorf("kuberunner: decode attempt plan: %w", err)
	}
	var plan AttemptPlan
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&plan); err != nil {
		return AttemptPlan{}, fmt.Errorf("kuberunner: decode attempt plan: %w", err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return AttemptPlan{}, fmt.Errorf("kuberunner: decode attempt plan: trailing JSON value")
	}
	if err := plan.validate(); err != nil {
		return AttemptPlan{}, err
	}
	return plan, nil
}

// ExecutableStateSource exposes the executable definition from the same pinned
// machine that decides transitions. It does not become a second machine: the
// controller still owns every transition and this view can only describe the
// already-journalled attempt.
type ExecutableStateSource interface {
	ExecutableState(string) (apiv1.Task, *apiv1.Gate, bool)
}

// BuildAttemptPlan projects one pinned workflow state into worker source.
func BuildAttemptPlan(run *apiv1.GooberRun, attempt AttemptID, machine StateMachine) (AttemptPlan, error) {
	source, ok := machine.(ExecutableStateSource)
	if !ok {
		return AttemptPlan{}, fmt.Errorf("kuberunner: pinned machine does not expose executable source")
	}
	task, gate, ok := source.ExecutableState(attempt.State)
	if !ok {
		return AttemptPlan{}, fmt.Errorf("kuberunner: state %q has no executable source", attempt.State)
	}
	env := apiv1.InvocationEnvelope{
		TaskID:     attempt.State,
		WorkflowID: run.Spec.Workflow.Name,
		RunID:      run.Spec.RunID,
		TriggerRef: run.Spec.Trigger.Ref,
		Gaggle:     run.Spec.Gaggle,
		Workspace:  AttemptWorkspace,
		RepoRef:    repoRefFromRun(run),
	}
	plan := AttemptPlan{Schema: AttemptPlanVersion, Attempt: attempt, Invocation: env}
	if gate != nil {
		if gate.Evaluator != apiv1.EvaluatorAgentic || gate.Agentic == nil {
			return AttemptPlan{}, fmt.Errorf("kuberunner: gate %q is not agentic", gate.Name)
		}
		plan.Kind = ExecutionAgentic
		plan.Agentic = &AgenticExecution{Goober: gate.Agentic.Goober, Review: true}
		plan.Invocation.Goal = "review stage " + gate.Name
		plan.Invocation.Limits.MaxDurationSeconds = gate.Agentic.TimeoutSeconds
		return plan, plan.validate()
	}

	plan.Invocation.Goal = task.Goal
	plan.Invocation.Capabilities = append([]string(nil), task.Capabilities...)
	plan.Invocation.Inputs = make(map[string]interface{}, len(task.Inputs))
	for key, value := range task.Inputs {
		plan.Invocation.Inputs[key] = value
	}
	if compiled := compiledWorkflow(machine); compiled != nil {
		limits, err := workflow.TaskLimits(compiled, task)
		if err != nil {
			return AttemptPlan{}, fmt.Errorf("kuberunner: project stage limits: %w", err)
		}
		plan.Invocation.Limits = limits
	} else {
		if task.Limits != nil {
			plan.Invocation.Limits = *task.Limits
		}
		if task.TimeoutSeconds > 0 {
			plan.Invocation.Limits.MaxDurationSeconds = task.TimeoutSeconds
		}
	}
	switch task.Type {
	case apiv1.TaskAgentic:
		plan.Kind = ExecutionAgentic
		plan.Agentic = &AgenticExecution{Goober: task.Goober}
	case apiv1.TaskDeterministic:
		if task.Run == nil {
			return AttemptPlan{}, fmt.Errorf("kuberunner: deterministic state %q has no run definition", task.Name)
		}
		plan.Kind = ExecutionProviderTask
		plan.Provider = &ProviderExecution{Run: *task.Run}
	default:
		return AttemptPlan{}, fmt.Errorf("kuberunner: state %q has unknown task type %q", task.Name, task.Type)
	}
	return plan, plan.validate()
}

// compiledWorkflow is deliberately narrow: BuildAttemptPlan is used with the
// production CompiledMachine. Returning nil for another source keeps limits at
// their direct task values without making transition authority depend on it.
func compiledWorkflow(machine StateMachine) *workflow.Machine {
	if compiled, ok := machine.(*CompiledMachine); ok {
		return compiled.machine
	}
	return nil
}

func repoRefFromRun(run *apiv1.GooberRun) apiv1.RepoRef {
	if run.Spec.Repository == nil {
		return apiv1.RepoRef{}
	}
	repo := run.Spec.Repository.Repository
	owner, name, found := strings.Cut(repo, "/")
	if !found {
		name, owner = repo, ""
	}
	return apiv1.RepoRef{Provider: apiv1.Provider(run.Spec.Repository.Provider), Owner: owner, Name: name}
}

func (r *RunReconciler) attemptPlan(run *apiv1.GooberRun, attempt AttemptID, machine StateMachine) (*AttemptPlan, error) {
	if _, ok := machine.(ExecutableStateSource); !ok {
		// Phase 1/2 embedders that expose only structural state retain their
		// skeleton Job. Production CompiledMachine always supplies source.
		return nil, nil
	}
	plan, err := BuildAttemptPlan(run, attempt, machine)
	if err != nil {
		return nil, err
	}
	if plan.Agentic != nil && plan.Agentic.Review {
		plan.Invocation.Capabilities = append([]string(nil), r.GooberCapabilities[plan.Agentic.Goober]...)
		if err := plan.validate(); err != nil {
			return nil, err
		}
	}
	if plan.Kind == ExecutionAgentic && planHasMutationCapabilities(plan) {
		return nil, fmt.Errorf("%w: agentic stage %q declares direct mutation capability", ErrUnsafeMutation, attempt.State)
	}
	if planHasMutationCapabilities(plan) && attempt.FenceEpoch < 1 {
		return nil, fmt.Errorf("%w: mutation-capable stage %q has no business-claim epoch", ErrClaimFenceLost, attempt.State)
	}
	return &plan, nil
}

func planHasMutationCapabilities(plan AttemptPlan) bool {
	for _, grant := range plan.Invocation.Capabilities {
		if capability.MutatesExternalState(grant) {
			return true
		}
	}
	return false
}
