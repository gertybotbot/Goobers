package kuberunner

import (
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"

	apiv1 "github.com/goobers/goobers/api/v1alpha1"
	"github.com/goobers/goobers/internal/capability"
	"github.com/goobers/goobers/internal/journal"
)

type sourcedTestMachine struct {
	*testMachine
	task apiv1.Task
	gate *apiv1.Gate
}

func (m *sourcedTestMachine) ExecutableState(state string) (apiv1.Task, *apiv1.Gate, bool) {
	if m.gate != nil && m.gate.Name == state {
		return apiv1.Task{}, m.gate, true
	}
	if m.task.Name == state {
		return m.task, nil, true
	}
	return apiv1.Task{}, nil, false
}

func sourcedRun() *apiv1.GooberRun {
	return &apiv1.GooberRun{Spec: apiv1.GooberRunSpec{
		RunID: "run-1", Gaggle: "web", Workflow: apiv1.PinnedWorkflow{Name: "ship"},
		Trigger:    apiv1.RunTrigger{Kind: "item", Ref: "42"},
		Repository: &apiv1.RepositoryIdentity{Provider: "github", Repository: "acme/web", ExternalID: "42"},
	}}
}

func TestBuildAttemptPlanUsesPinnedExecutableSource(t *testing.T) {
	machine := &sourcedTestMachine{testMachine: newTestMachine(), task: apiv1.Task{
		Name: "build", Type: apiv1.TaskAgentic, Goober: "coder", Goal: "implement",
		Inputs:         map[string]string{"mode": "safe"},
		Capabilities:   []string{string(capability.AgentModel), string(capability.RepoRead)},
		TimeoutSeconds: 90,
	}}
	attempt := AttemptID{RunUID: "uid-1", RunID: "run-1", State: "build", Attempt: 1, FenceEpoch: 7}
	plan, err := BuildAttemptPlan(sourcedRun(), attempt, machine)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}
	if plan.Kind != ExecutionAgentic || plan.Agentic == nil || plan.Agentic.Goober != "coder" {
		t.Fatalf("execution plan = %+v", plan)
	}
	if plan.Invocation.RepoRef.Provider != apiv1.ProviderGitHub || plan.Invocation.RepoRef.Owner != "acme" || plan.Invocation.RepoRef.Name != "web" {
		t.Fatalf("repo source = %+v", plan.Invocation.RepoRef)
	}
	if plan.Invocation.Inputs["mode"] != "safe" || plan.Invocation.Limits.MaxDurationSeconds != 90 {
		t.Fatalf("config source = inputs:%+v limits:%+v", plan.Invocation.Inputs, plan.Invocation.Limits)
	}
	encoded, err := EncodeAttemptPlan(plan)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := DecodeAttemptPlan(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !decoded.Attempt.Equal(attempt) || decoded.Agentic.Goober != "coder" {
		t.Fatalf("decoded plan = %+v", decoded)
	}
}

func TestAgenticGateUsesReferencedGooberCapabilitiesAndRefusesItsWriteGrants(t *testing.T) {
	gate := &apiv1.Gate{Name: "review", Evaluator: apiv1.EvaluatorAgentic, Agentic: &apiv1.AgenticGate{Goober: "reviewer"}}
	machine := &sourcedTestMachine{testMachine: newTestMachine(), gate: gate}
	attempt := AttemptID{RunUID: "uid-1", RunID: "run-1", State: "review", Attempt: 1, FenceEpoch: 5}
	r := &RunReconciler{GooberCapabilities: map[string][]string{"reviewer": {string(capability.AgentModel)}}}
	plan, err := r.attemptPlan(sourcedRun(), attempt, machine)
	if err != nil {
		t.Fatalf("review plan: %v", err)
	}
	if !plan.Agentic.Review || len(plan.Invocation.Capabilities) != 1 || plan.Invocation.Capabilities[0] != string(capability.AgentModel) {
		t.Fatalf("review capabilities = %+v", plan)
	}

	r.GooberCapabilities["reviewer"] = []string{string(capability.GitHubIssuesWrite)}
	if _, err := r.attemptPlan(sourcedRun(), attempt, machine); !errors.Is(err, ErrUnsafeMutation) {
		t.Fatalf("write-capable reviewer error = %v, want ErrUnsafeMutation", err)
	}
}

func TestDesiredJobWiresOnlyDeclaredCredentialSecretsAndFenceIdentity(t *testing.T) {
	plan := providerPlan()
	run := sourcedRun()
	r := &RunReconciler{CredentialBindings: map[string]CredentialSecretBinding{
		string(capability.GitHubIssuesWrite): {Env: "GOOBERS_GITHUB_TOKEN", SecretName: "issue-writer", SecretKey: "token"},
		string(capability.GitHubPRMerge):     {Env: "GOOBERS_MERGE_TOKEN", SecretName: "merger", SecretKey: "token"},
	}}
	job, err := r.desiredJob(run, plan.Attempt, &plan)
	if err != nil {
		t.Fatalf("desired job: %v", err)
	}
	container := job.Spec.Template.Spec.Containers[0]
	if len(container.Command) != 1 || container.Command[0] != AttemptWorkerEntry {
		t.Fatalf("worker command = %v", container.Command)
	}
	env := map[string]corev1.EnvVar{}
	for _, variable := range container.Env {
		env[variable.Name] = variable
	}
	if env[AttemptPlanEnv].Value == "" || env["GOOBERS_FENCE_EPOCH"].Value != "9" || env["GOOBERS_CLAIM_EXTERNAL_ID"].Value != "42" {
		t.Fatalf("source/fence env = %+v", env)
	}
	credential := env["GOOBERS_GITHUB_TOKEN"].ValueFrom
	if credential == nil || credential.SecretKeyRef == nil || credential.SecretKeyRef.Name != "issue-writer" || credential.SecretKeyRef.Key != "token" {
		t.Fatalf("declared credential binding = %+v", credential)
	}
	if _, leaked := env["GOOBERS_MERGE_TOKEN"]; leaked {
		t.Fatal("undeclared merge credential leaked into provider Job")
	}
}

func TestControllerRefusesMutationCapableAgentBeforeDispatchIntent(t *testing.T) {
	h := newHarness(t)
	machine := &sourcedTestMachine{testMachine: h.machine, task: apiv1.Task{
		Name: "build", Type: apiv1.TaskAgentic, Goober: "coder", Goal: "push",
		Capabilities: []string{string(capability.RepoPush)},
	}}
	h.reconciler.Machine = StaticMachineResolver{Machine: machine}
	before := len(h.journal.snapshot(testRunID))
	h.reconcile()
	after := h.journal.snapshot(testRunID)
	if len(after) != before {
		t.Fatalf("unsafe source journalled dispatch intent: before=%d after=%d types=%v", before, len(after), h.journal.types(testRunID))
	}
	if len(h.jobs()) != 0 {
		t.Fatal("unsafe agent source created a Job")
	}
	if !hasCondition(h.run().Status.Conditions, apiv1.GooberRunConditionReady, "False", "AttemptSourceInvalid") {
		t.Fatalf("missing AttemptSourceInvalid condition: %+v", h.run().Status.Conditions)
	}
}

func TestJournalAttemptAuthorityRejectsClosedAndMisaddressedAttempts(t *testing.T) {
	j := newFakeJournal()
	j.seed(testRunID)
	attempt := AttemptID{RunUID: testRunUID, RunID: testRunID, State: "build", Attempt: 1}
	if _, err := j.AppendStageStarted(testRunID, attempt); err != nil {
		t.Fatal(err)
	}
	authority := JournalAttemptAuthority{Journal: j}
	if err := authority.AssertAttempt(t.Context(), attempt); err != nil {
		t.Fatalf("current attempt rejected: %v", err)
	}
	other := attempt
	other.Attempt = 2
	if err := authority.AssertAttempt(t.Context(), other); !errors.Is(err, ErrAttemptStale) {
		t.Fatalf("misaddressed error = %v, want ErrAttemptStale", err)
	}
	if _, err := j.AppendRunFinished(testRunID, journal.PhaseAborted, "@abort"); err != nil {
		t.Fatal(err)
	}
	if err := authority.AssertAttempt(t.Context(), attempt); !errors.Is(err, ErrAttemptCancelled) {
		t.Fatalf("cancelled error = %v, want ErrAttemptCancelled", err)
	}
}
