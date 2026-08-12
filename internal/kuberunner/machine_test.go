package kuberunner

import (
	"testing"

	apiv1 "github.com/goobers/goobers/api/v1alpha1"
	"github.com/goobers/goobers/internal/journal"
	"github.com/goobers/goobers/internal/workflow"
)

func compileFixture(t *testing.T, spec apiv1.WorkflowSpec) *workflow.Machine {
	t.Helper()
	machine, err := workflow.Compile(workflow.Definition{
		Name:    "ship",
		Version: 1,
		Spec:    spec,
	})
	if err != nil {
		t.Fatalf("compile workflow: %v", err)
	}
	return machine
}

func shipSpec() apiv1.WorkflowSpec {
	return apiv1.WorkflowSpec{
		Gaggle: "web",
		Start:  "build",
		Tasks: []apiv1.Task{
			{Name: "build", Goober: "dev", Next: "approve"},
			{Name: "deploy", Goober: "dev"},
		},
		Gates: []apiv1.Gate{{
			Name:      "approve",
			Evaluator: apiv1.EvaluatorHuman,
			Human:     &apiv1.HumanGate{},
			Branches:  map[string]string{"pass": "deploy", "fail": "@abort"},
		}},
	}
}

// A run pins a definition digest and must complete on that exact definition
// (WF-016). An adapter that answered from a re-read live definition would
// silently defeat the pin, so the mismatch is refused at construction.
func TestNewCompiledMachineRefusesADigestMismatch(t *testing.T) {
	machine := compileFixture(t, shipSpec())

	if _, err := NewCompiledMachine(machine, machine.Digest()); err != nil {
		t.Fatalf("rejected the machine matching its own digest: %v", err)
	}

	other := "sha256:" + "0000000000000000000000000000000000000000000000000000000000000000"
	if _, err := NewCompiledMachine(machine, other); err == nil {
		t.Fatal("accepted a machine whose digest does not match the run's pin")
	}

	if _, err := NewCompiledMachine(nil, ""); err == nil {
		t.Fatal("accepted a nil machine")
	}
}

func TestCompiledMachineClassifiesStates(t *testing.T) {
	machine := compileFixture(t, shipSpec())
	adapter, err := NewCompiledMachine(machine, machine.Digest())
	if err != nil {
		t.Fatalf("adapt: %v", err)
	}

	if got := adapter.Start(); got != "build" {
		t.Errorf("start = %q, want %q", got, "build")
	}

	for state, want := range map[string]StateKind{
		"build":     StateTask,
		"deploy":    StateTask,
		"approve":   StateHumanGate, // executes no process: never gets a Job
		"@complete": StateTerminal,
		"":          StateTerminal, // the DSL spelling of the same terminal
		"@abort":    StateTerminal,
		"@escalate": StateTerminal,
		// "@join" ends a BRANCH, not a run. This slice has no parallel support,
		// so it must not be mistaken for a terminal that finishes the run.
		"@join":    StateUnknown,
		"nonsense": StateUnknown,
	} {
		if got := adapter.Kind(state); got != want {
			t.Errorf("Kind(%q) = %q, want %q", state, got, want)
		}
	}
}

func TestCompiledMachineTransitions(t *testing.T) {
	machine := compileFixture(t, shipSpec())
	adapter, err := NewCompiledMachine(machine, machine.Digest())
	if err != nil {
		t.Fatalf("adapt: %v", err)
	}

	if next, ok := adapter.Next("build", apiv1.ResultSuccess); !ok || next != "approve" {
		t.Errorf("Next(build, success) = %q/%v, want approve/true", next, ok)
	}
	// A task with no declared Next completes the run rather than dangling.
	if next, ok := adapter.Next("deploy", apiv1.ResultSuccess); ok || next != journal.TargetComplete {
		t.Errorf("Next(deploy, success) = %q/%v, want @complete/false", next, ok)
	}
	// A no-work stage short-circuits to a clean completion rather than
	// invoking a downstream stage with no subject.
	if next, ok := adapter.Next("build", apiv1.ResultNoWork); ok || next != journal.TargetComplete {
		t.Errorf("Next(build, no-work) = %q/%v, want @complete/false", next, ok)
	}
	if next, _ := adapter.Next("build", apiv1.ResultFailure); next != "@abort" {
		t.Errorf("Next(build, failure) = %q, want @abort", next)
	}
	// A gate routes on its declared branches.
	if next, ok := adapter.Next("approve", apiv1.ResultSuccess); !ok || next != "deploy" {
		t.Errorf("Next(approve, success) = %q/%v, want deploy/true", next, ok)
	}
	if next, _ := adapter.Next("approve", apiv1.ResultFailure); next != "@abort" {
		t.Errorf("Next(approve, failure) = %q, want @abort", next)
	}
}

func TestCompiledMachineTerminalPhases(t *testing.T) {
	machine := compileFixture(t, shipSpec())
	adapter, err := NewCompiledMachine(machine, machine.Digest())
	if err != nil {
		t.Fatalf("adapt: %v", err)
	}

	for target, want := range map[string]journal.RunPhase{
		"@complete": journal.PhaseCompleted,
		"":          journal.PhaseCompleted,
		"@abort":    journal.PhaseAborted,
		"@escalate": journal.PhaseEscalated,
	} {
		if got := adapter.TerminalPhase(target); got != want {
			t.Errorf("TerminalPhase(%q) = %q, want %q", target, got, want)
		}
	}
}

// Retry budget comes from the pinned definition, and defaults to a single
// attempt so an undeclared policy never silently grants retries.
func TestCompiledMachineMaxAttempts(t *testing.T) {
	spec := shipSpec()
	spec.Tasks[0].Retry = &apiv1.RetryPolicy{MaxAttempts: 3}
	machine := compileFixture(t, spec)
	adapter, err := NewCompiledMachine(machine, machine.Digest())
	if err != nil {
		t.Fatalf("adapt: %v", err)
	}

	if got := adapter.MaxAttempts("build"); got != 3 {
		t.Errorf("MaxAttempts(build) = %d, want 3", got)
	}
	if got := adapter.MaxAttempts("deploy"); got != 1 {
		t.Errorf("MaxAttempts(deploy) = %d, want the default 1", got)
	}
}

func TestStaticMachineResolverRequiresAMachine(t *testing.T) {
	if _, err := (StaticMachineResolver{}).Resolve(t.Context(), nil); err == nil {
		t.Fatal("resolved a machine when none was configured")
	}
}
