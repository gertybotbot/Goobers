package kuberunner

import (
	"context"
	"fmt"

	apiv1 "github.com/goobers/goobers/api/v1alpha1"
	"github.com/goobers/goobers/internal/journal"
	"github.com/goobers/goobers/internal/workflow"
)

// The machine adapter.
//
// The reconciler asks the pinned machine only structural questions. This
// adapter answers them from a compiled workflow.Machine, and it verifies the
// definition's digest against the run's pinned digest before answering
// anything: a run that started on one definition must complete on that exact
// definition (WF-016), so an adapter that answered from a re-read live
// definition would silently defeat the pin.

// CompiledMachine adapts a compiled workflow.Machine to StateMachine.
type CompiledMachine struct {
	machine *workflow.Machine
	start   string
}

// NewCompiledMachine wraps a compiled machine, verifying it matches the digest
// a run pinned. A mismatch is refused rather than tolerated: continuing on a
// different definition than the one the run committed to is a correctness
// failure, not a warning.
func NewCompiledMachine(m *workflow.Machine, pinnedDigest string) (*CompiledMachine, error) {
	if m == nil {
		return nil, fmt.Errorf("kuberunner: nil machine")
	}
	if pinnedDigest != "" && m.Digest() != pinnedDigest {
		return nil, fmt.Errorf("kuberunner: pinned workflow digest %s does not match compiled machine %s",
			pinnedDigest, m.Digest())
	}
	return &CompiledMachine{machine: m, start: m.Def.Spec.Start}, nil
}

// Start returns the machine's entry state.
func (c *CompiledMachine) Start() string { return c.start }

// Kind classifies a state.
func (c *CompiledMachine) Kind(state string) StateKind {
	// The successful terminal is spelled "" in the DSL and "@complete" in the
	// journal (journal.TargetComplete is the explicit representation of the
	// engine's empty-string terminal). Both must classify as terminal here, and
	// neither is in the reserved-target set, so they are matched explicitly.
	if state == "" || state == journal.TargetComplete {
		return StateTerminal
	}
	// "@join" is reserved but is NOT terminal: it ends a parallel BRANCH and the
	// run continues at the join state. This slice implements no parallel
	// execution, so it is reported unknown rather than terminal — classifying it
	// terminal would silently finish a run that should have fanned back in.
	if state == workflow.TargetJoin {
		return StateUnknown
	}
	if workflow.IsReservedAnyTarget(state) {
		return StateTerminal
	}
	if _, ok := c.machine.Task(state); ok {
		return StateTask
	}
	if gate, ok := c.machine.Gate(state); ok {
		switch gate.Evaluator {
		case apiv1.EvaluatorHuman:
			return StateHumanGate
		case apiv1.EvaluatorAutomated:
			return StateAutomatedGate
		default:
			// An agentic gate is a reviewer invocation: it runs a process, so
			// it is dispatched like a task.
			return StateTask
		}
	}
	return StateUnknown
}

// Next returns the state to advance to after a stage settles.
func (c *CompiledMachine) Next(state string, status apiv1.ResultStatus) (string, bool) {
	if task, ok := c.machine.Task(state); ok {
		switch status {
		case apiv1.ResultSuccess:
			if task.Next == "" {
				return journal.TargetComplete, false
			}
			return task.Next, true
		case apiv1.ResultNoWork:
			// A no-work stage short-circuits to a clean completion rather than
			// invoking a downstream stage with no subject.
			return journal.TargetComplete, false
		default:
			return "@abort", false
		}
	}
	if gate, ok := c.machine.Gate(state); ok {
		outcome := "pass"
		if status != apiv1.ResultSuccess {
			outcome = "fail"
		}
		if target, found := gate.Branches[outcome]; found && target != "" {
			return target, c.Kind(target) != StateTerminal
		}
		return "@abort", false
	}
	return journal.TargetComplete, false
}

// TerminalPhase maps a terminal target onto its journal phase.
func (c *CompiledMachine) TerminalPhase(target string) journal.RunPhase {
	switch target {
	case "@abort":
		return journal.PhaseAborted
	case "@escalate":
		return journal.PhaseEscalated
	case journal.TargetComplete, "":
		return journal.PhaseCompleted
	default:
		return journal.PhaseCompleted
	}
}

// MaxAttempts returns the attempt budget for a state.
func (c *CompiledMachine) MaxAttempts(state string) int {
	if task, ok := c.machine.Task(state); ok && task.Retry != nil && task.Retry.MaxAttempts > 0 {
		return int(task.Retry.MaxAttempts)
	}
	return 1
}

// StaticMachineResolver resolves the same machine for every run. It is the
// resolver used when the controller is handed an already-compiled definition.
type StaticMachineResolver struct {
	Machine StateMachine
}

// Resolve returns the configured machine.
func (s StaticMachineResolver) Resolve(context.Context, *apiv1.GooberRun) (StateMachine, error) {
	if s.Machine == nil {
		return nil, fmt.Errorf("kuberunner: no machine configured")
	}
	return s.Machine, nil
}
