package kuberunner

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	apiv1 "github.com/goobers/goobers/api/v1alpha1"
	"github.com/goobers/goobers/internal/journal"
)

// Stale-action admission.
//
// A human action arrives LATE by nature: a notification waits in a chat client,
// someone clicks an hour later. By then the run may have moved on, re-entered
// the same gate under a NEW occurrence, or been deleted and recreated. Every
// one of those, under name-based addressing, silently resolves the wrong
// decision onto the wrong occurrence — and looks entirely normal afterwards.
//
// These tests pin the two-part guard: run UID plus exact journal sequence.

const testActionName = "action-1"

type actionHarness struct {
	t          *testing.T
	reconciler *ActionReconciler
	client     client.Client
	journal    *fakeJournal
}

func newActionHarness(t *testing.T, runUID string, action *apiv1.GooberRunAction) *actionHarness {
	t.Helper()
	scheme := newTestScheme(t)

	run := &apiv1.GooberRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testRunName,
			Namespace: testNamespace,
			UID:       types.UID(runUID),
		},
		Spec: apiv1.GooberRunSpec{
			RunID:       testRunID,
			Gaggle:      "web",
			JournalRoot: "/journal",
			Trigger:     apiv1.RunTrigger{Kind: "manual"},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run, action).
		WithStatusSubresource(&apiv1.GooberRun{}, &apiv1.GooberRunAction{}).
		Build()

	j := newFakeJournal()
	j.seed(testRunID)

	return &actionHarness{
		t:          t,
		client:     c,
		journal:    j,
		reconciler: &ActionReconciler{Client: c, Scheme: scheme, Journal: j},
	}
}

func (h *actionHarness) reconcile() {
	h.t.Helper()
	if _, err := h.reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testActionName},
	}); err != nil {
		h.t.Fatalf("reconcile action: %v", err)
	}
}

func (h *actionHarness) action() *apiv1.GooberRunAction {
	h.t.Helper()
	var action apiv1.GooberRunAction
	key := types.NamespacedName{Namespace: testNamespace, Name: testActionName}
	if err := h.client.Get(context.Background(), key, &action); err != nil {
		h.t.Fatalf("get action: %v", err)
	}
	return &action
}

func gateAction(runUID string, seq int64) *apiv1.GooberRunAction {
	return &apiv1.GooberRunAction{
		ObjectMeta: metav1.ObjectMeta{Name: testActionName, Namespace: testNamespace, Generation: 1},
		Spec: apiv1.GooberRunActionSpec{
			Kind:     apiv1.ActionGateDecision,
			Actor:    "alice",
			Decision: "pass",
			Target: apiv1.ActionTarget{
				RunName:       testRunName,
				RunUID:        runUID,
				RunID:         testRunID,
				OccurrenceSeq: seq,
			},
		},
	}
}

// pauseRun parks the run at a gate and returns the pause sequence.
func (h *actionHarness) pauseRun(gate string) uint64 {
	h.t.Helper()
	seq, err := h.journal.AppendGatePaused(testRunID, gate, 0)
	if err != nil {
		h.t.Fatalf("pause run: %v", err)
	}
	return seq
}

func TestActionAcceptedForTheCurrentOccurrence(t *testing.T) {
	h := newActionHarness(t, testRunUID, gateAction(testRunUID, 2))
	if seq := h.pauseRun("approve"); seq != 2 {
		t.Fatalf("precondition: pause seq = %d, want 2", seq)
	}

	h.reconcile()

	action := h.action()
	if action.Status.Phase != apiv1.ActionAccepted {
		t.Fatalf("phase = %q (%s), want %q",
			action.Status.Phase, action.Status.Message, apiv1.ActionAccepted)
	}
}

// The core stale-click case: the run paused, resumed, and paused AGAIN at the
// same gate. An action quoting the FIRST pause must not resolve the second.
func TestActionRejectedForASupersededOccurrenceOfTheSameGate(t *testing.T) {
	h := newActionHarness(t, testRunUID, gateAction(testRunUID, 2))

	firstPause := h.pauseRun("approve")
	// The run moves on and returns to the very same gate.
	if _, err := h.journal.append(testRunID, journal.Event{
		Type: journal.EventGateEvaluated, Gate: "approve", Target: "build",
	}); err != nil {
		t.Fatalf("advance: %v", err)
	}
	secondPause := h.pauseRun("approve")

	if firstPause == secondPause {
		t.Fatal("precondition: the two pauses must have distinct sequences")
	}

	h.reconcile()

	action := h.action()
	if action.Status.Phase != apiv1.ActionRejected {
		t.Fatalf("phase = %q, want %q: an action for a superseded occurrence was admitted",
			action.Status.Phase, apiv1.ActionRejected)
	}
	if action.Status.RejectionReason != apiv1.ActionRejectedStaleOccurrence {
		t.Errorf("reason = %q, want %q", action.Status.RejectionReason, apiv1.ActionRejectedStaleOccurrence)
	}
}

// A GooberRun deleted and recreated under the same NAME is a different
// occurrence. A queued action naming the old UID must not land on it.
func TestActionRejectedForAStaleRunUID(t *testing.T) {
	h := newActionHarness(t, "uid-recreated", gateAction("uid-original", 2))
	h.pauseRun("approve")

	h.reconcile()

	action := h.action()
	if action.Status.Phase != apiv1.ActionRejected {
		t.Fatalf("phase = %q, want %q: an action for a previous CR occurrence was admitted",
			action.Status.Phase, apiv1.ActionRejected)
	}
	if action.Status.RejectionReason != apiv1.ActionRejectedRunUIDMismatch {
		t.Errorf("reason = %q, want %q", action.Status.RejectionReason, apiv1.ActionRejectedRunUIDMismatch)
	}
}

func TestActionRejectedWhenTheRunIsNotPaused(t *testing.T) {
	// No gate.paused at all: the run is executing, so there is no decision to
	// answer.
	h := newActionHarness(t, testRunUID, gateAction(testRunUID, 1))

	h.reconcile()

	action := h.action()
	if action.Status.Phase != apiv1.ActionRejected {
		t.Fatalf("phase = %q, want %q", action.Status.Phase, apiv1.ActionRejected)
	}
	if action.Status.RejectionReason != apiv1.ActionRejectedStaleOccurrence {
		t.Errorf("reason = %q, want %q", action.Status.RejectionReason, apiv1.ActionRejectedStaleOccurrence)
	}
}

func TestActionRejectedWhenTheRunDoesNotExist(t *testing.T) {
	action := gateAction(testRunUID, 2)
	action.Spec.Target.RunName = "no-such-run"
	h := newActionHarness(t, testRunUID, action)
	h.pauseRun("approve")

	h.reconcile()

	got := h.action()
	if got.Status.Phase != apiv1.ActionRejected {
		t.Fatalf("phase = %q, want %q", got.Status.Phase, apiv1.ActionRejected)
	}
	if got.Status.RejectionReason != apiv1.ActionRejectedRunNotFound {
		t.Errorf("reason = %q, want %q", got.Status.RejectionReason, apiv1.ActionRejectedRunNotFound)
	}
}

// A gate name is never sufficient identity. An action naming a DIFFERENT gate
// than the one the run is parked at is refused even when the sequence lines up.
func TestActionRejectedWhenItNamesADifferentGate(t *testing.T) {
	action := gateAction(testRunUID, 2)
	action.Spec.Target.Stage = "some-other-gate"
	h := newActionHarness(t, testRunUID, action)
	h.pauseRun("approve")

	h.reconcile()

	if got := h.action(); got.Status.Phase != apiv1.ActionRejected {
		t.Fatalf("phase = %q, want %q", got.Status.Phase, apiv1.ActionRejected)
	}
}

// Applied and Rejected are terminal. A requeue must not re-evaluate a decided
// action, or a decision could be journalled twice.
func TestDecidedActionsAreNotReevaluated(t *testing.T) {
	h := newActionHarness(t, testRunUID, gateAction(testRunUID, 2))
	h.pauseRun("approve")

	h.reconcile()
	first := h.action()
	if first.Status.Phase != apiv1.ActionAccepted {
		t.Fatalf("precondition: phase = %q", first.Status.Phase)
	}

	// Mark it applied, as the committing slice will.
	first.Status.Phase = apiv1.ActionApplied
	first.Status.AppliedSeq = 3
	if err := h.client.Status().Update(context.Background(), first); err != nil {
		t.Fatalf("mark applied: %v", err)
	}

	h.reconcile()

	after := h.action()
	if after.Status.Phase != apiv1.ActionApplied {
		t.Fatalf("phase = %q, want it to stay %q", after.Status.Phase, apiv1.ActionApplied)
	}
	if after.Status.AppliedSeq != 3 {
		t.Errorf("appliedSeq = %d, want the original receipt 3", after.Status.AppliedSeq)
	}
}

// Direct coverage of the occurrence rule for every action kind.
func TestCheckOccurrence(t *testing.T) {
	paused := JournalHead{Seq: 5, Phase: journal.PhaseRunning, Wait: WaitHumanGate, WaitGate: "approve", WaitSeq: 5}
	running := JournalHead{Seq: 5, Phase: journal.PhaseRunning}
	terminal := JournalHead{Seq: 9, Phase: journal.PhaseEscalated, TerminalStatus: "escalated"}

	spec := func(kind apiv1.GooberRunActionKind, seq int64) apiv1.GooberRunActionSpec {
		return apiv1.GooberRunActionSpec{
			Kind:   kind,
			Target: apiv1.ActionTarget{OccurrenceSeq: seq},
		}
	}

	for _, tc := range []struct {
		name    string
		spec    apiv1.GooberRunActionSpec
		head    JournalHead
		wantErr bool
	}{
		{"gate decision at the current pause", spec(apiv1.ActionGateDecision, 5), paused, false},
		{"gate decision at a stale pause", spec(apiv1.ActionGateDecision, 3), paused, true},
		{"gate decision while running", spec(apiv1.ActionGateDecision, 5), running, true},
		{"override at the current pause", spec(apiv1.ActionGateOverride, 5), paused, false},
		{"resume from the terminal head", spec(apiv1.ActionResumeFromTerminal, 9), terminal, false},
		{"resume from a stale terminal seq", spec(apiv1.ActionResumeFromTerminal, 4), terminal, true},
		{"resume a non-terminal run", spec(apiv1.ActionResumeFromTerminal, 5), running, true},
		{"cancel a live run", spec(apiv1.ActionCancel, 5), running, false},
		{"cancel a terminal run", spec(apiv1.ActionCancel, 9), terminal, true},
		// A sequence beyond the head means the requester saw a future the
		// journal has not reached: a mistake or a replay, never valid.
		{"rerun quoting a future sequence", spec(apiv1.ActionStageRerun, 99), running, true},
		{"rerun quoting a past sequence", spec(apiv1.ActionStageRerun, 2), running, false},
		{"unknown kind", spec("Nonsense", 1), running, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkOccurrence(tc.spec, tc.head)
			if tc.wantErr && err == nil {
				t.Fatal("expected the occurrence check to refuse this action")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("occurrence check refused a valid action: %v", err)
			}
		})
	}
}
