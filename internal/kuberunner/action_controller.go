package kuberunner

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apiv1 "github.com/goobers/goobers/api/v1alpha1"
)

// Occurrence-bound action admission.
//
// A human intervention is dangerous in a way an ordinary spec edit is not: it
// arrives LATE. A notification sits in a chat client, someone opens it an hour
// later and clicks "approve", and by then the run may have moved on, timed out
// and re-entered the same gate, or been deleted and recreated entirely. Every
// one of those cases would, under name-based addressing, resolve the wrong
// decision onto the wrong occurrence — and it would look completely normal in
// the audit trail.
//
// The guard is that an action must quote BOTH:
//
//   - the run's Kubernetes UID, which a recreated CR does not share; and
//   - the exact journal sequence of the pause it answers.
//
// Both must match the run's CURRENT authoritative position or the action is
// rejected with a stable reason. A gate name is never sufficient identity,
// because one gate can pause many times in a single run.
//
// Rejection is terminal and idempotent, as is application: an action carries
// its own receipt in status, so a requeue never journals a decision twice.

// ActionReconciler reconciles GooberRunAction objects.
type ActionReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// Journal is the canonical authority the action is validated against.
	Journal JournalStore
}

// Reconcile admits or rejects one action.
func (r *ActionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var action apiv1.GooberRunAction
	if err := r.Get(ctx, req.NamespacedName, &action); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Applied and Rejected are terminal. A one-shot fact is evaluated once.
	switch action.Status.Phase {
	case apiv1.ActionApplied, apiv1.ActionRejected:
		return ctrl.Result{}, nil
	}

	var run apiv1.GooberRun
	key := client.ObjectKey{Namespace: action.Namespace, Name: action.Spec.Target.RunName}
	if err := r.Get(ctx, key, &run); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, r.reject(ctx, &action, apiv1.ActionRejectedRunNotFound,
				fmt.Sprintf("no GooberRun %q in namespace %q", action.Spec.Target.RunName, action.Namespace))
		}
		return ctrl.Result{}, fmt.Errorf("get target run: %w", err)
	}

	// UID check. A CR deleted and recreated under the same name is a DIFFERENT
	// occurrence and inherits nothing from its predecessor, so a queued action
	// naming the old UID must not land on the new object.
	if string(run.UID) != action.Spec.Target.RunUID {
		return ctrl.Result{}, r.reject(ctx, &action, apiv1.ActionRejectedRunUIDMismatch,
			fmt.Sprintf("action targets run UID %q but %q currently has UID %q",
				action.Spec.Target.RunUID, action.Spec.Target.RunName, run.UID))
	}

	// Cross-check the canonical run id when the action carries one.
	if action.Spec.Target.RunID != "" && action.Spec.Target.RunID != run.Spec.RunID {
		return ctrl.Result{}, r.reject(ctx, &action, apiv1.ActionRejectedRunUIDMismatch,
			fmt.Sprintf("action targets run id %q but the run is %q",
				action.Spec.Target.RunID, run.Spec.RunID))
	}

	// Occurrence check against the JOURNAL, not against CR status: status is a
	// projection and may lag, and admitting a human decision on a lagging
	// projection is precisely the stale-click failure this guards.
	head, err := r.Journal.Head(run.Spec.RunID)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("read journal head: %w", err)
	}

	if err := checkOccurrence(action.Spec, head); err != nil {
		return ctrl.Result{}, r.reject(ctx, &action, apiv1.ActionRejectedStaleOccurrence, err.Error())
	}

	// Accepted. This slice validates and records admission; committing the
	// resulting journal transition is the next slice's work, and is left
	// explicitly undone rather than half-done.
	return ctrl.Result{}, r.setPhase(ctx, &action, apiv1.ActionAccepted, "Accepted",
		fmt.Sprintf("action targets the current occurrence (seq %d)", action.Spec.Target.OccurrenceSeq))
}

// checkOccurrence verifies the action answers the run's CURRENT occurrence.
func checkOccurrence(spec apiv1.GooberRunActionSpec, head JournalHead) error {
	switch spec.Kind {
	case apiv1.ActionGateDecision, apiv1.ActionGateOverride:
		if head.Wait != WaitHumanGate {
			return fmt.Errorf("run is not paused at a gate (journal seq %d, state %q)", head.Seq, head.State)
		}
		if projectSeq(head.WaitSeq) != spec.Target.OccurrenceSeq {
			return fmt.Errorf("action answers pause at seq %d but the run is paused at seq %d",
				spec.Target.OccurrenceSeq, projectSeq(head.WaitSeq))
		}
		if spec.Target.Stage != "" && spec.Target.Stage != head.WaitGate {
			return fmt.Errorf("action names gate %q but the run is paused at %q",
				spec.Target.Stage, head.WaitGate)
		}
		return nil

	case apiv1.ActionResumeFromTerminal:
		if !head.IsTerminal() {
			return fmt.Errorf("run is not terminal (journal seq %d)", head.Seq)
		}
		if projectSeq(head.Seq) != spec.Target.OccurrenceSeq {
			return fmt.Errorf("action resumes from seq %d but the terminal head is seq %d",
				spec.Target.OccurrenceSeq, projectSeq(head.Seq))
		}
		return nil

	case apiv1.ActionStageRerun, apiv1.ActionCancel:
		// These act on the live head. Quoting a sequence beyond it means the
		// requester saw a future the journal has not reached, which can only be
		// a mistake or a replay.
		if spec.Target.OccurrenceSeq > projectSeq(head.Seq) {
			return fmt.Errorf("action quotes seq %d beyond the journal head at seq %d",
				spec.Target.OccurrenceSeq, projectSeq(head.Seq))
		}
		if head.IsTerminal() && spec.Kind == apiv1.ActionCancel {
			return fmt.Errorf("run is already terminal (%q)", head.TerminalStatus)
		}
		return nil

	default:
		return fmt.Errorf("unknown action kind %q", spec.Kind)
	}
}

func (r *ActionReconciler) reject(ctx context.Context, action *apiv1.GooberRunAction, reason, message string) error {
	action.Status.RejectionReason = reason
	return r.setPhase(ctx, action, apiv1.ActionRejected, reason, message)
}

func (r *ActionReconciler) setPhase(ctx context.Context, action *apiv1.GooberRunAction, phase apiv1.GooberRunActionPhase, reason, message string) error {
	action.Status.ObservedGeneration = action.Generation
	action.Status.Phase = phase
	action.Status.Message = message

	condStatus := metav1.ConditionTrue
	if phase == apiv1.ActionRejected {
		condStatus = metav1.ConditionFalse
	}
	apimeta.SetStatusCondition(&action.Status.Conditions, metav1.Condition{
		Type:               apiv1.GooberRunConditionReady,
		Status:             condStatus,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: action.Generation,
	})

	if err := r.Status().Update(ctx, action); err != nil {
		return fmt.Errorf("update action status: %w", err)
	}
	return nil
}

// SetupWithManager registers the action reconciler.
func (r *ActionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&apiv1.GooberRunAction{}).
		Complete(r)
}
