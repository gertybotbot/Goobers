// Package kuberunner implements the Kubernetes-native Goobers runner: a
// controller-runtime reconciler that advances pinned workflow runs by creating
// one Kubernetes Job per executable stage attempt.
//
// # Authority
//
// The canonical append-only run journal is the ONLY authority. Everything else
// in this package is a projection or an execution surface:
//
//   - GooberRun.status is a projection. It can be deleted wholesale and
//     rebuilt exactly by one reconcile. When status and journal disagree, the
//     journal wins, unconditionally.
//   - A Kubernetes Job is execution evidence. A Job reaching Complete does NOT
//     advance a run. The controller advances a run only after it has fetched
//     and fully validated the attempt's result receipt and committed
//     stage.finished to the journal.
//   - Retry is owned here, not by the Job. Every attempt Job is created with
//     backoffLimit: 0 so that exactly one Pod runs per attempt and every retry
//     is a distinct, separately journalled attempt with its own deterministic
//     Job name.
//
// # Crash safety
//
// The dangerous window is between "decide to dispatch" and "Job exists". The
// controller closes it by appending stage.started FIRST and deriving the Job
// name deterministically from durable facts (run UID, state, branch, attempt).
// After a crash anywhere in that window, the next reconcile recomputes the same
// name; creating it again returns AlreadyExists, which is treated as success.
// No attempt is ever dispatched twice, and no dispatch is ever lost.
//
// # Business fencing
//
// Provider-item runs acquire a durable business claim before dispatch. The
// claim's monotonic epoch is carried by the Job and result receipt, and is
// revalidated immediately before result acceptance. Kubernetes leader-election
// Leases remain liveness machinery and are never business authority.
package kuberunner

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	apiv1 "github.com/goobers/goobers/api/v1alpha1"
	"github.com/goobers/goobers/internal/journal"
)

// DefaultWorkerImage is the attempt image used when a run does not pin one.
const DefaultWorkerImage = "ghcr.io/goobers/goober-runtime:latest"

// RunReconciler reconciles GooberRun objects.
type RunReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// Journal is the canonical authority. It is required.
	Journal JournalStore
	// Results fetches published attempt receipts. It is required.
	Results ResultReader
	// Machine resolves the pinned workflow machine for a run. It is required.
	Machine MachineResolver
	// Claims is durable provider-item ownership. It is required for runs whose
	// pinned repository identity has an ExternalID.
	Claims ClaimStore
	// ClaimTTL controls renewal cadence. Zero selects defaultClaimTTL.
	ClaimTTL time.Duration
	// WorkerImage overrides the attempt container image.
	WorkerImage string
	// CredentialBindings maps a declared stage capability to one Kubernetes
	// Secret key and process environment variable. The Job receives bindings
	// only for capabilities in its pinned AttemptPlan; undeclared credentials
	// are never materialized.
	CredentialBindings map[string]CredentialSecretBinding
	// GooberCapabilities supplies a referenced reviewer goober's declared grants.
	// Agentic gates have no stage-level capability field, so (as in the local
	// runner) their goober definition is the upper and effective bound.
	GooberCapabilities map[string][]string
	// ClaimNamespace is passed to workers so they assert the same centralized
	// retained-claim authority as the controller.
	ClaimNamespace string
	// Notifications derives durable output requests from committed journal
	// occurrences. It is optional and never participates in workflow decisions.
	Notifications *JournalNotificationProjector
}

// MachineResolver resolves the pinned compiled machine for a run.
//
// It is an interface, and the reconciler consults it for EVERY structural
// question (what kind of state is this? what comes next?), because the answer
// must come from the run's PINNED definition digest and never from live config.
// A config reload must not be able to retune a run in flight.
type MachineResolver interface {
	// Resolve returns the state machine view for a run's pinned workflow.
	Resolve(ctx context.Context, run *apiv1.GooberRun) (StateMachine, error)
}

// StateKind classifies what a machine state requires of the runner.
type StateKind string

const (
	// StateTask is an executable stage: it gets a Job.
	StateTask StateKind = "task"
	// StateHumanGate is a human decision. It executes NO process and gets NO
	// Job; the run parks until an occurrence-bound action arrives.
	StateHumanGate StateKind = "human-gate"
	// StateAutomatedGate is a bounded, side-effect-free check the controller
	// may evaluate locally. This slice does not evaluate them yet; it parks
	// rather than guessing.
	StateAutomatedGate StateKind = "automated-gate"
	// StateTerminal is a terminal target (@complete, @abort, @escalate).
	StateTerminal StateKind = "terminal"
	// StateUnknown means the state is not in the pinned machine.
	StateUnknown StateKind = "unknown"
)

// StateMachine is the minimal structural view the reconciler needs. It is
// deliberately narrow: this slice decides WHERE a run goes only insofar as it
// must know whether to dispatch, park, or terminalize.
type StateMachine interface {
	// Start returns the machine's entry state.
	Start() string
	// Kind classifies a state.
	Kind(state string) StateKind
	// Next returns the state to advance to after a stage finished with status.
	// The bool is false when the transition terminalizes the run.
	Next(state string, status apiv1.ResultStatus) (string, bool)
	// TerminalPhase maps a terminal target onto its journal phase.
	TerminalPhase(target string) journal.RunPhase
	// MaxAttempts returns the attempt budget for a state (1 = no retry).
	MaxAttempts(state string) int
}

// +kubebuilder:rbac:groups=goobers.dev,resources=gooberruns,verbs=get;list;watch
// +kubebuilder:rbac:groups=goobers.dev,resources=gooberruns/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=goobers.dev,resources=gooberrunactions,verbs=get;list;watch
// +kubebuilder:rbac:groups=goobers.dev,resources=gooberrunactions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch

// Reconcile advances one GooberRun.
//
// The order of the steps below is the correctness argument, not a style choice:
//
//  1. Read the journal FIRST. Nothing is decided from CR status.
//  2. If terminal, project and stop. Create no Job.
//  3. If parked at a human gate, project Waiting and stop. Create no Job.
//  4. If an attempt is open, judge it from its RESULT, never from Job status.
//  5. Otherwise dispatch: append intent, then create the deterministic Job.
//  6. Project status last, as a pure function of the journal head.
//
// Every step is idempotent. Re-running Reconcile from any point reaches the
// same state without inventing an unjournalled transition.
func (r *RunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, reconcileErr error) {
	// A notification projection failure must not become workflow authority: the
	// reconcile still advances from the journal. It does, however, need another
	// pass so a terminal run is not left without its durable output request.
	notificationRetry := false
	defer func() {
		if notificationRetry && reconcileErr == nil && !result.Requeue &&
			(result.RequeueAfter == 0 || result.RequeueAfter > time.Second) {
			result.RequeueAfter = time.Second
		}
	}()

	var run apiv1.GooberRun
	if err := r.Get(ctx, req.NamespacedName, &run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Step 1: the journal is read before anything else, and is the only input
	// to the decision below.
	head, err := r.Journal.Head(run.Spec.RunID)
	if err != nil {
		if errors.Is(err, ErrRunNotFound) {
			// The CR exists but its journal does not. This is not something the
			// controller may repair by inventing a journal: doing so would make
			// the CR the authority, which is exactly the inversion this design
			// forbids. Surface it and wait.
			return ctrl.Result{}, r.projectNotReady(ctx, &run, "JournalMissing",
				fmt.Sprintf("no canonical journal for run %q", run.Spec.RunID))
		}
		return ctrl.Result{}, fmt.Errorf("read journal head: %w", err)
	}

	if r.Notifications != nil {
		projection, notificationErr := r.Notifications.Project(ctx, string(run.UID), run.Spec.RunID)
		if notificationErr != nil {
			notificationRetry = true
			log.FromContext(ctx).Error(notificationErr,
				"durable notification projection failed; workflow authority is unaffected",
				"runId", run.Spec.RunID)
		} else if projection.Created > 0 {
			// notification.requested is itself a committed journal event. Reread so
			// status.observedSeq reflects it and every later decision uses one head.
			head, err = r.Journal.Head(run.Spec.RunID)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("reread journal after notification publication: %w", err)
			}
		}
		for _, wakeErr := range projection.WakeErrors {
			log.FromContext(ctx).Error(wakeErr,
				"advisory notification wake gossip failed; durable journal publication is unaffected",
				"runId", run.Spec.RunID, "publicationSeq", projection.HighestSeq)
		}
	}

	machine, err := r.Machine.Resolve(ctx, &run)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("resolve pinned machine: %w", err)
	}

	// Business claims are acquired and renewed from durable authority before
	// any executable decision. The journal records the granted epoch so a new
	// controller process recovers the same fence rather than trusting status.
	if key, required := businessClaimKey(&run); required {
		if head.IsTerminal() {
			return r.disposeTerminalClaim(ctx, &run, head)
		}
		ready, result, claimErr := r.ensureBusinessClaim(ctx, &run, head, key)
		if claimErr != nil || !ready {
			return result, claimErr
		}
		// Re-read after acquisition because the newly committed claim event is
		// authoritative input to every later attempt identity.
		head, err = r.Journal.Head(run.Spec.RunID)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("reread journal after claim: %w", err)
		}
	}

	// Step 2: terminal runs are done. No Job, no advancement, just projection.
	if head.IsTerminal() {
		return ctrl.Result{}, r.project(ctx, &run, head)
	}

	// Step 3: a human gate waits. It executes no process, so creating a Job for
	// it would be a category error — there is nothing to run.
	if head.Wait == WaitHumanGate {
		return r.claimRenewalResult(&run), r.project(ctx, &run, head)
	}

	// Step 4: an open attempt is judged by its result, never by Job status.
	if head.HasOpenAttempt() {
		return r.judgeOpenAttempt(ctx, &run, head, machine)
	}

	// Step 5: nothing open — decide what this state requires.
	state := head.State
	if state == "" {
		state = machine.Start()
	}
	if err := r.assertHeadFence(ctx, &run, head); err != nil {
		return ctrl.Result{}, r.projectNotReady(ctx, &run, "ClaimFenceLost",
			fmt.Sprintf("run no longer holds the current business fence: %v", err))
	}

	switch machine.Kind(state) {
	case StateTerminal:
		phase := machine.TerminalPhase(state)
		if _, err := r.Journal.AppendRunFinished(run.Spec.RunID, phase, state); err != nil {
			return ctrl.Result{}, fmt.Errorf("commit run.finished: %w", err)
		}
		return r.requeueAfterJournalWrite(ctx, &run)

	case StateHumanGate:
		// Park the run at this occurrence. gate.paused carries the sequence a
		// later action must quote, which is what makes a stale click detectable.
		if _, err := r.Journal.AppendGatePaused(run.Spec.RunID, state, head.Branch); err != nil {
			return ctrl.Result{}, fmt.Errorf("commit gate.paused: %w", err)
		}
		return r.requeueAfterJournalWrite(ctx, &run)

	case StateAutomatedGate:
		// Not evaluated in this slice. Parking is the honest behaviour: an
		// unevaluated gate must not be silently treated as a pass.
		return r.claimRenewalResult(&run), r.projectNotReady(ctx, &run, "AutomatedGateUnsupported",
			fmt.Sprintf("automated gate %q is not evaluated by this slice", state))

	case StateTask:
		return r.dispatchAttempt(ctx, &run, head, state, machine)

	default:
		return r.claimRenewalResult(&run), r.projectNotReady(ctx, &run, "UnknownState",
			fmt.Sprintf("state %q is not present in the pinned machine", state))
	}
}

// judgeOpenAttempt decides the fate of an in-flight attempt.
//
// The critical rule lives here: a Job's completion is NOT the transition. Even
// a Job whose status says Complete advances nothing unless its published result
// validates. A Job that succeeded but published a missing, malformed,
// misaddressed, or digest-mismatched result leaves the run exactly where it
// was, because "the process exited 0" is not the same claim as "the stage
// decided this".
func (r *RunReconciler) judgeOpenAttempt(ctx context.Context, run *apiv1.GooberRun, head JournalHead, machine StateMachine) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	attempt := AttemptID{
		RunUID:     string(run.UID),
		RunID:      run.Spec.RunID,
		State:      head.OpenStage,
		Attempt:    head.OpenAttempt,
		Branch:     head.Branch,
		FenceEpoch: claimEpoch(head),
	}

	// Try the result FIRST, before looking at the Job at all. A result that
	// validates is sufficient to advance regardless of what the Job's status
	// field currently says; the Job is only consulted to distinguish "still
	// running" from "died without publishing".
	raw, readErr := r.Results.ReadResult(attempt)
	if readErr == nil {
		result, valErr := ValidateResult(attempt, raw)
		if valErr == nil {
			return r.commitAttempt(ctx, run, attempt, result, machine)
		}
		// Published but invalid. This is a hard stop, not a retry loop: the
		// attempt made a claim the controller cannot verify, so the run does
		// not advance and an operator must look.
		logger.Error(valErr, "attempt published an invalid result; refusing to advance",
			"attempt", attempt.String())
		return r.claimRenewalResult(run), r.projectNotReady(ctx, run, "InvalidResult",
			fmt.Sprintf("attempt %s published an unusable result: %v", attempt.String(), valErr))
	}
	if !errors.Is(readErr, ErrResultMissing) {
		return ctrl.Result{}, fmt.Errorf("read attempt result: %w", readErr)
	}

	// No result yet. Consult the Job only to tell "in flight" from "gone".
	var job batchv1.Job
	jobName := JobName(attempt)
	err := r.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: jobName}, &job)
	switch {
	case apierrors.IsNotFound(err):
		// Journal says an attempt is open but no Job exists. Either the crash
		// window between append and create is still open, or the Job was
		// deleted. Recreating under the SAME deterministic name is safe and is
		// exactly what makes the append-then-create ordering recoverable.
		return r.createAttemptJob(ctx, run, attempt, machine)
	case err != nil:
		return ctrl.Result{}, fmt.Errorf("get attempt job: %w", err)
	}

	if jobFailed(&job) {
		// The Job failed without publishing a result. Retry ownership is the
		// controller's: it decides whether a NEW attempt is dispatched, and
		// that attempt gets its own journal events and its own Job name. The
		// failed Job is never restarted in place — backoffLimit is 0.
		return r.handleFailedAttempt(ctx, run, attempt, machine)
	}

	// Pending, running, or complete-but-not-yet-published. Wait. A Job that
	// reports Complete with no result lands here and is picked up by the next
	// reconcile; if it never publishes, the failure path above eventually
	// applies once the Job is observed failed, and a completed-but-silent Job
	// is surfaced rather than guessed at.
	if jobSucceeded(&job) {
		return r.claimRenewalResult(run), r.projectNotReady(ctx, run, "ResultMissing",
			fmt.Sprintf("attempt %s job completed without publishing a result", attempt.String()))
	}
	return r.claimRenewalResult(run), r.project(ctx, run, head)
}

// commitAttempt journals a validated result and, when the transition
// terminalizes, the terminal run.finished.
//
// Ordering is load-bearing: stage.finished commits BEFORE run.finished, always,
// and both commit before status is projected. A reader replaying the journal
// therefore always sees the stage settle before the run does.
func (r *RunReconciler) commitAttempt(ctx context.Context, run *apiv1.GooberRun, attempt AttemptID, result ValidatedResult, machine StateMachine) (ctrl.Result, error) {
	if err := r.assertAttemptFence(ctx, run, attempt); err != nil {
		return ctrl.Result{}, r.projectNotReady(ctx, run, "ClaimFenceLost",
			fmt.Sprintf("attempt %s no longer holds the current business fence: %v", attempt.String(), err))
	}
	if _, err := r.Journal.AppendStageFinished(run.Spec.RunID, attempt, result); err != nil {
		return ctrl.Result{}, fmt.Errorf("commit stage.finished: %w", err)
	}

	next, ok := machine.Next(attempt.State, result.Envelope.Status)
	if !ok || machine.Kind(next) == StateTerminal {
		target := next
		if !ok {
			target = journal.TargetComplete
		}
		phase := machine.TerminalPhase(target)
		if _, err := r.Journal.AppendRunFinished(run.Spec.RunID, phase, target); err != nil {
			return ctrl.Result{}, fmt.Errorf("commit run.finished: %w", err)
		}
	}

	return r.requeueAfterJournalWrite(ctx, run)
}

// handleFailedAttempt applies the controller's retry decision.
func (r *RunReconciler) handleFailedAttempt(ctx context.Context, run *apiv1.GooberRun, attempt AttemptID, machine StateMachine) (ctrl.Result, error) {
	if err := r.assertAttemptFence(ctx, run, attempt); err != nil {
		return ctrl.Result{}, r.projectNotReady(ctx, run, "ClaimFenceLost",
			fmt.Sprintf("failed attempt %s no longer holds the current business fence: %v", attempt.String(), err))
	}
	// Close the failed attempt in the journal so the retry is a visibly
	// separate attempt rather than an unexplained second Job.
	failed := ValidatedResult{
		Attempt: attempt,
		Envelope: apiv1.ResultEnvelope{
			Status: apiv1.ResultFailure,
			Error: &apiv1.ErrorInfo{
				Code:    "attempt_job_failed",
				Message: "attempt job failed without publishing a result",
			},
		},
	}
	if _, err := r.Journal.AppendStageFinished(run.Spec.RunID, attempt, failed); err != nil {
		return ctrl.Result{}, fmt.Errorf("commit failed stage.finished: %w", err)
	}

	if attempt.Attempt >= machine.MaxAttempts(attempt.State) {
		next, ok := machine.Next(attempt.State, apiv1.ResultFailure)
		target := next
		if !ok {
			target = "@abort"
		}
		phase := machine.TerminalPhase(target)
		if _, err := r.Journal.AppendRunFinished(run.Spec.RunID, phase, target); err != nil {
			return ctrl.Result{}, fmt.Errorf("commit run.finished: %w", err)
		}
	}
	return r.requeueAfterJournalWrite(ctx, run)
}

// dispatchAttempt opens a new attempt: append intent, then create the Job.
func (r *RunReconciler) dispatchAttempt(ctx context.Context, run *apiv1.GooberRun, head JournalHead, state string, machine StateMachine) (ctrl.Result, error) {
	attempt := AttemptID{
		RunUID:     string(run.UID),
		RunID:      run.Spec.RunID,
		State:      state,
		Attempt:    head.NextAttempt(state, head.Branch),
		Branch:     head.Branch,
		FenceEpoch: claimEpoch(head),
	}

	// Resolve and validate pinned source before opening the durable attempt. In
	// particular, a mutation-capable agent is refused before stage.started so it
	// cannot receive a raw write credential and leave an unexecutable open slot.
	if _, err := r.attemptPlan(run, attempt, machine); err != nil {
		return ctrl.Result{}, r.projectNotReady(ctx, run, "AttemptSourceInvalid", err.Error())
	}

	// Intent is journalled BEFORE the Job exists. If the process dies here, the
	// next reconcile sees an open attempt with no Job and recreates it under
	// the same deterministic name.
	if _, err := r.Journal.AppendStageStarted(run.Spec.RunID, attempt); err != nil {
		return ctrl.Result{}, fmt.Errorf("commit stage.started: %w", err)
	}
	return r.createAttemptJob(ctx, run, attempt, machine)
}

// createAttemptJob creates the deterministically-named Job, treating an
// existing one as success.
func (r *RunReconciler) createAttemptJob(ctx context.Context, run *apiv1.GooberRun, attempt AttemptID, machine StateMachine) (ctrl.Result, error) {
	if err := r.assertAttemptFence(ctx, run, attempt); err != nil {
		return ctrl.Result{}, r.projectNotReady(ctx, run, "ClaimFenceLost",
			fmt.Sprintf("refusing to dispatch %s without the current business fence: %v", attempt.String(), err))
	}
	plan, err := r.attemptPlan(run, attempt, machine)
	if err != nil {
		return ctrl.Result{}, r.projectNotReady(ctx, run, "AttemptSourceInvalid", err.Error())
	}
	job, err := r.desiredJob(run, attempt, plan)
	if err != nil {
		return ctrl.Result{}, r.projectNotReady(ctx, run, "AttemptSourceInvalid", err.Error())
	}
	if err := controllerutil.SetControllerReference(run, job, r.Scheme); err != nil {
		return ctrl.Result{}, fmt.Errorf("set owner reference: %w", err)
	}

	err = r.Create(ctx, job)
	switch {
	case err == nil:
	case apierrors.IsAlreadyExists(err):
		// Create-once, but never blind adoption. Attempt Jobs now carry scoped
		// credentials, so a foreign or differently sourced object under the
		// deterministic name must not be mistaken for this occurrence.
		var existing batchv1.Job
		if getErr := r.Get(ctx, client.ObjectKey{Namespace: job.Namespace, Name: job.Name}, &existing); getErr != nil {
			return ctrl.Result{}, fmt.Errorf("get existing attempt job: %w", getErr)
		}
		if validateErr := validateExistingAttemptJob(run, job, &existing); validateErr != nil {
			return ctrl.Result{}, r.projectNotReady(ctx, run, "AttemptJobConflict", validateErr.Error())
		}
	default:
		return ctrl.Result{}, fmt.Errorf("create attempt job: %w", err)
	}

	head, headErr := r.Journal.Head(run.Spec.RunID)
	if headErr != nil {
		return ctrl.Result{}, fmt.Errorf("reread journal head: %w", headErr)
	}
	return r.claimRenewalResult(run), r.project(ctx, run, head)
}

// desiredJob builds the attempt Job.
func (r *RunReconciler) desiredJob(run *apiv1.GooberRun, attempt AttemptID, plan *AttemptPlan) (*batchv1.Job, error) {
	labels := AttemptLabels(run.Spec.Gaggle, attempt)
	env := []corev1.EnvVar{
		{Name: "GOOBERS_RUN_ID", Value: attempt.RunID},
		{Name: "GOOBERS_RUN_UID", Value: attempt.RunUID},
		{Name: "GOOBERS_STATE", Value: attempt.State},
		{Name: "GOOBERS_ATTEMPT", Value: fmt.Sprintf("%d", attempt.Attempt)},
		{Name: "GOOBERS_BRANCH", Value: fmt.Sprintf("%d", attempt.Branch)},
		{Name: "GOOBERS_FENCE_EPOCH", Value: fmt.Sprintf("%d", attempt.FenceEpoch)},
		{Name: "GOOBERS_RESULT_PATH", Value: ResultMountPath + "/" + ResultFileName},
		{Name: "GOOBERS_RESULTS_ROOT", Value: ResultMountPath},
		{Name: "GOOBERS_JOURNAL_ROOT", Value: run.Spec.JournalRoot},
	}
	command := []string(nil)
	if plan != nil {
		encoded, err := EncodeAttemptPlan(*plan)
		if err != nil {
			return nil, err
		}
		env = append(env, corev1.EnvVar{Name: AttemptPlanEnv, Value: encoded})
		credentialEnv := map[string]string{}
		capabilities := append([]string(nil), plan.Invocation.Capabilities...)
		sort.Strings(capabilities)
		for _, grant := range capabilities {
			binding, ok := r.CredentialBindings[grant]
			if !ok {
				continue
			}
			if err := binding.validate(grant); err != nil {
				return nil, err
			}
			if previous, exists := credentialEnv[binding.Env]; exists && previous != grant {
				return nil, fmt.Errorf("kuberunner: capabilities %q and %q collide on credential environment %q", previous, grant, binding.Env)
			}
			credentialEnv[binding.Env] = grant
			env = append(env, corev1.EnvVar{Name: binding.Env, ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: binding.SecretName}, Key: binding.SecretKey},
			}})
		}
		command = []string{AttemptWorkerEntry}
	}
	if key, ok := businessClaimKey(run); ok {
		claimNamespace := r.ClaimNamespace
		if claimNamespace == "" {
			claimNamespace = DefaultClaimNamespace
		}
		env = append(env,
			corev1.EnvVar{Name: "GOOBERS_CLAIM_NAMESPACE", Value: claimNamespace},
			corev1.EnvVar{Name: "GOOBERS_CLAIM_GAGGLE", Value: key.Gaggle},
			corev1.EnvVar{Name: "GOOBERS_CLAIM_PROVIDER", Value: key.Provider},
			corev1.EnvVar{Name: "GOOBERS_CLAIM_EXTERNAL_ID", Value: key.ExternalID},
		)
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      JobName(attempt),
			Namespace: run.Namespace,
			Labels:    labels,
			Annotations: map[string]string{
				"goobers.dev/run-id": attempt.RunID,
				"goobers.dev/state":  attempt.State,
			},
		},
		Spec: batchv1.JobSpec{
			// backoffLimit 0 is not a tuning knob. Kubernetes must never retry
			// an attempt on its own: a Kubernetes-initiated retry would be an
			// unjournalled second execution sharing one attempt identity, so
			// two different executions could publish to the same result slot.
			// Retry belongs to the controller, where each retry is a distinct
			// journalled attempt with its own Job.
			BackoffLimit: ptr.To(int32(0)),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy:      corev1.RestartPolicyNever,
					ServiceAccountName: run.Spec.ServiceAccountName,
					Containers: []corev1.Container{{
						Name:    "attempt",
						Image:   r.workerImage(run),
						Command: command,
						Env:     env,
						VolumeMounts: []corev1.VolumeMount{{
							Name:      "result",
							MountPath: ResultMountPath,
						}},
					}},
					Volumes: []corev1.Volume{{
						Name: "result",
						VolumeSource: corev1.VolumeSource{
							EmptyDir: &corev1.EmptyDirVolumeSource{},
						},
					}},
				},
			},
		},
	}
	return job, nil
}

func validateExistingAttemptJob(run *apiv1.GooberRun, want, got *batchv1.Job) error {
	owner := metav1.GetControllerOf(got)
	if owner == nil || owner.Kind != "GooberRun" || owner.Name != run.Name || owner.UID != run.UID {
		return fmt.Errorf("kuberunner: existing Job %q is not controlled by run UID %q", got.Name, run.UID)
	}
	for key, value := range want.Labels {
		if got.Labels[key] != value {
			return fmt.Errorf("kuberunner: existing Job %q has mismatched label %q", got.Name, key)
		}
	}
	wantPod, gotPod := want.Spec.Template.Spec, got.Spec.Template.Spec
	if len(wantPod.Containers) != 1 || len(gotPod.Containers) != 1 ||
		!reflect.DeepEqual(wantPod.Containers[0], gotPod.Containers[0]) ||
		wantPod.ServiceAccountName != gotPod.ServiceAccountName ||
		wantPod.RestartPolicy != gotPod.RestartPolicy ||
		!reflect.DeepEqual(want.Spec.BackoffLimit, got.Spec.BackoffLimit) {
		return fmt.Errorf("kuberunner: existing Job %q does not match the pinned attempt source", got.Name)
	}
	return nil
}

func (r *RunReconciler) workerImage(run *apiv1.GooberRun) string {
	if run.Spec.WorkerImage != "" {
		return run.Spec.WorkerImage
	}
	if r.WorkerImage != "" {
		return r.WorkerImage
	}
	return DefaultWorkerImage
}

// requeueAfterJournalWrite reprojects immediately after a journal commit, so
// the next decision is made against the head that commit produced rather than a
// stale one.
func (r *RunReconciler) requeueAfterJournalWrite(ctx context.Context, run *apiv1.GooberRun) (ctrl.Result, error) {
	head, err := r.Journal.Head(run.Spec.RunID)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("reread journal head: %w", err)
	}
	if err := r.project(ctx, run, head); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil
}

// project writes the status projection.
//
// ProjectStatus is a pure function of the journal head plus the attempt refs,
// which is what makes "delete status and rebuild it exactly" true by
// construction rather than by careful maintenance.
func (r *RunReconciler) project(ctx context.Context, run *apiv1.GooberRun, head JournalHead) error {
	desired := ProjectStatus(head, run.Generation, string(run.UID), run.Spec.RunID)
	return r.applyStatus(ctx, run, desired)
}

// projectNotReady projects the journal head and marks the run not-ready with a
// stable reason. It never invents workflow state.
func (r *RunReconciler) projectNotReady(ctx context.Context, run *apiv1.GooberRun, reason, message string) error {
	head, err := r.Journal.Head(run.Spec.RunID)
	if err != nil && !errors.Is(err, ErrRunNotFound) {
		return fmt.Errorf("read journal head: %w", err)
	}
	desired := ProjectStatus(head, run.Generation, string(run.UID), run.Spec.RunID)
	apimeta.SetStatusCondition(&desired.Conditions, metav1.Condition{
		Type:               apiv1.GooberRunConditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: run.Generation,
	})
	return r.applyStatus(ctx, run, desired)
}

func (r *RunReconciler) applyStatus(ctx context.Context, run *apiv1.GooberRun, desired apiv1.GooberRunStatus) error {
	if statusEqual(run.Status, desired) {
		return nil
	}
	run.Status = desired
	if err := r.Status().Update(ctx, run); err != nil {
		return fmt.Errorf("update status: %w", err)
	}
	return nil
}

// ProjectStatus builds the full status projection from a journal head.
//
// It is exported and pure so that the repair property is directly testable:
// ProjectStatus(head) must equal the status a live reconcile produced, and
// wiping status changes nothing about its output.
func ProjectStatus(head JournalHead, generation int64, runUID, runID string) apiv1.GooberRunStatus {
	status := apiv1.GooberRunStatus{
		ObservedGeneration: generation,
		Phase:              ProjectPhase(head),
		State:              head.State,
		ObservedSeq:        projectSeq(head.Seq),
		TerminalStatus:     head.TerminalStatus,
		LastErrorCode:      head.LastErrorCode,
	}

	if len(head.Attempts) > 0 {
		for _, durable := range head.Attempts {
			attempt := AttemptID{RunUID: runUID, RunID: runID, State: durable.State, Attempt: durable.Number, Branch: durable.Branch}
			ref := apiv1.AttemptRef{JobName: JobName(attempt), State: durable.State, Attempt: int32(durable.Number), Branch: int32(durable.Branch), DispatchedSeq: projectSeq(durable.Seq)}
			if durable.Number > 1 {
				ref.AttemptClass = string(journal.AttemptPolicy)
			}
			status.Attempts = append(status.Attempts, ref)
		}
		sort.SliceStable(status.Attempts, func(i, j int) bool {
			return status.Attempts[i].JobName < status.Attempts[j].JobName
		})
	}

	if head.Wait == WaitHumanGate {
		status.Pause = &apiv1.PauseOccurrence{
			Gate:   head.WaitGate,
			Seq:    projectSeq(head.WaitSeq),
			Branch: int32(head.Branch),
		}
	}

	ready := metav1.Condition{
		Type:               apiv1.GooberRunConditionReady,
		Status:             metav1.ConditionTrue,
		Reason:             "Projected",
		Message:            "status reflects the canonical journal",
		ObservedGeneration: generation,
	}
	settled := metav1.Condition{
		Type:               apiv1.GooberRunConditionSettled,
		Status:             metav1.ConditionFalse,
		Reason:             "NotTerminal",
		Message:            "no run.finished committed",
		ObservedGeneration: generation,
	}
	if head.IsTerminal() {
		settled.Status = metav1.ConditionTrue
		settled.Reason = "RunFinished"
		settled.Message = fmt.Sprintf("run.finished committed with status %q", head.TerminalStatus)
	}
	apimeta.SetStatusCondition(&status.Conditions, ready)
	apimeta.SetStatusCondition(&status.Conditions, settled)
	return status
}

func businessClaimKey(run *apiv1.GooberRun) (ClaimKey, bool) {
	if run.Spec.Repository == nil || run.Spec.Repository.ExternalID == "" {
		return ClaimKey{}, false
	}
	return ClaimKey{Gaggle: run.Spec.Gaggle, Provider: run.Spec.Repository.Provider, ExternalID: run.Spec.Repository.ExternalID}, true
}

func claimEpoch(head JournalHead) int64 {
	if head.Claim == nil {
		return 0
	}
	return head.Claim.Epoch
}

func (r *RunReconciler) claimTTL() time.Duration {
	if r.ClaimTTL > 0 {
		return r.ClaimTTL
	}
	return defaultClaimTTL
}

// claimRenewalResult makes lease renewal independent of watch traffic. A Job
// may run quietly for longer than the claim TTL, and a human gate may receive
// no Kubernetes events at all; neither is permission for the fence to expire.
func (r *RunReconciler) claimRenewalResult(run *apiv1.GooberRun) ctrl.Result {
	if _, required := businessClaimKey(run); !required {
		return ctrl.Result{}
	}
	return ctrl.Result{RequeueAfter: r.claimTTL() / 3}
}

func (r *RunReconciler) ensureBusinessClaim(ctx context.Context, run *apiv1.GooberRun, head JournalHead, key ClaimKey) (bool, ctrl.Result, error) {
	if r.Claims == nil {
		return false, ctrl.Result{}, r.projectNotReady(ctx, run, "ClaimStoreUnavailable", "provider-item run requires durable business-claim authority")
	}
	if head.Claim == nil {
		token, err := r.Claims.Acquire(ctx, run.Namespace, key, run.Spec.RunID, string(run.UID), r.claimTTL())
		if err != nil {
			if errors.Is(err, ErrClaimHeld) || errors.Is(err, ErrClaimContention) {
				return false, ctrl.Result{RequeueAfter: r.claimTTL() / 4}, r.projectNotReady(ctx, run, "ClaimHeld", err.Error())
			}
			return false, ctrl.Result{}, fmt.Errorf("acquire business claim: %w", err)
		}
		if _, err := r.Journal.AppendClaimAcquired(run.Spec.RunID, token); err != nil {
			return false, ctrl.Result{}, fmt.Errorf("commit claim.acquired: %w", err)
		}
		res, err := r.requeueAfterJournalWrite(ctx, run)
		return false, res, err
	}
	if head.Claim.Key != key || head.Claim.RunID != run.Spec.RunID || head.Claim.RunUID != string(run.UID) {
		return false, ctrl.Result{}, r.projectNotReady(ctx, run, "ClaimFenceLost", "journal claim does not identify this run occurrence")
	}
	if _, err := r.Claims.Renew(ctx, run.Namespace, *head.Claim, r.claimTTL()); err != nil {
		if errors.Is(err, ErrClaimFenceLost) || errors.Is(err, ErrClaimContention) {
			return false, ctrl.Result{RequeueAfter: r.claimTTL() / 4}, r.projectNotReady(ctx, run, "ClaimFenceLost", err.Error())
		}
		return false, ctrl.Result{}, fmt.Errorf("renew business claim: %w", err)
	}
	return true, ctrl.Result{}, nil
}

func (r *RunReconciler) assertAttemptFence(ctx context.Context, run *apiv1.GooberRun, attempt AttemptID) error {
	key, required := businessClaimKey(run)
	if !required {
		return nil
	}
	if r.Claims == nil || attempt.FenceEpoch == 0 {
		return ErrClaimFenceLost
	}
	return r.Claims.AssertCurrent(ctx, run.Namespace, ClaimToken{Key: key, RunID: run.Spec.RunID, RunUID: string(run.UID), Epoch: attempt.FenceEpoch})
}

func (r *RunReconciler) assertHeadFence(ctx context.Context, run *apiv1.GooberRun, head JournalHead) error {
	key, required := businessClaimKey(run)
	if !required {
		return nil
	}
	if r.Claims == nil || head.Claim == nil {
		return ErrClaimFenceLost
	}
	token := *head.Claim
	if token.Key != key || token.RunID != run.Spec.RunID || token.RunUID != string(run.UID) {
		return ErrClaimFenceLost
	}
	return r.Claims.AssertCurrent(ctx, run.Namespace, token)
}

func (r *RunReconciler) disposeTerminalClaim(ctx context.Context, run *apiv1.GooberRun, head JournalHead) (ctrl.Result, error) {
	if head.Claim == nil {
		return ctrl.Result{}, r.project(ctx, run, head)
	}
	if r.Claims == nil {
		return ctrl.Result{}, r.projectNotReady(ctx, run, "ClaimStoreUnavailable", "terminal claim cannot be released without durable authority")
	}
	if err := r.Claims.Release(ctx, run.Namespace, *head.Claim); err != nil {
		if errors.Is(err, ErrClaimFenceLost) {
			return ctrl.Result{}, r.projectNotReady(ctx, run, "ClaimFenceLost", "terminal run no longer owns its recorded business fence")
		}
		return ctrl.Result{}, fmt.Errorf("release terminal business claim: %w", err)
	}
	if _, err := r.Journal.AppendClaimReleased(run.Spec.RunID, *head.Claim); err != nil {
		return ctrl.Result{}, fmt.Errorf("commit claim.released: %w", err)
	}
	return r.requeueAfterJournalWrite(ctx, run)
}

// statusEqual compares projections ignoring condition timestamps, which are
// wall-clock noise and would otherwise cause an endless update loop.
func statusEqual(a, b apiv1.GooberRunStatus) bool {
	if a.ObservedGeneration != b.ObservedGeneration ||
		a.Phase != b.Phase ||
		a.State != b.State ||
		a.ObservedSeq != b.ObservedSeq ||
		a.TerminalStatus != b.TerminalStatus ||
		a.LastErrorCode != b.LastErrorCode {
		return false
	}
	if (a.Pause == nil) != (b.Pause == nil) {
		return false
	}
	if a.Pause != nil && *a.Pause != *b.Pause {
		return false
	}
	if len(a.Attempts) != len(b.Attempts) {
		return false
	}
	for i := range a.Attempts {
		if a.Attempts[i] != b.Attempts[i] {
			return false
		}
	}
	if len(a.Conditions) != len(b.Conditions) {
		return false
	}
	for i := range a.Conditions {
		x, y := a.Conditions[i], b.Conditions[i]
		if x.Type != y.Type || x.Status != y.Status || x.Reason != y.Reason ||
			x.Message != y.Message || x.ObservedGeneration != y.ObservedGeneration {
			return false
		}
	}
	return true
}

// jobSucceeded reports a Complete condition.
func jobSucceeded(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return job.Status.Succeeded > 0
}

// jobFailed reports a Failed condition.
func jobFailed(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// SetupWithManager registers the reconciler.
func (r *RunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&apiv1.GooberRun{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}
