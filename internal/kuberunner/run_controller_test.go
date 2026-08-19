package kuberunner

import (
	"context"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	apiv1 "github.com/goobers/goobers/api/v1alpha1"
	"github.com/goobers/goobers/internal/journal"
)

// newTestScheme registers exactly the types these tests exercise. It is built
// locally rather than borrowed from internal/operator because that package
// imports this one, so reusing its scheme would create an import cycle.
func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := batchv1.AddToScheme(s); err != nil {
		t.Fatalf("add batch scheme: %v", err)
	}
	s.AddKnownTypes(apiv1.GroupVersion,
		&apiv1.GooberRun{}, &apiv1.GooberRunList{},
		&apiv1.GooberRunAction{}, &apiv1.GooberRunActionList{},
	)
	metav1.AddToGroupVersion(s, apiv1.GroupVersion)
	return s
}

const (
	testNamespace = "goobers-system"
	testRunName   = "run-object"
	testRunID     = "run-1"
	testRunUID    = "uid-1"
)

type harness struct {
	t          *testing.T
	reconciler *RunReconciler
	client     client.Client
	journal    *fakeJournal
	results    *fakeResults
	machine    *testMachine
}

func newHarness(t *testing.T) *harness {
	t.Helper()

	scheme := newTestScheme(t)

	run := &apiv1.GooberRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:       testRunName,
			Namespace:  testNamespace,
			UID:        types.UID(testRunUID),
			Generation: 1,
		},
		Spec: apiv1.GooberRunSpec{
			RunID:       testRunID,
			Gaggle:      "web",
			JournalRoot: "/journal",
			Workflow: apiv1.PinnedWorkflow{
				Name:    "ship",
				Version: 1,
				Digest:  "sha256:" + "aa" + "00000000000000000000000000000000000000000000000000000000000000"[:62],
			},
			Trigger: apiv1.RunTrigger{Kind: "manual"},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(run).
		WithStatusSubresource(&apiv1.GooberRun{}, &apiv1.GooberRunAction{}).
		Build()

	j := newFakeJournal()
	j.seed(testRunID)
	results := newFakeResults()
	machine := newTestMachine()

	return &harness{
		t:       t,
		client:  c,
		journal: j,
		results: results,
		machine: machine,
		reconciler: &RunReconciler{
			Client:  c,
			Scheme:  scheme,
			Journal: j,
			Results: results,
			Machine: StaticMachineResolver{Machine: machine},
		},
	}
}

func (h *harness) reconcile() ctrl.Result {
	h.t.Helper()
	res, err := h.reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testRunName},
	})
	if err != nil {
		h.t.Fatalf("reconcile: %v", err)
	}
	return res
}

// reconcileUntilStable drives reconcile to a fixed point, which is how the
// controller actually behaves under a requeue.
func (h *harness) reconcileUntilStable(max int) {
	h.t.Helper()
	for i := 0; i < max; i++ {
		if res := h.reconcile(); !res.Requeue {
			return
		}
	}
	h.t.Fatalf("reconcile did not stabilise within %d iterations", max)
}

func (h *harness) run() *apiv1.GooberRun {
	h.t.Helper()
	var run apiv1.GooberRun
	key := types.NamespacedName{Namespace: testNamespace, Name: testRunName}
	if err := h.client.Get(context.Background(), key, &run); err != nil {
		h.t.Fatalf("get run: %v", err)
	}
	return &run
}

func (h *harness) jobs() []batchv1.Job {
	h.t.Helper()
	var list batchv1.JobList
	if err := h.client.List(context.Background(), &list, client.InNamespace(testNamespace)); err != nil {
		h.t.Fatalf("list jobs: %v", err)
	}
	return list.Items
}

func (h *harness) attempt(state string, n int) AttemptID {
	return AttemptID{RunUID: testRunUID, RunID: testRunID, State: state, Attempt: n}
}

// completeJob marks a Job Complete, which is EVIDENCE only.
func (h *harness) completeJob(name string) {
	h.t.Helper()
	h.setJobCondition(name, batchv1.JobComplete)
}

func (h *harness) failJob(name string) {
	h.t.Helper()
	h.setJobCondition(name, batchv1.JobFailed)
}

func (h *harness) setJobCondition(name string, condType batchv1.JobConditionType) {
	h.t.Helper()
	var job batchv1.Job
	key := types.NamespacedName{Namespace: testNamespace, Name: name}
	if err := h.client.Get(context.Background(), key, &job); err != nil {
		h.t.Fatalf("get job %s: %v", name, err)
	}
	job.Status.Conditions = append(job.Status.Conditions, batchv1.JobCondition{
		Type:   condType,
		Status: corev1.ConditionTrue,
	})
	if condType == batchv1.JobComplete {
		job.Status.Succeeded = 1
	}
	if err := h.client.Status().Update(context.Background(), &job); err != nil {
		h.t.Fatalf("update job status: %v", err)
	}
}

// --- dispatch -------------------------------------------------------------

func TestReconcileCreatesOneJobPerAttempt(t *testing.T) {
	h := newHarness(t)
	h.reconcile()

	jobs := h.jobs()
	if len(jobs) != 1 {
		t.Fatalf("got %d jobs, want 1", len(jobs))
	}
	want := JobName(h.attempt("build", 1))
	if jobs[0].Name != want {
		t.Errorf("job name = %q, want the deterministic %q", jobs[0].Name, want)
	}

	// Dispatch intent must be journalled BEFORE the Job exists, so a crash in
	// between is recoverable.
	if got := h.journal.types(testRunID); len(got) != 2 || got[1] != journal.EventStageStarted {
		t.Fatalf("journal = %v, want run.started then stage.started", got)
	}
}

// backoffLimit 0 is a correctness requirement, not a tuning choice: any
// Kubernetes-initiated restart would be a second execution sharing one attempt
// identity, able to publish to the same result slot as the first.
func TestAttemptJobsDisableKubernetesRetry(t *testing.T) {
	h := newHarness(t)
	h.reconcile()

	job := h.jobs()[0]
	if job.Spec.BackoffLimit == nil {
		t.Fatal("backoffLimit is unset; Kubernetes would apply its default retry")
	}
	if *job.Spec.BackoffLimit != 0 {
		t.Errorf("backoffLimit = %d, want 0 (retry belongs to the controller)", *job.Spec.BackoffLimit)
	}
	if got := job.Spec.Template.Spec.RestartPolicy; got != corev1.RestartPolicyNever {
		t.Errorf("restartPolicy = %q, want %q", got, corev1.RestartPolicyNever)
	}
}

// Create-once. Repeated reconciles, duplicate watch events, and a controller
// restart must all converge on exactly one Job per attempt.
func TestRepeatedReconcileCreatesJobOnce(t *testing.T) {
	h := newHarness(t)

	for i := 0; i < 8; i++ {
		h.reconcile()
	}

	jobs := h.jobs()
	if len(jobs) != 1 {
		t.Fatalf("got %d jobs after 8 reconciles, want exactly 1", len(jobs))
	}

	// And the journal must not have grown a stage.started per reconcile.
	started := 0
	for _, ev := range h.journal.snapshot(testRunID) {
		if ev.Type == journal.EventStageStarted {
			started++
		}
	}
	if started != 1 {
		t.Fatalf("journal has %d stage.started events, want 1", started)
	}
}

// The crash window: intent is journalled, then the process dies before the Job
// is created. The next reconcile must create the Job under the SAME name and
// must NOT open a second attempt.
func TestReconcileRecreatesJobAfterCrashBetweenAppendAndCreate(t *testing.T) {
	h := newHarness(t)

	// Simulate the crash: journal the intent by hand, create no Job.
	attempt := h.attempt("build", 1)
	if _, err := h.journal.AppendStageStarted(testRunID, attempt); err != nil {
		t.Fatalf("seed stage.started: %v", err)
	}
	if len(h.jobs()) != 0 {
		t.Fatal("precondition: no job should exist yet")
	}

	h.reconcile()

	jobs := h.jobs()
	if len(jobs) != 1 {
		t.Fatalf("got %d jobs, want 1 recreated under the deterministic name", len(jobs))
	}
	if jobs[0].Name != JobName(attempt) {
		t.Errorf("job name = %q, want %q", jobs[0].Name, JobName(attempt))
	}

	started := 0
	for _, ev := range h.journal.snapshot(testRunID) {
		if ev.Type == journal.EventStageStarted {
			started++
		}
	}
	if started != 1 {
		t.Fatalf("recovery opened %d attempts, want 1", started)
	}
}

func TestCreateAttemptJobRefusesForeignDeterministicNameCollision(t *testing.T) {
	h := newHarness(t)
	attempt := h.attempt("build", 1)
	foreign := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: JobName(attempt), Namespace: testNamespace},
		Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers:    []corev1.Container{{Name: "attempt", Image: "foreign/image"}},
		}}},
	}
	if err := h.client.Create(context.Background(), foreign); err != nil {
		t.Fatalf("create colliding Job: %v", err)
	}
	if _, err := h.reconciler.createAttemptJob(context.Background(), h.run(), attempt, h.machine); err != nil {
		t.Fatalf("conflict projection: %v", err)
	}
	if !hasCondition(h.run().Status.Conditions, apiv1.GooberRunConditionReady, metav1.ConditionFalse, "AttemptJobConflict") {
		t.Fatalf("missing AttemptJobConflict condition: %+v", h.run().Status.Conditions)
	}
	jobs := h.jobs()
	if len(jobs) != 1 || jobs[0].Spec.Template.Spec.Containers[0].Image != "foreign/image" {
		t.Fatalf("controller overwrote foreign Job: %+v", jobs)
	}
}

// --- results advance runs, Jobs do not ------------------------------------

func TestValidResultCommitsStageThenRunFinished(t *testing.T) {
	h := newHarness(t)
	h.reconcile()

	attempt := h.attempt("build", 1)
	h.completeJob(JobName(attempt))
	h.results.publish(t, attempt, apiv1.ResultEnvelope{
		Status:  apiv1.ResultSuccess,
		Summary: "built",
		Artifacts: []apiv1.ArtifactPointer{{
			Path:   "artifacts/build/out.tar",
			Digest: apiv1.Digest([]byte("payload")),
		}},
	})

	h.reconcileUntilStable(10)

	// Ordering is the contract: a stage settles before the run does.
	want := []journal.EventType{
		journal.EventRunStarted,
		journal.EventStageStarted,
		journal.EventStageFinished,
		journal.EventRunFinished,
	}
	got := h.journal.types(testRunID)
	if len(got) != len(want) {
		t.Fatalf("journal = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("journal = %v, want %v", got, want)
		}
	}

	run := h.run()
	if run.Status.Phase != apiv1.GooberRunCompleted {
		t.Errorf("phase = %q, want %q", run.Status.Phase, apiv1.GooberRunCompleted)
	}
	if run.Status.TerminalStatus != string(journal.PhaseCompleted) {
		t.Errorf("terminalStatus = %q, want %q", run.Status.TerminalStatus, journal.PhaseCompleted)
	}
}

// The load-bearing negative: a Job that SUCCEEDED but published nothing usable
// must not advance the run. "The process exited 0" is not the same claim as
// "the stage decided this".
func TestJobSuccessWithoutValidResultDoesNotAdvance(t *testing.T) {
	for _, tc := range []struct {
		name    string
		publish func(*harness, AttemptID)
	}{
		{
			name:    "no result published at all",
			publish: func(*harness, AttemptID) {},
		},
		{
			name: "result bytes are garbage",
			publish: func(h *harness, a AttemptID) {
				h.results.publishRaw(a, []byte("{not a receipt"))
			},
		},
		{
			name: "result belongs to a different attempt",
			publish: func(h *harness, a AttemptID) {
				stale := a
				stale.Attempt = 99
				raw, err := EncodeReceipt(stale, apiv1.ResultEnvelope{Status: apiv1.ResultSuccess})
				if err != nil {
					h.t.Fatalf("encode: %v", err)
				}
				// Publish it at the CURRENT attempt's slot: a stale worker
				// writing into the live location.
				h.results.publishRaw(a, raw)
			},
		},
		{
			name: "digest does not match the envelope",
			publish: func(h *harness, a AttemptID) {
				raw, err := EncodeReceipt(a, apiv1.ResultEnvelope{Status: apiv1.ResultSuccess})
				if err != nil {
					h.t.Fatalf("encode: %v", err)
				}
				tampered := append([]byte(nil), raw...)
				// Corrupt the envelope body while leaving the digest claiming
				// the original bytes.
				for i := 0; i+7 < len(tampered); i++ {
					if string(tampered[i:i+7]) == "success" {
						copy(tampered[i:i+7], "failure")
						break
					}
				}
				h.results.publishRaw(a, tampered)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			h.reconcile()

			attempt := h.attempt("build", 1)
			h.completeJob(JobName(attempt))
			tc.publish(h, attempt)

			before := h.journal.types(testRunID)
			h.reconcile()
			after := h.journal.types(testRunID)

			if len(after) != len(before) {
				t.Fatalf("journal advanced on an unusable result: %v -> %v", before, after)
			}
			for _, ev := range after {
				if ev == journal.EventRunFinished || ev == journal.EventStageFinished {
					t.Fatalf("journal contains %q despite an unusable result: %v", ev, after)
				}
			}

			run := h.run()
			if run.Status.Phase.IsTerminal() {
				t.Fatalf("status projected terminal phase %q on an unusable result", run.Status.Phase)
			}
		})
	}
}

// A validated result advances the run even if the Job's own status has not (or
// will never) report Complete. The result is the authority; the Job is not.
func TestValidResultAdvancesWithoutJobCompletion(t *testing.T) {
	h := newHarness(t)
	h.reconcile()

	attempt := h.attempt("build", 1)
	// Note: the Job status is left entirely untouched.
	h.results.publish(t, attempt, apiv1.ResultEnvelope{Status: apiv1.ResultSuccess})

	h.reconcileUntilStable(10)

	if h.run().Status.Phase != apiv1.GooberRunCompleted {
		t.Fatalf("phase = %q, want %q", h.run().Status.Phase, apiv1.GooberRunCompleted)
	}
}

// --- human gates ----------------------------------------------------------

func TestHumanGateCreatesNoJobAndProjectsWaiting(t *testing.T) {
	h := newHarness(t)
	h.machine.start = "approve"
	h.machine.kinds["approve"] = StateHumanGate

	h.reconcileUntilStable(10)

	if jobs := h.jobs(); len(jobs) != 0 {
		t.Fatalf("created %d jobs for a human gate; a human gate executes no process", len(jobs))
	}

	run := h.run()
	if run.Status.Phase != apiv1.GooberRunWaiting {
		t.Fatalf("phase = %q, want %q", run.Status.Phase, apiv1.GooberRunWaiting)
	}
	if run.Status.Pause == nil {
		t.Fatal("waiting run projects no pause occurrence; a human action would have nothing to quote")
	}
	if run.Status.Pause.Gate != "approve" {
		t.Errorf("pause gate = %q, want %q", run.Status.Pause.Gate, "approve")
	}
	if run.Status.Pause.Seq == 0 {
		t.Error("pause occurrence has no sequence")
	}

	// Parking must be idempotent: repeated reconciles must not append a
	// gate.paused per pass.
	paused := 0
	for _, ev := range h.journal.snapshot(testRunID) {
		if ev.Type == journal.EventGatePaused {
			paused++
		}
	}
	if paused != 1 {
		t.Fatalf("journal has %d gate.paused events, want 1", paused)
	}
}

// --- retry ownership ------------------------------------------------------

func TestFailedJobRetryIsControllerOwnedAndSeparatelyJournalled(t *testing.T) {
	h := newHarness(t)
	h.machine.attempts["build"] = 2 // allow one retry

	h.reconcile()
	first := h.attempt("build", 1)
	h.failJob(JobName(first))

	h.reconcile() // journals the failed attempt
	h.reconcile() // dispatches the retry

	jobs := h.jobs()
	if len(jobs) != 2 {
		t.Fatalf("got %d jobs, want 2 (each retry is its own Job)", len(jobs))
	}

	second := h.attempt("build", 2)
	names := map[string]bool{}
	for _, job := range jobs {
		names[job.Name] = true
	}
	if !names[JobName(first)] || !names[JobName(second)] {
		t.Fatalf("jobs %v do not match the two deterministic attempt names", names)
	}

	// Each attempt must be separately visible in the journal.
	starts := 0
	for _, ev := range h.journal.snapshot(testRunID) {
		if ev.Type == journal.EventStageStarted {
			starts++
		}
	}
	if starts != 2 {
		t.Fatalf("journal has %d stage.started events, want 2 distinct attempts", starts)
	}
}

// Attempt numbering is journal authority. Losing the entire status projection
// between attempts must not reuse attempt 1 or collide with its Job.
func TestAttemptNumberComesFromJournalAfterStatusLoss(t *testing.T) {
	h := newHarness(t)
	h.machine.attempts["build"] = 2
	h.reconcile()
	first := h.attempt("build", 1)
	h.failJob(JobName(first))
	h.reconcile() // commits attempt 1 stage.finished

	run := h.run()
	run.Status = apiv1.GooberRunStatus{}
	if err := h.client.Status().Update(context.Background(), run); err != nil {
		t.Fatalf("delete status projection: %v", err)
	}

	h.reconcile()
	second := h.attempt("build", 2)
	names := map[string]bool{}
	for _, job := range h.jobs() {
		names[job.Name] = true
	}
	if !names[JobName(first)] || !names[JobName(second)] {
		t.Fatalf("jobs after status loss = %v, want durable attempts 1 and 2", names)
	}
	if got := h.run().Status.Attempts; len(got) != 2 || got[0].DispatchedSeq == 0 || got[1].DispatchedSeq == 0 {
		t.Fatalf("attempt projection was not rebuilt from stage.started events: %+v", got)
	}
}

func TestClaimRecoveryAndStaleWorkerReceiptAreFenced(t *testing.T) {
	h := newHarness(t)
	now := time.Date(2026, 8, 19, 14, 0, 0, 0, time.UTC)
	claims := &KubeClaimStore{Client: h.client, Now: func() time.Time { return now }}
	run := h.run()
	run.Spec.Repository = &apiv1.RepositoryIdentity{Provider: "github", ExternalID: "42"}
	// The fake API does not enforce spec immutability, which lets this focused
	// harness opt into business claims after construction.
	if err := h.client.Update(context.Background(), run); err != nil {
		t.Fatalf("add repository identity: %v", err)
	}
	h.reconciler.Claims = claims
	h.reconciler.ClaimTTL = time.Minute

	h.reconcile()                                    // acquire + journal claim.acquired
	if res := h.reconcile(); res.RequeueAfter <= 0 { // recover journal fence and dispatch
		t.Fatal("claimed quiet Job has no periodic renewal requeue")
	}
	head, err := h.journal.Head(testRunID)
	if err != nil || head.Claim == nil || head.Claim.Epoch != 1 {
		t.Fatalf("journal claim after dispatch = %+v, err=%v", head.Claim, err)
	}
	oldAttempt := h.attempt("build", 1)
	oldAttempt.FenceEpoch = 1
	if jobs := h.jobs(); len(jobs) != 1 {
		t.Fatalf("jobs = %d, want 1", len(jobs))
	}

	// Simulate controller restart: a fresh reconciler has no process-local
	// ownership state, yet renews epoch 1 from the journal and creates nothing.
	restarted := *h.reconciler
	h.reconciler = &restarted
	h.reconcile()
	if jobs := h.jobs(); len(jobs) != 1 {
		t.Fatalf("restart created %d jobs, want exactly 1", len(jobs))
	}
	acquired := 0
	for _, ev := range h.journal.snapshot(testRunID) {
		if ev.Type == journal.EventClaimAcquired {
			acquired++
		}
	}
	if acquired != 1 {
		t.Fatalf("restart journalled %d claim acquisitions, want 1", acquired)
	}

	// Let epoch 1 expire and grant epoch 2 to another run. The stale worker can
	// still publish a correctly encoded epoch-1 receipt, but acceptance must
	// fail before stage.finished is committed.
	now = now.Add(2 * time.Minute)
	key := ClaimKey{Gaggle: "web", Provider: "github", ExternalID: "42"}
	if token, err := claims.Acquire(context.Background(), testNamespace, key, "run-2", "uid-2", time.Minute); err != nil || token.Epoch != 2 {
		t.Fatalf("take over claim: token=%+v err=%v", token, err)
	}
	h.results.publish(t, oldAttempt, apiv1.ResultEnvelope{Status: apiv1.ResultSuccess})
	before := len(h.journal.snapshot(testRunID))
	h.reconcile()
	after := h.journal.snapshot(testRunID)
	if len(after) != before {
		t.Fatalf("stale worker advanced journal: %d -> %d events", before, len(after))
	}
	for _, ev := range after {
		if ev.Type == journal.EventStageFinished {
			t.Fatal("stale epoch receipt committed stage.finished")
		}
	}
	if !hasCondition(h.run().Status.Conditions, apiv1.GooberRunConditionReady, metav1.ConditionFalse, "ClaimFenceLost") {
		t.Fatalf("missing ClaimFenceLost condition: %+v", h.run().Status.Conditions)
	}
}

func TestTerminalRunCommitsBeforeBusinessClaimRelease(t *testing.T) {
	h := newHarness(t)
	now := time.Date(2026, 8, 19, 14, 0, 0, 0, time.UTC)
	claims := &KubeClaimStore{Client: h.client, Now: func() time.Time { return now }}
	run := h.run()
	run.Spec.Repository = &apiv1.RepositoryIdentity{Provider: "github", ExternalID: "42"}
	if err := h.client.Update(context.Background(), run); err != nil {
		t.Fatalf("add repository identity: %v", err)
	}
	h.reconciler.Claims, h.reconciler.ClaimTTL = claims, time.Minute
	h.reconcile() // claim
	h.reconcile() // dispatch
	attempt := h.attempt("build", 1)
	attempt.FenceEpoch = 1
	h.results.publish(t, attempt, apiv1.ResultEnvelope{Status: apiv1.ResultSuccess})
	h.reconcileUntilStable(10)

	types := h.journal.types(testRunID)
	runFinished, claimReleased := -1, -1
	for i, eventType := range types {
		if eventType == journal.EventRunFinished {
			runFinished = i
		}
		if eventType == journal.EventClaimReleased {
			claimReleased = i
		}
	}
	if runFinished < 0 || claimReleased < 0 || runFinished >= claimReleased {
		t.Fatalf("terminal claim ordering = %v, want run.finished before claim.released", types)
	}
	key := ClaimKey{Gaggle: "web", Provider: "github", ExternalID: "42"}
	if token, err := claims.Acquire(context.Background(), testNamespace, key, "run-2", "uid-2", time.Minute); err != nil || token.Epoch != 2 {
		t.Fatalf("claim after terminal release: token=%+v err=%v", token, err)
	}
}

func TestExhaustedRetriesTerminalizeTheRun(t *testing.T) {
	h := newHarness(t)
	// Default MaxAttempts is 1: the first failure exhausts the budget.
	h.reconcile()
	h.failJob(JobName(h.attempt("build", 1)))
	h.reconcileUntilStable(10)

	run := h.run()
	if run.Status.Phase != apiv1.GooberRunAborted {
		t.Fatalf("phase = %q, want %q", run.Status.Phase, apiv1.GooberRunAborted)
	}

	// stage.finished must still precede run.finished, even on the failure path.
	types := h.journal.types(testRunID)
	stageIdx, runIdx := -1, -1
	for i, ev := range types {
		if ev == journal.EventStageFinished && stageIdx < 0 {
			stageIdx = i
		}
		if ev == journal.EventRunFinished && runIdx < 0 {
			runIdx = i
		}
	}
	if stageIdx < 0 || runIdx < 0 || stageIdx > runIdx {
		t.Fatalf("journal %v does not settle the stage before the run", types)
	}
}

// --- status is a projection ----------------------------------------------

// The repair property: status is derived, so deleting it wholesale and
// reconciling must reproduce it EXACTLY. If this ever fails, some fact is
// living only in status, which would make status an authority.
func TestStatusRebuildsExactlyFromTheJournal(t *testing.T) {
	h := newHarness(t)
	h.reconcile()

	attempt := h.attempt("build", 1)
	h.results.publish(t, attempt, apiv1.ResultEnvelope{Status: apiv1.ResultSuccess})
	h.reconcileUntilStable(10)

	before := h.run().Status.DeepCopy()

	// Wipe the projection entirely, as an operator or a restore might.
	run := h.run()
	run.Status = apiv1.GooberRunStatus{}
	if err := h.client.Status().Update(context.Background(), run); err != nil {
		t.Fatalf("wipe status: %v", err)
	}
	if h.run().Status.Phase != "" {
		t.Fatal("precondition: status was not actually wiped")
	}

	appendsBefore := h.journal.appends
	h.reconcileUntilStable(10)
	if h.journal.appends != appendsBefore {
		t.Fatalf("rebuilding status wrote %d journal events; projection must not mutate authority",
			h.journal.appends-appendsBefore)
	}

	after := h.run().Status
	if !statusEqual(after, *before) {
		t.Errorf("status after journal rebuild differs from original\nafter:  %+v\nbefore: %+v", after, *before)
	}
	if after.Phase != before.Phase {
		t.Errorf("phase after rebuild = %q, want %q", after.Phase, before.Phase)
	}
	if after.ObservedSeq != before.ObservedSeq {
		t.Errorf("observedSeq after rebuild = %d, want %d", after.ObservedSeq, before.ObservedSeq)
	}
	if after.State != before.State {
		t.Errorf("state after rebuild = %q, want %q", after.State, before.State)
	}
	if after.TerminalStatus != before.TerminalStatus {
		t.Errorf("terminalStatus after rebuild = %q, want %q", after.TerminalStatus, before.TerminalStatus)
	}
}

// ProjectStatus is pure, so the same head always yields the same projection.
func TestProjectStatusIsPure(t *testing.T) {
	head := JournalHead{
		Seq:            7,
		Phase:          journal.PhaseCompleted,
		State:          "@complete",
		TerminalStatus: "completed",
	}
	first := ProjectStatus(head, 3, testRunUID, testRunID)
	second := ProjectStatus(head, 3, testRunUID, testRunID)

	if !statusEqual(first, second) {
		t.Fatal("ProjectStatus is not a pure function of its inputs")
	}
	if first.Phase != apiv1.GooberRunCompleted {
		t.Errorf("phase = %q, want %q", first.Phase, apiv1.GooberRunCompleted)
	}
}

func TestProjectPhaseMapsJournalPhases(t *testing.T) {
	for _, tc := range []struct {
		head JournalHead
		want apiv1.GooberRunPhase
	}{
		{JournalHead{Phase: journal.PhaseCompleted}, apiv1.GooberRunCompleted},
		{JournalHead{Phase: journal.PhaseFailed}, apiv1.GooberRunFailed},
		{JournalHead{Phase: journal.PhaseAborted}, apiv1.GooberRunAborted},
		{JournalHead{Phase: journal.PhaseEscalated}, apiv1.GooberRunEscalated},
		{JournalHead{Phase: journal.PhaseRunning, Seq: 2}, apiv1.GooberRunRunning},
		{JournalHead{Phase: journal.PhaseRunning, Seq: 0}, apiv1.GooberRunPending},
		// Waiting is an operational refinement of running, never a terminal
		// claim: a parked run has NOT finished.
		{JournalHead{Phase: journal.PhaseRunning, Seq: 3, Wait: WaitHumanGate}, apiv1.GooberRunWaiting},
	} {
		if got := ProjectPhase(tc.head); got != tc.want {
			t.Errorf("ProjectPhase(%+v) = %q, want %q", tc.head, got, tc.want)
		}
	}
}

// A terminal run is done: no further Jobs, ever.
func TestTerminalRunCreatesNoJob(t *testing.T) {
	h := newHarness(t)
	if _, err := h.journal.AppendRunFinished(testRunID, journal.PhaseCompleted, journal.TargetComplete); err != nil {
		t.Fatalf("seed terminal: %v", err)
	}

	h.reconcileUntilStable(5)

	if jobs := h.jobs(); len(jobs) != 0 {
		t.Fatalf("created %d jobs for a terminal run", len(jobs))
	}
	if got := h.run().Status.Phase; got != apiv1.GooberRunCompleted {
		t.Fatalf("phase = %q, want %q", got, apiv1.GooberRunCompleted)
	}
}

// A CR whose journal does not exist must NOT be repaired by inventing one:
// that would make the CR the authority.
func TestMissingJournalDoesNotFabricateState(t *testing.T) {
	h := newHarness(t)
	delete(h.journal.events, testRunID)

	h.reconcile()

	if jobs := h.jobs(); len(jobs) != 0 {
		t.Fatalf("created %d jobs for a run with no journal", len(jobs))
	}
	run := h.run()
	if run.Status.Phase.IsTerminal() {
		t.Fatalf("projected terminal phase %q with no journal", run.Status.Phase)
	}
	if !hasCondition(run.Status.Conditions, apiv1.GooberRunConditionReady, metav1.ConditionFalse, "JournalMissing") {
		t.Errorf("expected a Ready=False/JournalMissing condition, got %+v", run.Status.Conditions)
	}
}

func TestOwnerReferenceIsSetOnAttemptJobs(t *testing.T) {
	h := newHarness(t)
	h.reconcile()

	job := h.jobs()[0]
	if len(job.OwnerReferences) != 1 {
		t.Fatalf("got %d owner references, want 1", len(job.OwnerReferences))
	}
	owner := job.OwnerReferences[0]
	if owner.Kind != "GooberRun" || string(owner.UID) != testRunUID {
		t.Errorf("owner = %s/%s, want GooberRun/%s", owner.Kind, owner.UID, testRunUID)
	}
}

func TestDuplicateJobObservationIsIdempotent(t *testing.T) {
	h := newHarness(t)
	h.reconcile()

	attempt := h.attempt("build", 1)
	name := JobName(attempt)

	// Simulate a duplicate create racing in from another replica.
	_, err := h.reconciler.createAttemptJob(context.Background(), h.run(), attempt, h.machine)
	if err != nil {
		t.Fatalf("duplicate create returned an error instead of treating AlreadyExists as success: %v", err)
	}

	if jobs := h.jobs(); len(jobs) != 1 {
		t.Fatalf("got %d jobs after a duplicate create, want 1", len(jobs))
	}

	var job batchv1.Job
	key := types.NamespacedName{Namespace: testNamespace, Name: name}
	if err := h.client.Get(context.Background(), key, &job); err != nil {
		if apierrors.IsNotFound(err) {
			t.Fatalf("the deterministic job %q vanished", name)
		}
		t.Fatalf("get job: %v", err)
	}
}

func hasCondition(conds []metav1.Condition, condType string, status metav1.ConditionStatus, reason string) bool {
	for _, c := range conds {
		if c.Type == condType && c.Status == status && c.Reason == reason {
			return true
		}
	}
	return false
}
