package kuberunner

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	apiv1 "github.com/goobers/goobers/api/v1alpha1"
	"github.com/goobers/goobers/internal/journal"
)

// These exercise the PRODUCTION on-disk implementations against a real journal,
// so the in-memory doubles used by the reconciler tests cannot quietly diverge
// from the behaviour they stand in for.

func newRealJournal(t *testing.T) (*FSJournalStore, string) {
	t.Helper()
	runsDir := t.TempDir()

	run, err := journal.Create(runsDir, journal.RunIdentity{
		RunID:           "run-real",
		Workflow:        "ship",
		WorkflowVersion: 1,
		Gaggle:          "web",
		Trigger:         journal.Trigger{Kind: journal.TriggerManual},
	}, nil)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := run.Close(); err != nil {
		t.Fatalf("close run: %v", err)
	}
	return NewFSJournalStore(runsDir), "run-real"
}

func TestFSJournalStoreHeadTracksAppends(t *testing.T) {
	store, runID := newRealJournal(t)

	head, err := store.Head(runID)
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if head.Phase != journal.PhaseRunning {
		t.Fatalf("phase = %q, want %q", head.Phase, journal.PhaseRunning)
	}
	if head.Identity.RunID != runID {
		t.Errorf("identity run id = %q, want %q", head.Identity.RunID, runID)
	}

	attempt := AttemptID{RunUID: "uid-1", RunID: runID, State: "build", Attempt: 1}
	if _, err := store.AppendStageStarted(runID, attempt); err != nil {
		t.Fatalf("append stage.started: %v", err)
	}

	head, err = store.Head(runID)
	if err != nil {
		t.Fatalf("head after start: %v", err)
	}
	if !head.HasOpenAttempt() {
		t.Fatal("head does not report the open attempt")
	}
	if head.OpenStage != "build" || head.OpenAttempt != 1 {
		t.Errorf("open attempt = %s/%d, want build/1", head.OpenStage, head.OpenAttempt)
	}

	// Committing a validated result must close the attempt.
	envelope := apiv1.ResultEnvelope{Status: apiv1.ResultSuccess, Summary: "done"}
	raw, err := EncodeReceipt(attempt, envelope)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	result, err := ValidateResult(attempt, raw)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if _, err := store.AppendStageFinished(runID, attempt, result); err != nil {
		t.Fatalf("append stage.finished: %v", err)
	}

	head, err = store.Head(runID)
	if err != nil {
		t.Fatalf("head after finish: %v", err)
	}
	if head.HasOpenAttempt() {
		t.Fatalf("attempt %s/%d is still open after stage.finished", head.OpenStage, head.OpenAttempt)
	}

	if _, err := store.AppendRunFinished(runID, journal.PhaseCompleted, journal.TargetComplete); err != nil {
		t.Fatalf("append run.finished: %v", err)
	}
	head, err = store.Head(runID)
	if err != nil {
		t.Fatalf("head after terminal: %v", err)
	}
	if !head.IsTerminal() {
		t.Fatalf("phase = %q, want a terminal phase", head.Phase)
	}
	if head.TerminalStatus != string(journal.PhaseCompleted) {
		t.Errorf("terminalStatus = %q, want %q", head.TerminalStatus, journal.PhaseCompleted)
	}
}

func TestFSJournalStoreRejectsUnknownAndUnsafeRuns(t *testing.T) {
	store, _ := newRealJournal(t)

	if _, err := store.Head("no-such-run"); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("unknown run error = %v, want %v", err, ErrRunNotFound)
	}
	// A run id from a CR spec is operator input, so traversal is a real
	// boundary rather than a formality.
	for _, bad := range []string{"../escape", "..", "."} {
		if _, err := store.Head(bad); err == nil {
			t.Errorf("accepted an unsafe run id %q", bad)
		}
	}
}

func TestFSJournalStoreRecoversClaimFenceAndAttemptHighWater(t *testing.T) {
	store, runID := newRealJournal(t)
	token := ClaimToken{Key: ClaimKey{Gaggle: "web", Provider: "github", ExternalID: "42"}, RunID: runID, RunUID: "uid-1", Epoch: 7}
	if _, err := store.AppendClaimAcquired(runID, token); err != nil {
		t.Fatalf("append claim.acquired: %v", err)
	}
	for i := 1; i <= 2; i++ {
		if _, err := store.AppendStageStarted(runID, AttemptID{RunUID: "uid-1", RunID: runID, State: "build", Attempt: i, FenceEpoch: 7}); err != nil {
			t.Fatalf("append attempt %d: %v", i, err)
		}
	}

	// A new store instance simulates process restart and must recover both
	// authority facts from bytes, without status or process memory.
	restarted := NewFSJournalStore(store.RunsDir)
	head, err := restarted.Head(runID)
	if err != nil {
		t.Fatalf("restart head: %v", err)
	}
	if head.Claim == nil || !head.Claim.Equal(token) {
		t.Fatalf("recovered claim = %+v, want %+v", head.Claim, token)
	}
	if got := head.NextAttempt("build", 0); got != 3 {
		t.Fatalf("next attempt = %d, want 3 from journal high-water", got)
	}
	if _, err := restarted.AppendClaimReleased(runID, token); err != nil {
		t.Fatalf("append claim.released: %v", err)
	}
	head, err = restarted.Head(runID)
	if err != nil {
		t.Fatalf("head after release: %v", err)
	}
	if head.Claim != nil {
		t.Fatalf("released claim still active in head: %+v", head.Claim)
	}
}

// HeadFromEvents is the shared derivation used by both the production store and
// the reconciler tests, so its rules are pinned directly.
func TestHeadFromEventsDerivation(t *testing.T) {
	t.Run("gate pause then evaluation clears the wait", func(t *testing.T) {
		head := HeadFromEvents([]journal.Event{
			{Seq: 1, Type: journal.EventRunStarted, Status: "running"},
			{Seq: 2, Type: journal.EventGatePaused, Gate: "approve"},
		})
		if head.Wait != WaitHumanGate || head.WaitSeq != 2 {
			t.Fatalf("wait = %q at seq %d, want human-gate at seq 2", head.Wait, head.WaitSeq)
		}

		head = HeadFromEvents([]journal.Event{
			{Seq: 1, Type: journal.EventRunStarted, Status: "running"},
			{Seq: 2, Type: journal.EventGatePaused, Gate: "approve"},
			{Seq: 3, Type: journal.EventGateEvaluated, Gate: "approve", Target: "deploy"},
		})
		if head.Wait != WaitNone {
			t.Fatalf("wait = %q after evaluation, want none", head.Wait)
		}
		if head.State != "deploy" {
			t.Errorf("state = %q, want %q", head.State, "deploy")
		}
	})

	t.Run("a stale stage.finished does not close a newer attempt", func(t *testing.T) {
		// A late duplicate for attempt 1 arriving after attempt 2 opened must
		// not clear the in-flight attempt, or the controller would dispatch a
		// third Job for work already running.
		head := HeadFromEvents([]journal.Event{
			{Seq: 1, Type: journal.EventRunStarted, Status: "running"},
			{Seq: 2, Type: journal.EventStageStarted, Stage: "build", Attempt: 1},
			{Seq: 3, Type: journal.EventStageFinished, Stage: "build", Attempt: 1, Status: "failure"},
			{Seq: 4, Type: journal.EventStageStarted, Stage: "build", Attempt: 2},
			{Seq: 5, Type: journal.EventStageFinished, Stage: "build", Attempt: 1, Status: "failure"},
		})
		if !head.HasOpenAttempt() || head.OpenAttempt != 2 {
			t.Fatalf("open attempt = %d (open=%v), want attempt 2 still open",
				head.OpenAttempt, head.HasOpenAttempt())
		}
	})

	t.Run("a new attempt clears a prior park", func(t *testing.T) {
		head := HeadFromEvents([]journal.Event{
			{Seq: 1, Type: journal.EventRunStarted, Status: "running"},
			{Seq: 2, Type: journal.EventGatePaused, Gate: "approve"},
			{Seq: 3, Type: journal.EventStageStarted, Stage: "build", Attempt: 1},
		})
		if head.Wait != WaitNone {
			t.Fatalf("wait = %q while executing, want none", head.Wait)
		}
	})

	t.Run("errors surface their code", func(t *testing.T) {
		head := HeadFromEvents([]journal.Event{
			{Seq: 1, Type: journal.EventRunStarted, Status: "running"},
			{Seq: 2, Type: journal.EventError, Error: &journal.ErrorDetail{Code: "provider_denied"}},
		})
		if head.LastErrorCode != "provider_denied" {
			t.Errorf("lastErrorCode = %q, want %q", head.LastErrorCode, "provider_denied")
		}
	})
}

// --- result transport -----------------------------------------------------

func TestFSResultReaderRoundTrip(t *testing.T) {
	root := t.TempDir()
	reader := NewFSResultReader(root)
	attempt := AttemptID{RunUID: "uid-1", RunID: "run-1", State: "build", Attempt: 1}

	if _, err := reader.ReadResult(attempt); !errors.Is(err, ErrResultMissing) {
		t.Fatalf("unpublished result error = %v, want %v", err, ErrResultMissing)
	}

	envelope := apiv1.ResultEnvelope{Status: apiv1.ResultSuccess, Summary: "built"}
	if err := reader.PublishResult(attempt, envelope); err != nil {
		t.Fatalf("publish: %v", err)
	}

	raw, err := reader.ReadResult(attempt)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	result, err := ValidateResult(attempt, raw)
	if err != nil {
		t.Fatalf("validate a receipt this package published: %v", err)
	}
	if result.Envelope.Summary != "built" {
		t.Errorf("summary = %q, want %q", result.Envelope.Summary, "built")
	}
}

// A half-written file must read as MISSING, not as a failure: treating a
// partial publish as a failed attempt would burn a retry on a transient.
func TestFSResultReaderTreatsAnEmptyFileAsMissing(t *testing.T) {
	root := t.TempDir()
	reader := NewFSResultReader(root)
	attempt := AttemptID{RunUID: "uid-1", RunID: "run-1", State: "build", Attempt: 1}

	path, err := reader.ResultPath(attempt)
	if err != nil {
		t.Fatalf("result path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatalf("write empty: %v", err)
	}

	if _, err := reader.ReadResult(attempt); !errors.Is(err, ErrResultMissing) {
		t.Fatalf("empty file error = %v, want %v", err, ErrResultMissing)
	}
}

// The path must be recomputable from the attempt alone, since after a crash the
// controller has nothing else to go on.
func TestFSResultReaderPathIsDeterministic(t *testing.T) {
	reader := NewFSResultReader("/results")
	attempt := AttemptID{RunUID: "uid-1", RunID: "run-1", State: "build", Attempt: 2}

	first, err := reader.ResultPath(attempt)
	if err != nil {
		t.Fatalf("result path: %v", err)
	}
	second, err := reader.ResultPath(attempt)
	if err != nil {
		t.Fatalf("result path: %v", err)
	}
	if first != second {
		t.Fatalf("result path is not deterministic: %q vs %q", first, second)
	}
	if _, err := reader.ResultPath(AttemptID{RunID: "../escape"}); err == nil {
		t.Error("accepted a traversing run id")
	}
}

// Journal sequences must survive the uint64 -> int64 API narrowing. uint64 is
// not a representable Kubernetes API type, and a status write carrying one
// panics inside structured-merge-diff, so this conversion is load-bearing.
func TestProjectSeqNarrowsSafely(t *testing.T) {
	for _, tc := range []struct {
		in   uint64
		want int64
	}{
		{0, 0},
		{1, 1},
		{1 << 40, 1 << 40},
		{uint64(1)<<63 - 1, int64(1)<<62 + (int64(1)<<62 - 1)},
		{1 << 63, int64(1)<<62 + (int64(1)<<62 - 1)}, // saturates, never negative
		{^uint64(0), int64(1)<<62 + (int64(1)<<62 - 1)},
	} {
		if got := projectSeq(tc.in); got != tc.want {
			t.Errorf("projectSeq(%d) = %d, want %d", tc.in, got, tc.want)
		}
		if projectSeq(tc.in) < 0 {
			t.Errorf("projectSeq(%d) produced a negative sequence", tc.in)
		}
	}
}
