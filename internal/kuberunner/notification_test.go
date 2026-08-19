package kuberunner

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	apiv1 "github.com/goobers/goobers/api/v1alpha1"
	"github.com/goobers/goobers/internal/journal"
)

type recordingWakeGossip struct {
	mu    sync.Mutex
	hints []WakeHint
	err   error
}

func (g *recordingWakeGossip) PublishWake(_ context.Context, hint WakeHint) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.hints = append(g.hints, hint)
	return g.err
}

func (g *recordingWakeGossip) snapshot() []WakeHint {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]WakeHint(nil), g.hints...)
}

func notificationEvents(t *testing.T, store *FSJournalStore, runID string) []journal.Event {
	t.Helper()
	events, err := store.Events(runID)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	var requests []journal.Event
	for _, ev := range events {
		if ev.Type == journal.EventNotificationRequested {
			requests = append(requests, ev)
		}
	}
	return requests
}

func TestJournalNotificationProjectorRecoversDurablePublicationAfterRestart(t *testing.T) {
	store, runID := newRealJournal(t)
	if _, err := store.AppendGatePaused(runID, "approve", 0); err != nil {
		t.Fatalf("append gate pause: %v", err)
	}
	policy := NotificationProjectionPolicy{Sinks: []string{"operations"}, TTL: time.Hour}

	// Simulate a crash after the durable publication and before wake gossip.
	first := &JournalNotificationProjector{Journal: store, Policy: policy}
	result, err := first.Project(context.Background(), "uid-1", runID)
	if err != nil {
		t.Fatalf("first project: %v", err)
	}
	if result.Created != 1 || result.HighestSeq != 3 {
		t.Fatalf("first projection = %+v, want one request at seq 3", result)
	}
	requests := notificationEvents(t, store, runID)
	if len(requests) != 1 {
		t.Fatalf("notification requests = %d, want 1", len(requests))
	}
	request := requests[0].NotificationRequest
	if request == nil || request.EventID != "journal:2" || request.Transition != "waiting" ||
		request.Source.Stage != "approve" || !reflect.DeepEqual(request.Sinks, []string{"operations"}) {
		t.Fatalf("durable request = %+v", request)
	}

	// A new store and projector are process-restart equivalents. They recover
	// the request from the journal, append nothing, and may repeat the advisory
	// doorbell because the durable publication—not gossip—is the source of truth.
	gossip := &recordingWakeGossip{}
	restarted := &JournalNotificationProjector{
		Journal: NewFSJournalStore(store.RunsDir),
		// A config reload does not rewrite the already-durable request. The
		// original operations sink and expiry remain the occurrence's output.
		Policy: NotificationProjectionPolicy{Sinks: []string{"new-operations"}, TTL: 2 * time.Hour},
		Gossip: gossip,
	}
	result, err = restarted.Project(context.Background(), "uid-1", runID)
	if err != nil {
		t.Fatalf("restart project: %v", err)
	}
	if result.Created != 0 || len(notificationEvents(t, store, runID)) != 1 {
		t.Fatalf("restart duplicated publication: result=%+v", result)
	}
	hints := gossip.snapshot()
	if len(hints) != 1 || hints[0].SourceSeq != 2 || hints[0].PublicationSeq != 3 {
		t.Fatalf("restart wake hints = %+v", hints)
	}
}

func TestJournalNotificationProjectorPublishesOccurrencesInJournalOrder(t *testing.T) {
	store, runID := newRealJournal(t)
	if _, err := store.AppendGatePaused(runID, "approve", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendRunFinished(runID, journal.PhaseEscalated, "@escalate"); err != nil {
		t.Fatal(err)
	}
	gossip := &recordingWakeGossip{}
	projector := &JournalNotificationProjector{
		Journal: store,
		Policy:  NotificationProjectionPolicy{Sinks: []string{"operations"}},
		Gossip:  gossip,
	}
	result, err := projector.Project(context.Background(), "uid-1", runID)
	if err != nil {
		t.Fatal(err)
	}
	if result.Created != 2 {
		t.Fatalf("created = %d, want 2", result.Created)
	}
	requests := notificationEvents(t, store, runID)
	if len(requests) != 2 {
		t.Fatalf("requests = %d, want 2", len(requests))
	}
	if got := []string{requests[0].NotificationRequest.EventID, requests[1].NotificationRequest.EventID}; !reflect.DeepEqual(got, []string{"journal:2", "journal:3"}) {
		t.Fatalf("request source order = %v", got)
	}
	if requests[1].NotificationRequest.Severity != apiv1.NotificationSeverityCritical {
		t.Fatalf("terminal severity = %q, want critical", requests[1].NotificationRequest.Severity)
	}
	if hints := gossip.snapshot(); len(hints) != 1 || hints[0].SourceSeq != 3 || hints[0].PublicationSeq != 5 {
		t.Fatalf("coalesced newest wake hint = %+v", hints)
	}

	// Replaying an older occurrence after the newer terminal request exists is
	// inert: both deterministic identities recover their original publications.
	result, err = projector.Project(context.Background(), "uid-1", runID)
	if err != nil || result.Created != 0 || len(notificationEvents(t, store, runID)) != 2 {
		t.Fatalf("idempotent replay = %+v, err=%v", result, err)
	}
}

func TestJournalNotificationProjectorConcurrentReplayPublishesOnce(t *testing.T) {
	store, runID := newRealJournal(t)
	if _, err := store.AppendRunFinished(runID, journal.PhaseCompleted, journal.TargetComplete); err != nil {
		t.Fatal(err)
	}
	policy := NotificationProjectionPolicy{Sinks: []string{"operations"}}
	start := make(chan struct{})
	type outcome struct {
		result NotificationProjectionResult
		err    error
	}
	outcomes := make(chan outcome, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			result, err := (&JournalNotificationProjector{
				Journal: NewFSJournalStore(store.RunsDir), Policy: policy,
			}).Project(context.Background(), "uid-1", runID)
			outcomes <- outcome{result: result, err: err}
		}()
	}
	close(start)
	created := 0
	for i := 0; i < 2; i++ {
		outcome := <-outcomes
		if outcome.err != nil {
			t.Fatalf("concurrent projection: %v", outcome.err)
		}
		created += outcome.result.Created
	}
	if created != 1 || len(notificationEvents(t, store, runID)) != 1 {
		t.Fatalf("concurrent created total = %d, durable requests = %d", created, len(notificationEvents(t, store, runID)))
	}
}

func TestWakeGossipFailureCannotInvalidateDurablePublication(t *testing.T) {
	store, runID := newRealJournal(t)
	if _, err := store.AppendRunFinished(runID, journal.PhaseCompleted, journal.TargetComplete); err != nil {
		t.Fatal(err)
	}
	projector := &JournalNotificationProjector{
		Journal: store,
		Policy:  NotificationProjectionPolicy{Sinks: []string{"operations"}},
		Gossip:  &recordingWakeGossip{err: errors.New("gossip unavailable")},
	}
	result, err := projector.Project(context.Background(), "uid-1", runID)
	if err != nil {
		t.Fatalf("gossip became publication authority: %v", err)
	}
	if result.Created != 1 || len(result.WakeErrors) != 1 || len(notificationEvents(t, store, runID)) != 1 {
		t.Fatalf("projection with failed gossip = %+v", result)
	}
}

func TestWakeCoalescerRejectsStaleAndDuplicateHints(t *testing.T) {
	var coalescer WakeCoalescer
	newest := WakeHint{RunID: "run-1", RunUID: "uid-1", SourceSeq: 8, PublicationSeq: 12}
	if !coalescer.Offer(newest) {
		t.Fatal("new hint was rejected")
	}
	for _, stale := range []WakeHint{
		newest,
		{RunID: "run-1", RunUID: "uid-1", SourceSeq: 7, PublicationSeq: 11},
		{RunID: "run-1", RunUID: "uid-1", SourceSeq: 99, PublicationSeq: 0},
	} {
		if coalescer.Offer(stale) {
			t.Fatalf("accepted stale or invalid hint %+v", stale)
		}
	}
	if !coalescer.Offer(WakeHint{RunID: "run-1", RunUID: "uid-1", SourceSeq: 9, PublicationSeq: 13}) {
		t.Fatal("strictly newer hint was rejected")
	}
	if !coalescer.Offer(WakeHint{RunID: "run-1", RunUID: "uid-2", SourceSeq: 2, PublicationSeq: 3}) {
		t.Fatal("recreated run occurrence was suppressed by predecessor high-water")
	}
}

func TestRunReconcilerProjectsPauseNotificationWithoutCreatingAuthority(t *testing.T) {
	h := newHarness(t)
	h.machine.start = "approve"
	h.machine.kinds["approve"] = StateHumanGate
	h.reconciler.Notifications = &JournalNotificationProjector{
		Journal: h.journal,
		Policy:  NotificationProjectionPolicy{Sinks: []string{"operations"}},
	}

	// First pass commits the authoritative pause. The next pass derives output
	// from that occurrence and then projects Waiting from the journal head.
	h.reconcile()
	h.reconcile()
	h.reconcile()
	if got := h.journal.types(testRunID); !reflect.DeepEqual(got, []journal.EventType{
		journal.EventRunStarted,
		journal.EventGatePaused,
		journal.EventNotificationRequested,
	}) {
		t.Fatalf("journal event order = %v", got)
	}
	if len(h.jobs()) != 0 {
		t.Fatal("notification projection created an execution Job")
	}
	run := h.run()
	if run.Status.Phase != apiv1.GooberRunWaiting || run.Status.ObservedSeq != 3 || run.Status.Pause == nil || run.Status.Pause.Seq != 2 {
		t.Fatalf("projected run status = %+v", run.Status)
	}
}
