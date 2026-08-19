package kuberunner

import (
	"context"
	"fmt"
	"sync"

	apiv1 "github.com/goobers/goobers/api/v1alpha1"
	"github.com/goobers/goobers/internal/journal"
)

// Test doubles.
//
// These are in-memory stand-ins for the journal and the result transport. They
// exist so the reconciler's DECISIONS can be tested at speed without a
// filesystem or an API server. They deliberately preserve the two properties
// the reconciler's correctness depends on:
//
//   - the journal is append-only and its head is DERIVED from the events, using
//     the same HeadFromEvents the production store uses. A fake that tracked
//     head state independently could stay consistent where the real one would
//     not, which would make these tests worse than useless.
//   - a result is only visible once published, and published bytes are returned
//     verbatim so the digest check is exercised for real.
//
// The on-disk implementations are covered separately against a real journal.

// fakeJournal is an in-memory append-only journal.
type fakeJournal struct {
	mu     sync.Mutex
	events map[string][]journal.Event
	// appendErr, when set, fails the next append. It simulates a crash between
	// the controller deciding to act and the write landing.
	appendErr error
	// appends counts committed events, so a test can assert that a repeated
	// reconcile did NOT write again.
	appends int
}

func newFakeJournal() *fakeJournal {
	return &fakeJournal{events: map[string][]journal.Event{}}
}

// seed starts a run with a run.started event, as journal.Create would.
func (f *fakeJournal) seed(runID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events[runID] = []journal.Event{{
		Schema: journal.EventSchema,
		Seq:    1,
		Type:   journal.EventRunStarted,
		Status: string(journal.PhaseRunning),
	}}
}

func (f *fakeJournal) append(runID string, ev journal.Event) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.appendErr != nil {
		err := f.appendErr
		f.appendErr = nil
		return 0, err
	}
	existing, ok := f.events[runID]
	if !ok {
		return 0, fmt.Errorf("%w: %s", ErrRunNotFound, runID)
	}
	ev.Schema = journal.EventSchema
	ev.Seq = uint64(len(existing)) + 1
	f.events[runID] = append(existing, ev)
	f.appends++
	return ev.Seq, nil
}

// snapshot returns a copy of a run's events for assertions.
func (f *fakeJournal) snapshot(runID string) []journal.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]journal.Event(nil), f.events[runID]...)
}

// types returns the event type sequence, which is what the ordering assertions
// actually care about.
func (f *fakeJournal) types(runID string) []journal.EventType {
	var out []journal.EventType
	for _, ev := range f.snapshot(runID) {
		out = append(out, ev.Type)
	}
	return out
}

func (f *fakeJournal) Head(runID string) (JournalHead, error) {
	f.mu.Lock()
	events, ok := f.events[runID]
	snapshot := append([]journal.Event(nil), events...)
	f.mu.Unlock()
	if !ok {
		return JournalHead{}, fmt.Errorf("%w: %s", ErrRunNotFound, runID)
	}
	// Derive the head exactly as production does.
	head := HeadFromEvents(snapshot)
	head.Identity = journal.RunIdentity{RunID: runID}
	return head, nil
}

func (f *fakeJournal) AppendStageStarted(runID string, attempt AttemptID) (uint64, error) {
	ev := journal.Event{
		Type:    journal.EventStageStarted,
		Stage:   attempt.State,
		Attempt: attempt.Attempt,
		Branch:  attempt.Branch,
	}
	if attempt.Attempt > 1 {
		ev.AttemptClass = journal.AttemptPolicy
	}
	return f.append(runID, ev)
}

func (f *fakeJournal) AppendStageFinished(runID string, attempt AttemptID, result ValidatedResult) (uint64, error) {
	ev := journal.Event{
		Type:    journal.EventStageFinished,
		Stage:   attempt.State,
		Attempt: attempt.Attempt,
		Branch:  attempt.Branch,
		Status:  string(result.Envelope.Status),
		Outputs: result.Envelope.Outputs,
	}
	for _, ptr := range result.Envelope.Artifacts {
		ev.Artifacts = append(ev.Artifacts, journal.Ref{Path: ptr.Path, Digest: ptr.Digest})
	}
	return f.append(runID, ev)
}

func (f *fakeJournal) AppendRunFinished(runID string, status journal.RunPhase, target string) (uint64, error) {
	return f.append(runID, journal.Event{
		Type:   journal.EventRunFinished,
		Status: string(status),
		Target: target,
	})
}

func (f *fakeJournal) AppendGatePaused(runID string, gate string, branch int) (uint64, error) {
	return f.append(runID, journal.Event{
		Type:   journal.EventGatePaused,
		Gate:   gate,
		Branch: branch,
	})
}

func (f *fakeJournal) AppendClaimAcquired(runID string, token ClaimToken) (uint64, error) {
	return f.append(runID, journal.Event{
		Type: journal.EventClaimAcquired, Name: token.Key.ExternalID, Gaggle: token.Key.Gaggle, RunID: token.RunID,
		Runner: map[string]any{"provider": token.Key.Provider, "externalId": token.Key.ExternalID, "runUid": token.RunUID, "fenceEpoch": fmt.Sprintf("%d", token.Epoch)},
	})
}

func (f *fakeJournal) AppendClaimReleased(runID string, token ClaimToken) (uint64, error) {
	return f.append(runID, journal.Event{
		Type: journal.EventClaimReleased, Name: token.Key.ExternalID, Gaggle: token.Key.Gaggle, RunID: token.RunID,
		Runner: map[string]any{"provider": token.Key.Provider, "externalId": token.Key.ExternalID, "runUid": token.RunUID, "fenceEpoch": fmt.Sprintf("%d", token.Epoch)},
	})
}

// fakeResults is an in-memory result transport.
type fakeResults struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newFakeResults() *fakeResults { return &fakeResults{data: map[string][]byte{}} }

// publish stores a well-formed receipt for an attempt.
func (f *fakeResults) publish(t interface{ Fatalf(string, ...any) }, attempt AttemptID, envelope apiv1.ResultEnvelope) {
	raw, err := EncodeReceipt(attempt, envelope)
	if err != nil {
		t.Fatalf("encode receipt: %v", err)
	}
	f.publishRaw(attempt, raw)
}

// publishRaw stores arbitrary bytes, so a test can publish a deliberately
// invalid receipt.
func (f *fakeResults) publishRaw(attempt AttemptID, raw []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[JobName(attempt)] = raw
}

func (f *fakeResults) ReadResult(attempt AttemptID) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	raw, ok := f.data[JobName(attempt)]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrResultMissing, attempt.String())
	}
	return raw, nil
}

// testMachine is a small explicit state machine. Using a hand-built graph
// rather than the workflow compiler keeps these tests about the RECONCILER:
// a compiler change should not be able to break them, and a machine bug should
// not be able to mask a reconciler bug.
type testMachine struct {
	start    string
	kinds    map[string]StateKind
	next     map[string]string
	attempts map[string]int
}

func newTestMachine() *testMachine {
	return &testMachine{
		start:    "build",
		kinds:    map[string]StateKind{"build": StateTask, "@complete": StateTerminal, "@abort": StateTerminal},
		next:     map[string]string{"build": "@complete"},
		attempts: map[string]int{},
	}
}

func (m *testMachine) Start() string { return m.start }

func (m *testMachine) Kind(state string) StateKind {
	if kind, ok := m.kinds[state]; ok {
		return kind
	}
	return StateUnknown
}

func (m *testMachine) Next(state string, status apiv1.ResultStatus) (string, bool) {
	if status != apiv1.ResultSuccess && status != apiv1.ResultNoWork {
		return "@abort", false
	}
	target, ok := m.next[state]
	if !ok || target == "@complete" {
		return journal.TargetComplete, false
	}
	return target, true
}

func (m *testMachine) TerminalPhase(target string) journal.RunPhase {
	switch target {
	case "@abort":
		return journal.PhaseAborted
	case "@escalate":
		return journal.PhaseEscalated
	default:
		return journal.PhaseCompleted
	}
}

func (m *testMachine) MaxAttempts(state string) int {
	if n, ok := m.attempts[state]; ok && n > 0 {
		return n
	}
	return 1
}

var _ StateMachine = (*testMachine)(nil)
var _ JournalStore = (*fakeJournal)(nil)
var _ ResultReader = (*fakeResults)(nil)
var _ MachineResolver = StaticMachineResolver{}
var _ = context.Background
