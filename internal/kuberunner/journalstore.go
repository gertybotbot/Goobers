package kuberunner

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	apiv1 "github.com/goobers/goobers/api/v1alpha1"
	"github.com/goobers/goobers/internal/journal"
)

// The journal authority layer.
//
// Everything the reconciler decides is decided from a JournalHead: a read-only
// projection of the canonical append-only event log. The controller never keeps
// workflow state in memory across reconciles, never trusts CR status as input,
// and never infers a transition from a Job's status alone. If the process dies
// mid-reconcile, the next reconcile re-derives the identical head from the same
// durable bytes and reaches the same decision.
//
// This file is also where the "journal wins" rule is made mechanical rather
// than aspirational: ProjectStatus builds a GooberRunStatus purely as a
// function of the head, so wiping status and reconciling reproduces it exactly.

// GateWaitKind classifies why a run is parked.
type GateWaitKind string

const (
	// WaitNone means the run is not parked.
	WaitNone GateWaitKind = ""
	// WaitHumanGate means the run is parked at a human gate. A human gate
	// executes NO process, so the controller must create no Job for it — it
	// waits for an occurrence-bound GooberRunAction.
	WaitHumanGate GateWaitKind = "human-gate"
)

// JournalHead is the authoritative view of a run at one instant, derived
// entirely from committed events.
type JournalHead struct {
	// Identity is the pinned run identity from run.yaml.
	Identity journal.RunIdentity
	// Seq is the highest committed event sequence.
	Seq uint64
	// Phase is the reconstructed run phase.
	Phase journal.RunPhase
	// State is the machine state the run currently occupies.
	State string
	// Branch is the branch the current cursor sits on.
	Branch int
	// OpenStage is the stage of an in-flight attempt (a stage.started with no
	// matching stage.finished). Empty when no attempt is open.
	OpenStage string
	// OpenAttempt is the attempt number of the in-flight attempt.
	OpenAttempt int
	// OpenAttemptSeq is the sequence of the opening stage.started event. It
	// binds a dispatched Job to an exact journal occurrence.
	OpenAttemptSeq uint64
	// Wait classifies why the run is parked, if it is.
	Wait GateWaitKind
	// WaitGate is the gate name a parked run waits at.
	WaitGate string
	// WaitSeq is the sequence of the pausing event — the occurrence identity a
	// human action must quote.
	WaitSeq uint64
	// TerminalStatus is the committed run.finished status, when terminal.
	TerminalStatus string
	// LastErrorCode is the most recent journalled error classifier.
	LastErrorCode string
	// Attempts is the complete dispatch history reconstructed from
	// stage.started. Attempt numbering and status projection both derive from
	// this list; status is never an input.
	Attempts []JournalAttempt
	// Claim is the current business-claim fence recorded by claim.acquired.
	// It is cleared only by claim.released.
	Claim *ClaimToken
}

// JournalAttempt is one durable stage.started occurrence.
type JournalAttempt struct {
	State  string
	Branch int
	Number int
	Seq    uint64
}

// NextAttempt returns the next 1-based attempt number from journal history.
func (h JournalHead) NextAttempt(state string, branch int) int {
	highest := 0
	for _, attempt := range h.Attempts {
		if attempt.State == state && attempt.Branch == branch && attempt.Number > highest {
			highest = attempt.Number
		}
	}
	return highest + 1
}

// IsTerminal reports whether a run.finished has been committed.
func (h JournalHead) IsTerminal() bool {
	switch h.Phase {
	case journal.PhaseCompleted, journal.PhaseFailed, journal.PhaseAborted, journal.PhaseEscalated:
		return true
	default:
		return false
	}
}

// HasOpenAttempt reports whether a stage attempt is in flight.
func (h JournalHead) HasOpenAttempt() bool { return h.OpenStage != "" }

// JournalStore reads and appends the canonical journal. It is an interface so
// the reconciler can be exercised without a filesystem, but the production
// implementation is the plain on-disk run journal. Business-claim contention
// is a separate retained ConfigMap authority; it never replaces this workflow
// record.
type JournalStore interface {
	// Head returns the authoritative view of a run.
	Head(runID string) (JournalHead, error)
	// Events returns one immutable snapshot of the committed event log. It is
	// used by output projectors only; workflow decisions continue to use Head.
	Events(runID string) ([]journal.Event, error)
	// AppendStageStarted records dispatch intent for an attempt and returns the
	// committed sequence. It is called BEFORE the Job is created.
	AppendStageStarted(runID string, attempt AttemptID) (uint64, error)
	// AppendStageFinished commits a validated stage result.
	AppendStageFinished(runID string, attempt AttemptID, result ValidatedResult) (uint64, error)
	// AppendRunFinished commits the terminal transition.
	AppendRunFinished(runID string, status journal.RunPhase, target string) (uint64, error)
	// AppendGatePaused records a run parking at a human gate.
	AppendGatePaused(runID string, gate string, branch int) (uint64, error)
	// AppendClaimAcquired durably binds a run to the fencing epoch granted by
	// the business claim store. It is committed before any Job is dispatched.
	AppendClaimAcquired(runID string, token ClaimToken) (uint64, error)
	// AppendClaimReleased records terminal claim disposition after run.finished.
	AppendClaimReleased(runID string, token ClaimToken) (uint64, error)
	// EnsureNotificationRequested durably publishes a request derived from an
	// exact workflow-event occurrence. The check and append are atomic under the
	// journal writer lock, so restart or concurrent reconciliation cannot mint a
	// second publication. Notification output is never workflow authority.
	EnsureNotificationRequested(runID string, sourceSeq uint64, request apiv1.NotificationRequest) (seq uint64, created bool, err error)
}

// ErrRunNotFound means no journal exists for a run id.
var ErrRunNotFound = errors.New("kuberunner: run journal not found")

// FSJournalStore is the on-disk JournalStore over a runs directory.
type FSJournalStore struct {
	// RunsDir is the parent directory holding one subdirectory per run.
	RunsDir string
}

// NewFSJournalStore builds a store over runsDir.
func NewFSJournalStore(runsDir string) *FSJournalStore { return &FSJournalStore{RunsDir: runsDir} }

// runDir resolves a run's journal directory, refusing an id that could escape
// RunsDir. Run ids reaching here come from a CR spec, which is operator input,
// so this is a real boundary and not a formality.
func (s *FSJournalStore) runDir(runID string) (string, error) {
	if !apiv1.ValidRunID(runID) {
		return "", fmt.Errorf("kuberunner: invalid run id %q", runID)
	}
	return filepath.Join(s.RunsDir, runID), nil
}

// Head derives the authoritative view from the committed event log.
func (s *FSJournalStore) Head(runID string) (JournalHead, error) {
	identity, events, err := s.snapshot(runID)
	if err != nil {
		return JournalHead{}, err
	}
	head := HeadFromEvents(events)
	head.Identity = identity
	return head, nil
}

// Events returns a single committed event-log snapshot.
func (s *FSJournalStore) Events(runID string) ([]journal.Event, error) {
	_, events, err := s.snapshot(runID)
	return events, err
}

func (s *FSJournalStore) snapshot(runID string) (journal.RunIdentity, []journal.Event, error) {
	dir, err := s.runDir(runID)
	if err != nil {
		return journal.RunIdentity{}, nil, err
	}
	reader, err := journal.OpenRead(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return journal.RunIdentity{}, nil, fmt.Errorf("%w: %s", ErrRunNotFound, runID)
		}
		return journal.RunIdentity{}, nil, err
	}
	identity, err := reader.Identity()
	if err != nil {
		return journal.RunIdentity{}, nil, err
	}
	// Read events ONCE and derive everything from that single snapshot. Reading
	// the log twice (once for events, once for phase) can straddle a concurrent
	// append and produce a head that never actually existed.
	events, err := reader.Events()
	if err != nil {
		return journal.RunIdentity{}, nil, err
	}
	return identity, events, nil
}

// HeadFromEvents derives a head from an already-read event slice. It is
// exported so tests and any future replay tooling reconstruct a head by exactly
// the same rules the controller uses, rather than a lookalike.
func HeadFromEvents(events []journal.Event) JournalHead {
	head := JournalHead{Phase: journal.PhaseFromEvents(events)}

	for _, ev := range events {
		if ev.Seq > head.Seq {
			head.Seq = ev.Seq
		}
		switch ev.Type {
		case journal.EventRunStarted:
			head.State = ""
		case journal.EventStageStarted:
			head.Attempts = append(head.Attempts, JournalAttempt{
				State: ev.Stage, Branch: ev.Branch, Number: ev.Attempt, Seq: ev.Seq,
			})
			head.OpenStage = ev.Stage
			head.OpenAttempt = ev.Attempt
			head.OpenAttemptSeq = ev.Seq
			head.Branch = ev.Branch
			head.State = ev.Stage
			// A newly started attempt clears any parked state: the run is
			// executing again.
			head.Wait, head.WaitGate, head.WaitSeq = WaitNone, "", 0
		case journal.EventStageFinished:
			// Only the matching open attempt closes. A late duplicate for an
			// older attempt must not clear a newer in-flight one.
			if ev.Stage == head.OpenStage && ev.Attempt == head.OpenAttempt {
				head.OpenStage, head.OpenAttempt, head.OpenAttemptSeq = "", 0, 0
			}
		case journal.EventGatePaused:
			head.Wait = WaitHumanGate
			head.WaitGate = ev.Gate
			head.WaitSeq = ev.Seq
			head.Branch = ev.Branch
			head.State = ev.Gate
		case journal.EventGateEvaluated, journal.EventGateOverridden:
			head.Wait, head.WaitGate, head.WaitSeq = WaitNone, "", 0
			if ev.Target != "" {
				head.State = ev.Target
			}
		case journal.EventRunResumed:
			head.Wait, head.WaitGate, head.WaitSeq = WaitNone, "", 0
			head.OpenStage, head.OpenAttempt, head.OpenAttemptSeq = "", 0, 0
			if ev.Target != "" {
				head.State = ev.Target
			}
		case journal.EventRunFinished:
			head.TerminalStatus = ev.Status
			head.Wait, head.WaitGate, head.WaitSeq = WaitNone, "", 0
			head.OpenStage, head.OpenAttempt, head.OpenAttemptSeq = "", 0, 0
			if ev.Target != "" {
				head.State = ev.Target
			}
		case journal.EventError:
			if ev.Error != nil {
				head.LastErrorCode = ev.Error.Code
			}
		case journal.EventClaimAcquired:
			if token, ok := claimTokenFromEvent(ev); ok {
				head.Claim = &token
			}
		case journal.EventClaimReleased, journal.EventClaimForceReleased:
			head.Claim = nil
		}
	}
	return head
}

func claimTokenFromEvent(ev journal.Event) (ClaimToken, bool) {
	if ev.Runner == nil {
		return ClaimToken{}, false
	}
	epochText, ok := ev.Runner["fenceEpoch"].(string)
	epoch, epochErr := strconv.ParseInt(epochText, 10, 64)
	runUID, uidOK := ev.Runner["runUid"].(string)
	provider, providerOK := ev.Runner["provider"].(string)
	externalID, externalOK := ev.Runner["externalId"].(string)
	if !ok || epochErr != nil || epoch < 1 || !uidOK || !providerOK || !externalOK || ev.Gaggle == "" || ev.RunID == "" {
		return ClaimToken{}, false
	}
	return ClaimToken{
		Key:   ClaimKey{Gaggle: ev.Gaggle, Provider: provider, ExternalID: externalID},
		RunID: ev.RunID, RunUID: runUID, Epoch: epoch,
	}, true
}

// withRun opens the run journal for append, runs fn, and closes it. Every
// append path goes through here so the run lock is always released, including
// on a failing append: a leaked lock would wedge every later reconcile of that
// run, which is a far worse outcome than the append error itself.
func (s *FSJournalStore) withRun(runID string, fn func(*journal.Run) error) error {
	dir, err := s.runDir(runID)
	if err != nil {
		return err
	}
	if !journal.Recorded(dir) {
		return fmt.Errorf("%w: %s", ErrRunNotFound, runID)
	}
	run, _, err := journal.Recover(dir)
	if err != nil {
		return fmt.Errorf("kuberunner: open run %q: %w", runID, err)
	}
	defer func() { _ = run.Close() }()
	return fn(run)
}

// AppendStageStarted records dispatch intent before the Job is created.
func (s *FSJournalStore) AppendStageStarted(runID string, attempt AttemptID) (uint64, error) {
	var seq uint64
	err := s.withRun(runID, func(run *journal.Run) error {
		ev := journal.Event{
			Type:    journal.EventStageStarted,
			Stage:   attempt.State,
			Attempt: attempt.Attempt,
			Branch:  attempt.Branch,
		}
		// Attempts after the first are policy retries in this slice: the
		// controller owns retry, and it only retries under declared policy.
		// Infrastructure retries are a later slice and must be tagged
		// AttemptInfra so conformance keeps excluding them.
		if attempt.Attempt > 1 {
			ev.AttemptClass = journal.AttemptPolicy
		}
		if err := run.Append(ev); err != nil {
			return err
		}
		run.SetMachineState(attempt.State)
		seq = run.Seq()
		return nil
	})
	return seq, err
}

// AppendStageFinished commits a validated stage result. The ValidatedResult
// parameter type is the guard: an unvalidated envelope cannot reach this
// function, because ValidatedResult has no exported constructor.
func (s *FSJournalStore) AppendStageFinished(runID string, attempt AttemptID, result ValidatedResult) (uint64, error) {
	var seq uint64
	err := s.withRun(runID, func(run *journal.Run) error {
		ev := journal.Event{
			Type:      journal.EventStageFinished,
			Stage:     attempt.State,
			Attempt:   attempt.Attempt,
			Branch:    attempt.Branch,
			Status:    string(result.Envelope.Status),
			Outputs:   result.Envelope.Outputs,
			Integrity: result.Envelope.Integrity,
		}
		if attempt.Attempt > 1 {
			ev.AttemptClass = journal.AttemptPolicy
		}
		for _, ptr := range result.Envelope.Artifacts {
			ev.Artifacts = append(ev.Artifacts, journal.Ref{
				Path:      ptr.Path,
				Digest:    ptr.Digest,
				Size:      ptr.Size,
				MediaType: ptr.MediaType,
				Integrity: ptr.Integrity,
			})
		}
		if result.Envelope.Error != nil {
			ev.Error = &journal.ErrorDetail{
				Code:    result.Envelope.Error.Code,
				Message: result.Envelope.Error.Message,
			}
		}
		if err := run.Append(ev); err != nil {
			return err
		}
		seq = run.Seq()
		return nil
	})
	return seq, err
}

// AppendRunFinished commits the terminal transition. Terminal state must be
// durable BEFORE claims are released or provider cleanup runs; making this the
// controller's own call rather than a Job's is what keeps that ordering
// structurally enforceable.
func (s *FSJournalStore) AppendRunFinished(runID string, status journal.RunPhase, target string) (uint64, error) {
	var seq uint64
	err := s.withRun(runID, func(run *journal.Run) error {
		if err := run.Append(journal.Event{
			Type:   journal.EventRunFinished,
			Status: string(status),
			Target: target,
		}); err != nil {
			return err
		}
		seq = run.Seq()
		return nil
	})
	return seq, err
}

// AppendGatePaused records a run parking at a human gate.
func (s *FSJournalStore) AppendGatePaused(runID string, gate string, branch int) (uint64, error) {
	var seq uint64
	err := s.withRun(runID, func(run *journal.Run) error {
		if err := run.Append(journal.Event{
			Type:   journal.EventGatePaused,
			Gate:   gate,
			Branch: branch,
		}); err != nil {
			return err
		}
		run.SetMachineState(gate)
		seq = run.Seq()
		return nil
	})
	return seq, err
}

func (s *FSJournalStore) AppendClaimAcquired(runID string, token ClaimToken) (uint64, error) {
	return s.appendClaimEvent(runID, journal.EventClaimAcquired, token)
}

func (s *FSJournalStore) AppendClaimReleased(runID string, token ClaimToken) (uint64, error) {
	return s.appendClaimEvent(runID, journal.EventClaimReleased, token)
}

// EnsureNotificationRequested appends a deterministic output publication once.
// The source occurrence is rechecked while this process owns the journal's
// exclusive writer lock. A caller can therefore never publish a request for an
// event that was absent from the canonical log, and a restarted controller
// recovers the original request rather than rendering a replacement.
func (s *FSJournalStore) EnsureNotificationRequested(runID string, sourceSeq uint64, request apiv1.NotificationRequest) (uint64, bool, error) {
	var seq uint64
	var created bool
	err := s.withRun(runID, func(run *journal.Run) error {
		reader, err := journal.OpenRead(run.Dir())
		if err != nil {
			return err
		}
		events, err := reader.Events()
		if err != nil {
			return err
		}
		foundSource := false
		for _, ev := range events {
			if ev.Seq == sourceSeq && (ev.Type == journal.EventGatePaused || ev.Type == journal.EventRunFinished) {
				foundSource = true
			}
		}
		if !foundSource {
			return fmt.Errorf("kuberunner: notification source event %d is not a publishable journal occurrence", sourceSeq)
		}
		for _, ev := range events {
			if ev.Type != journal.EventNotificationRequested || ev.NotificationRequest == nil ||
				ev.NotificationRequest.NotificationID != request.NotificationID {
				continue
			}
			// The first durable rendering wins. A config reload may change the
			// selected sinks or TTL, but replay must recover the bytes published
			// for this occurrence rather than rewrite output history.
			seq = ev.Seq
			return nil
		}
		if err := run.Append(journal.Event{
			Type:                journal.EventNotificationRequested,
			NotificationRequest: &request,
		}); err != nil {
			return err
		}
		seq = run.Seq()
		created = true
		return nil
	})
	return seq, created, err
}

func (s *FSJournalStore) appendClaimEvent(runID string, eventType journal.EventType, token ClaimToken) (uint64, error) {
	var seq uint64
	err := s.withRun(runID, func(run *journal.Run) error {
		if err := run.Append(journal.Event{
			Type: eventType, Name: token.Key.ExternalID, Gaggle: token.Key.Gaggle,
			RunID: token.RunID,
			Runner: map[string]any{"provider": token.Key.Provider, "externalId": token.Key.ExternalID,
				"runUid": token.RunUID, "fenceEpoch": strconv.FormatInt(token.Epoch, 10)},
		}); err != nil {
			return err
		}
		seq = run.Seq()
		return nil
	})
	return seq, err
}

// projectSeq narrows a journal sequence to the API's int64 representation.
//
// The journal counts sequences as uint64, but uint64 is not a representable
// Kubernetes API type (structured-merge-diff cannot encode it, and a uint64
// field panics on any status write). The narrowing is safe for any real
// journal — a run would need 2^63 events to overflow — and is saturated rather
// than wrapped so that even an absurd value projects as a large sequence
// rather than silently becoming negative.
func projectSeq(seq uint64) int64 {
	const maxInt64 = uint64(1)<<63 - 1
	if seq > maxInt64 {
		return int64(maxInt64)
	}
	return int64(seq)
}

// ProjectPhase maps a journal phase onto the projected CR phase.
func ProjectPhase(head JournalHead) apiv1.GooberRunPhase {
	switch head.Phase {
	case journal.PhaseCompleted:
		return apiv1.GooberRunCompleted
	case journal.PhaseFailed:
		return apiv1.GooberRunFailed
	case journal.PhaseAborted:
		return apiv1.GooberRunAborted
	case journal.PhaseEscalated:
		return apiv1.GooberRunEscalated
	}
	// Non-terminal. Waiting is an operational refinement of running, not a
	// journal phase of its own.
	if head.Wait != WaitNone {
		return apiv1.GooberRunWaiting
	}
	if head.Seq == 0 {
		return apiv1.GooberRunPending
	}
	return apiv1.GooberRunRunning
}
