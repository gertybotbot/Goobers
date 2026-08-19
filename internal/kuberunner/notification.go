package kuberunner

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	apiv1 "github.com/goobers/goobers/api/v1alpha1"
	"github.com/goobers/goobers/internal/journal"
	"github.com/goobers/goobers/internal/notification"
)

const defaultNotificationTTL = 24 * time.Hour

// NotificationProjectionPolicy selects deployment-owned sinks for durable
// journal-derived notifications. Sink credentials and transports remain in the
// notification registry; a workflow event never constructs them.
type NotificationProjectionPolicy struct {
	Sinks []string
	TTL   time.Duration
}

func (p NotificationProjectionPolicy) normalized() (NotificationProjectionPolicy, error) {
	if len(p.Sinks) == 0 {
		return p, errors.New("kuberunner: at least one notification sink is required")
	}
	if p.TTL == 0 {
		p.TTL = defaultNotificationTTL
	}
	if p.TTL <= 0 {
		return p, errors.New("kuberunner: notification TTL must be positive")
	}
	return p, nil
}

// WakeHint says only that a durable journal publication may now exist. It is
// deliberately too small to carry a workflow decision: a receiver MUST reread
// the named run journal and may use the sequences only to coalesce wakeups.
type WakeHint struct {
	RunID          string `json:"runId"`
	RunUID         string `json:"runUid"`
	SourceSeq      uint64 `json:"sourceSeq"`
	PublicationSeq uint64 `json:"publicationSeq"`
}

// WakeGossip publishes lossy, advisory wake hints after durable publication.
// Failure or reordering cannot affect workflow correctness.
type WakeGossip interface {
	PublishWake(context.Context, WakeHint) error
}

// NotificationProjectionResult reports output work without becoming an input
// to workflow reconciliation.
type NotificationProjectionResult struct {
	Created    int
	HighestSeq uint64
	WakeErrors []error
}

// JournalNotificationProjector derives exact operational notification requests
// from committed human-pause and terminal occurrences. The request is appended
// to the SAME canonical journal before any wake gossip is attempted. Replaying
// after a crash therefore either creates the one request or recovers it.
type JournalNotificationProjector struct {
	Journal JournalStore
	Policy  NotificationProjectionPolicy
	Gossip  WakeGossip
}

// Project publishes every previously unseen notifiable occurrence in journal
// order. It never advances a workflow, releases a claim, or trusts a wake hint.
func (p *JournalNotificationProjector) Project(ctx context.Context, runUID, runID string) (NotificationProjectionResult, error) {
	if p == nil || p.Journal == nil {
		return NotificationProjectionResult{}, errors.New("kuberunner: notification projector requires a journal")
	}
	policy, err := p.Policy.normalized()
	if err != nil {
		return NotificationProjectionResult{}, err
	}
	head, err := p.Journal.Head(runID)
	if err != nil {
		return NotificationProjectionResult{}, err
	}
	events, err := p.Journal.Events(runID)
	if err != nil {
		return NotificationProjectionResult{}, err
	}

	var result NotificationProjectionResult
	var newestHint WakeHint
	for _, ev := range events {
		request, ok, renderErr := notificationFromEvent(head.Identity, ev, policy)
		if renderErr != nil {
			return result, renderErr
		}
		if !ok {
			continue
		}
		seq, created, ensureErr := p.Journal.EnsureNotificationRequested(runID, ev.Seq, request)
		if ensureErr != nil {
			return result, ensureErr
		}
		if created {
			result.Created++
		}
		if seq > result.HighestSeq {
			result.HighestSeq = seq
			newestHint = WakeHint{
				RunID: runID, RunUID: runUID, SourceSeq: ev.Seq, PublicationSeq: seq,
			}
		}
	}
	if p.Gossip != nil && newestHint.PublicationSeq != 0 {
		if gossipErr := p.Gossip.PublishWake(ctx, newestHint); gossipErr != nil {
			// Gossip is a doorbell, never an acknowledgement. Its failure is
			// reported for diagnostics but does not roll back or invalidate the
			// already-durable publication.
			result.WakeErrors = append(result.WakeErrors, gossipErr)
		}
	}
	return result, nil
}

func notificationFromEvent(identity journal.RunIdentity, ev journal.Event, policy NotificationProjectionPolicy) (apiv1.NotificationRequest, bool, error) {
	var severity apiv1.NotificationSeverity
	var transition, title, body, stage string
	switch ev.Type {
	case journal.EventGatePaused:
		severity = apiv1.NotificationSeverityWarning
		transition = "waiting"
		stage = ev.Gate
		title = fmt.Sprintf("Run %s needs a decision", identity.RunID)
		body = fmt.Sprintf("Workflow %s is waiting at human gate %s.", identity.Workflow, ev.Gate)
	case journal.EventRunFinished:
		transition = ev.Status
		stage = ev.Target
		title = fmt.Sprintf("Run %s %s", identity.RunID, ev.Status)
		body = fmt.Sprintf("Workflow %s finished in state %s with status %s.", identity.Workflow, ev.Target, ev.Status)
		switch journal.RunPhase(ev.Status) {
		case journal.PhaseCompleted:
			severity = apiv1.NotificationSeverityInfo
		case journal.PhaseEscalated:
			severity = apiv1.NotificationSeverityCritical
		default:
			severity = apiv1.NotificationSeverityError
		}
	default:
		return apiv1.NotificationRequest{}, false, nil
	}

	sequence := strconv.FormatUint(ev.Seq, 10)
	request := apiv1.NotificationRequest{
		Schema:         apiv1.NotificationRequestSchema,
		NotificationID: identity.RunID + ":journal:" + sequence,
		IncidentID:     identity.RunID,
		EventID:        "journal:" + sequence,
		Severity:       severity,
		Transition:     transition,
		Title:          title,
		Body:           body,
		Facts: []apiv1.NotificationFact{
			{Name: "journal-seq", Value: sequence},
			{Name: "gaggle", Value: identity.Gaggle},
		},
		Evidence: []apiv1.NotificationEvidenceRef{{Kind: "journal-event", ID: sequence}},
		Source: apiv1.NotificationSource{
			RunID: identity.RunID, Workflow: identity.Workflow, Stage: stage,
		},
		Sinks:          append([]string(nil), policy.Sinks...),
		ExpiresAt:      ev.Time.Add(policy.TTL).UTC(),
		IdempotencyKey: identity.RunID + ":journal:" + sequence,
	}
	if err := notification.ValidateRequest(request); err != nil {
		return apiv1.NotificationRequest{}, false, fmt.Errorf("render notification from journal event %d: %w", ev.Seq, err)
	}
	return request, true, nil
}

// WakeCoalescer rejects duplicate and stale wake hints. Accepted hints still
// carry no authority: the caller uses them only to enqueue a journal reread.
// Run UID is part of the key so a delayed hint for a deleted/recreated CR can
// never suppress the new occurrence's first wake.
type WakeCoalescer struct {
	mu      sync.Mutex
	highest map[wakeRun]uint64
}

type wakeRun struct {
	runID  string
	runUID string
}

// Offer reports whether hint is newer than every accepted hint for this exact
// run occurrence.
func (c *WakeCoalescer) Offer(hint WakeHint) bool {
	if hint.RunID == "" || hint.RunUID == "" || hint.SourceSeq == 0 || hint.PublicationSeq == 0 {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.highest == nil {
		c.highest = make(map[wakeRun]uint64)
	}
	key := wakeRun{runID: hint.RunID, runUID: hint.RunUID}
	if hint.PublicationSeq <= c.highest[key] {
		return false
	}
	c.highest[key] = hint.PublicationSeq
	return true
}
