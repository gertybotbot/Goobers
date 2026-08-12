package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// This file declares the two RUNTIME CRDs of the Kubernetes-native runner
// (docs/design/kubernetes-native-runner.md). They are deliberately the ONLY
// runtime kinds: there is no CRD per event, artifact, attempt, or claim.
// High-volume immutable data lives in the run journal and artifact store, and
// a Kubernetes Job already represents one stage attempt.
//
// The authority rule these types encode:
//
//	The canonical run journal is authoritative. GooberRunStatus is a
//	projection of it and can be deleted and rebuilt exactly. A Job's status is
//	execution evidence, never workflow authority.

// GooberRunPhase is the coarse projected lifecycle phase of a run. It mirrors
// journal.RunPhase plus the operational "Waiting" projection a controller needs
// in order to say "parked at a human gate" in `kubectl get`. Waiting is NOT a
// journal phase: the journal still considers such a run running, so this value
// must never be used to infer a terminal transition.
// +kubebuilder:validation:Enum=Pending;Running;Waiting;Completed;Failed;Aborted;Escalated
type GooberRunPhase string

const (
	// GooberRunPending is a projected run that has not yet been reconciled.
	GooberRunPending GooberRunPhase = "Pending"
	// GooberRunRunning is a projected run actively advancing.
	GooberRunRunning GooberRunPhase = "Running"
	// GooberRunWaiting is a projected run parked at a human gate awaiting an
	// occurrence-bound GooberRunAction. No Job exists for a waiting run.
	GooberRunWaiting GooberRunPhase = "Waiting"
	// GooberRunCompleted projects journal phase "completed".
	GooberRunCompleted GooberRunPhase = "Completed"
	// GooberRunFailed projects journal phase "failed".
	GooberRunFailed GooberRunPhase = "Failed"
	// GooberRunAborted projects journal phase "aborted".
	GooberRunAborted GooberRunPhase = "Aborted"
	// GooberRunEscalated projects journal phase "escalated".
	GooberRunEscalated GooberRunPhase = "Escalated"
)

// IsTerminal reports whether the phase projects a committed run.finished.
func (p GooberRunPhase) IsTerminal() bool {
	switch p {
	case GooberRunCompleted, GooberRunFailed, GooberRunAborted, GooberRunEscalated:
		return true
	default:
		return false
	}
}

// GooberRunConditionReady is the condition summarizing whether the projection
// is in sync with the durable journal head.
const GooberRunConditionReady = "Ready"

// GooberRunConditionSettled is the condition asserting a committed terminal
// run.finished event exists in the canonical journal.
const GooberRunConditionSettled = "Settled"

// PinnedWorkflow is the immutable workflow identity a run started on and must
// complete on (WF-016). Every field is required and immutable: a config reload
// must never retune work in flight, and a stale action must never reopen the
// wrong occurrence of a machine.
type PinnedWorkflow struct {
	// Name is the workflow definition name.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name" yaml:"name"`
	// Version is the registry-assigned monotonic definition version.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=1
	Version int32 `json:"version" yaml:"version"`
	// Digest is the tamper-evident content digest of the compiled definition
	// ("sha256:<64-hex>", from workflow Machine.Digest()).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^sha256:[0-9a-f]{64}$`
	Digest string `json:"digest" yaml:"digest"`
	// DSLVersion is the workflow DSL version the definition compiled under.
	// +optional
	DSLVersion string `json:"dslVersion,omitempty" yaml:"dslVersion,omitempty"`
	// GraphRef points at the immutable compiled-graph input snapshot in the
	// journal. The controller resolves the machine by digest, never by
	// re-reading live config.
	// +optional
	GraphRef *ArtifactPointer `json:"graphRef,omitempty" yaml:"graphRef,omitempty"`
}

// RepositoryIdentity pins the provider kind and repository an attempt acts on.
// Provider kind lives in the immutable pinned identity precisely because the
// live Gitea incident was caused by terminal/cleanup work routing to the wrong
// provider: a run must not be able to discover its provider from mutable config
// at a terminal seam.
type RepositoryIdentity struct {
	// Provider is the provider kind ("github", "gitea", "azuredevops").
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Provider string `json:"provider" yaml:"provider"`
	// Repository is the provider-scoped repository identifier (e.g. "org/repo").
	// +optional
	Repository string `json:"repository,omitempty" yaml:"repository,omitempty"`
	// ExternalID is the backlog item / issue identifier this run claims, when
	// the run is item-triggered. It is the claim key's external component.
	// +optional
	ExternalID string `json:"externalId,omitempty" yaml:"externalId,omitempty"`
}

// RunTrigger records what caused the run to start.
type RunTrigger struct {
	// Kind is the trigger kind.
	// +kubebuilder:validation:Enum=manual;schedule;signal;item
	// +kubebuilder:validation:Required
	Kind string `json:"kind" yaml:"kind"`
	// Ref is the trigger-specific reference: cron expression, signal name, or
	// backlog item id. Empty for a bare manual run.
	// +optional
	Ref string `json:"ref,omitempty" yaml:"ref,omitempty"`
}

// GooberRunSpec is the IMMUTABLE pinned identity of one run. Kubernetes enforces
// the immutability with a CEL transition rule so that neither an operator nor a
// controller bug can retune a run in flight; every mutable fact about the run
// lives in the canonical journal and is projected into status.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="GooberRun spec is immutable: pinned run identity may never be retuned in flight"
type GooberRunSpec struct {
	// RunID is the globally unique run identifier — the OpenTelemetry trace id
	// for the run, and the journal directory name.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._-]*$`
	RunID string `json:"runId" yaml:"runId"`
	// Gaggle is the gaggle this run belongs to.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Gaggle string `json:"gaggle" yaml:"gaggle"`
	// Goober is the selected worker persona for agentic stages.
	// +optional
	Goober string `json:"goober,omitempty" yaml:"goober,omitempty"`
	// Workflow is the pinned workflow identity (WF-016).
	// +kubebuilder:validation:Required
	Workflow PinnedWorkflow `json:"workflow" yaml:"workflow"`
	// Trigger is what started the run.
	// +kubebuilder:validation:Required
	Trigger RunTrigger `json:"trigger" yaml:"trigger"`
	// Repository pins provider routing for every attempt and terminal seam.
	// +optional
	Repository *RepositoryIdentity `json:"repository,omitempty" yaml:"repository,omitempty"`
	// RunControls pins the effective inherited safety budgets this run started
	// with, so a config reload cannot widen a live run's budget.
	// +optional
	RunControls *RunControls `json:"runControls,omitempty" yaml:"runControls,omitempty"`
	// Inputs are the content-digested immutable input snapshots pinned at run
	// start. They are pointers, never inline bulk data.
	// +optional
	// +listType=atomic
	Inputs []ArtifactPointer `json:"inputs,omitempty" yaml:"inputs,omitempty"`
	// JournalRoot is the durable location of the canonical run journal. It is
	// the authority the controller reads before every decision.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	JournalRoot string `json:"journalRoot" yaml:"journalRoot"`
	// ArtifactStoreClass names the artifact-store profile attempts publish to.
	// +optional
	ArtifactStoreClass string `json:"artifactStoreClass,omitempty" yaml:"artifactStoreClass,omitempty"`
	// WorkerImage is the container image an attempt Job runs.
	// +optional
	WorkerImage string `json:"workerImage,omitempty" yaml:"workerImage,omitempty"`
	// ServiceAccountName is the per-gaggle service account attempt Jobs run as.
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty" yaml:"serviceAccountName,omitempty"`
}

// AttemptRef projects one dispatched Kubernetes Job. It is evidence, not
// authority: the controller records what it created so that a duplicate
// observation is recognisable, but only a committed journal event advances the
// machine.
type AttemptRef struct {
	// JobName is the deterministic Job name derived from run UID, state, and
	// attempt (see kuberunner.JobName). It is stable across reconciles, which is
	// what makes create-once safe after a crash between journal append and Job
	// create.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	JobName string `json:"jobName" yaml:"jobName"`
	// State is the workflow machine state this attempt executes.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	State string `json:"state" yaml:"state"`
	// Attempt is the 1-based attempt number within the state.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=1
	Attempt int32 `json:"attempt" yaml:"attempt"`
	// Branch is the parallel branch id (0 is the run's root branch).
	// +optional
	Branch int32 `json:"branch,omitempty" yaml:"branch,omitempty"`
	// AttemptClass tags why a non-initial attempt exists ("policy", "infra",
	// "human"). Empty on the initial attempt.
	// +kubebuilder:validation:Enum=policy;infra;human
	// +optional
	AttemptClass string `json:"attemptClass,omitempty" yaml:"attemptClass,omitempty"`
	// DispatchedSeq is the journal sequence of the stage.started event this Job
	// was dispatched for. It binds the Job to an exact journal occurrence.
	// +optional
	DispatchedSeq int64 `json:"dispatchedSeq,omitempty" yaml:"dispatchedSeq,omitempty"`
}

// PauseOccurrence identifies the exact journal occurrence a run is parked at.
// A human action must quote it, so a stale click on an old notification cannot
// resolve a later pause at the same gate name.
type PauseOccurrence struct {
	// Gate is the gate name the run is paused at.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Gate string `json:"gate" yaml:"gate"`
	// Seq is the journal sequence of the pausing event. It is the occurrence
	// identity: the same gate may pause many times in one run.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=1
	Seq int64 `json:"seq" yaml:"seq"`
	// Branch is the branch the pause occurred on.
	// +optional
	Branch int32 `json:"branch,omitempty" yaml:"branch,omitempty"`
}

// GooberRunStatus is a PROJECTION of the canonical journal. Every field here is
// rebuildable: deleting the whole status and reconciling must reproduce it
// exactly. When status and journal disagree, the journal wins.
type GooberRunStatus struct {
	// ObservedGeneration is the .metadata.generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" yaml:"observedGeneration,omitempty"`
	// Phase is the coarse projected lifecycle phase.
	// +optional
	Phase GooberRunPhase `json:"phase,omitempty" yaml:"phase,omitempty"`
	// State is the current machine state (or terminal target) the journal says
	// the run occupies.
	// +optional
	State string `json:"state,omitempty" yaml:"state,omitempty"`
	// ObservedSeq is the highest journal sequence this projection reflects. It
	// is the projection's watermark, never workflow progress in itself.
	//
	// It is int64 rather than the journal's own uint64 because uint64 is not a
	// representable Kubernetes API type: the apiserver's structured-merge-diff
	// cannot encode it, so a uint64 field panics on any status write. Sequences
	// are small monotonic counters, so the narrowing is not lossy in practice.
	// +optional
	ObservedSeq int64 `json:"observedSeq,omitempty" yaml:"observedSeq,omitempty"`
	// Attempts are the Jobs the controller has dispatched, most recent last.
	// +optional
	// +listType=map
	// +listMapKey=jobName
	Attempts []AttemptRef `json:"attempts,omitempty" yaml:"attempts,omitempty"`
	// Pause is the occurrence a waiting run is parked at. Set iff phase is
	// Waiting.
	// +optional
	Pause *PauseOccurrence `json:"pause,omitempty" yaml:"pause,omitempty"`
	// TerminalStatus is the committed run.finished status, when terminal.
	// +optional
	TerminalStatus string `json:"terminalStatus,omitempty" yaml:"terminalStatus,omitempty"`
	// LastErrorCode is the most recent journalled error classifier.
	// +optional
	LastErrorCode string `json:"lastErrorCode,omitempty" yaml:"lastErrorCode,omitempty"`
	// Conditions follow standard Kubernetes conventions.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" yaml:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=grun
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`
// +kubebuilder:printcolumn:name="Seq",type=integer,JSONPath=`.status.observedSeq`
// +kubebuilder:printcolumn:name="Workflow",type=string,JSONPath=`.spec.workflow.name`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// GooberRun is one pinned workflow run reconciled by the Kubernetes-native
// runner. Its spec is immutable pinned identity; its status is a rebuildable
// projection of the canonical journal.
type GooberRun struct {
	metav1.TypeMeta   `json:",inline" yaml:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty" yaml:"metadata,omitempty"`

	// +kubebuilder:validation:Required
	Spec GooberRunSpec `json:"spec" yaml:"spec"`
	// +optional
	Status GooberRunStatus `json:"status,omitempty" yaml:"status,omitempty"`
}

// +kubebuilder:object:root=true

// GooberRunList is a list of GooberRun objects.
type GooberRunList struct {
	metav1.TypeMeta `json:",inline" yaml:",inline"`
	metav1.ListMeta `json:"metadata,omitempty" yaml:"metadata,omitempty"`
	Items           []GooberRun `json:"items" yaml:"items"`
}
