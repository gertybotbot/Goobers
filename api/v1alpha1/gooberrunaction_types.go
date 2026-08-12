package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// GooberRunActionKind is the intervention an action requests.
// +kubebuilder:validation:Enum=GateDecision;GateOverride;StageRerun;Cancel;ResumeFromTerminal
type GooberRunActionKind string

const (
	// ActionGateDecision supplies a human gate's verdict.
	ActionGateDecision GooberRunActionKind = "GateDecision"
	// ActionGateOverride replaces a nondeterministic gate's verdict with a
	// configured branch. It requires a rationale.
	ActionGateOverride GooberRunActionKind = "GateOverride"
	// ActionStageRerun reruns one agentic task or gate with a one-off
	// instruction addendum.
	ActionStageRerun GooberRunActionKind = "StageRerun"
	// ActionCancel interrupts a live run.
	ActionCancel GooberRunActionKind = "Cancel"
	// ActionResumeFromTerminal reopens an escalated or failed run at a chosen
	// workflow state.
	ActionResumeFromTerminal GooberRunActionKind = "ResumeFromTerminal"
)

// GooberRunActionPhase is the projected disposition of a one-shot action.
// +kubebuilder:validation:Enum=Pending;Accepted;Applied;Rejected
type GooberRunActionPhase string

const (
	// ActionPending is an action not yet evaluated.
	ActionPending GooberRunActionPhase = "Pending"
	// ActionAccepted is an action whose targeting validated but whose journal
	// effect is not yet committed.
	ActionAccepted GooberRunActionPhase = "Accepted"
	// ActionApplied is an action whose journal effect is durably committed. It
	// is terminal: an applied action is never replayed.
	ActionApplied GooberRunActionPhase = "Applied"
	// ActionRejected is an action refused as stale, misaddressed, or invalid.
	// It is terminal.
	ActionRejected GooberRunActionPhase = "Rejected"
)

// Action rejection reason codes. These are stable machine-readable classifiers
// so the portal and API can explain a refusal without parsing prose.
const (
	// ActionRejectedStaleOccurrence means the run is no longer at the quoted
	// pause/terminal occurrence: a later occurrence superseded it. This is the
	// guard against a delayed notification or a double-clicked button acting on
	// the wrong occurrence of the same gate name.
	ActionRejectedStaleOccurrence = "StaleOccurrence"
	// ActionRejectedRunUIDMismatch means the action targets a different object
	// than the GooberRun that currently bears the run name — a recreated CR is
	// a different occurrence and inherits nothing.
	ActionRejectedRunUIDMismatch = "RunUIDMismatch"
	// ActionRejectedRunNotFound means no GooberRun matches the target.
	ActionRejectedRunNotFound = "RunNotFound"
	// ActionRejectedInvalid means the action payload is structurally invalid
	// for its kind.
	ActionRejectedInvalid = "Invalid"
)

// ActionTarget binds an action to an EXACT machine occurrence. Addressing by
// stage or gate NAME alone is insufficient: the same gate may pause repeatedly
// within one run, and a run CR may be deleted and recreated. Both the run UID
// and the journal sequence must match, or the action is rejected.
type ActionTarget struct {
	// RunName is the GooberRun object name in the action's namespace.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	RunName string `json:"runName" yaml:"runName"`
	// RunUID is the Kubernetes UID of the targeted GooberRun. A recreated CR
	// gets a fresh UID, so a queued action cannot leak across occurrences.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	RunUID string `json:"runUid" yaml:"runUid"`
	// RunID is the canonical journal run identifier, carried for auditability
	// and cross-checked against the run's pinned spec.
	// +optional
	RunID string `json:"runId,omitempty" yaml:"runId,omitempty"`
	// OccurrenceSeq is the journal sequence of the exact pause or terminal
	// event this action answers. It is the anti-stale-click guard.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=1
	OccurrenceSeq int64 `json:"occurrenceSeq" yaml:"occurrenceSeq"`
	// Stage names the stage or gate the occurrence belongs to. It is a
	// human-legibility cross-check, not the identity.
	// +optional
	Stage string `json:"stage,omitempty" yaml:"stage,omitempty"`
	// Branch is the branch the occurrence lives on (0 is the root branch).
	// +optional
	Branch int32 `json:"branch,omitempty" yaml:"branch,omitempty"`
}

// GooberRunActionSpec is an IMMUTABLE one-shot intervention request. It is
// immutable because an action is a fact ("this actor decided this, for this
// occurrence"), not desired state: editing one in place would rewrite history.
//
// The conditional-requirement rules below test size() rather than comparing
// against an empty-string literal. A bare ” in a marker round-trips into a
// Unicode right double quote in the generated YAML, which then fails CEL
// compilation and makes the apiserver reject the whole CRD at install time.
// size(x) > 0 states the same requirement and survives the round trip.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="GooberRunAction spec is immutable: an intervention is a one-shot fact, not desired state"
// +kubebuilder:validation:XValidation:rule="self.kind != 'GateOverride' || (has(self.rationale) && size(self.rationale) > 0)",message="a gate override requires a rationale"
// +kubebuilder:validation:XValidation:rule="self.kind != 'GateDecision' || (has(self.decision) && size(self.decision) > 0)",message="a gate decision requires a decision"
// +kubebuilder:validation:XValidation:rule="self.kind != 'ResumeFromTerminal' || (has(self.targetState) && size(self.targetState) > 0)",message="a resume from terminal requires a target state"
type GooberRunActionSpec struct {
	// Kind is the intervention being requested.
	// +kubebuilder:validation:Required
	Kind GooberRunActionKind `json:"kind" yaml:"kind"`
	// Target binds this action to an exact run UID and journal occurrence.
	// +kubebuilder:validation:Required
	Target ActionTarget `json:"target" yaml:"target"`
	// Actor identifies the human principal that requested the intervention. It
	// is journalled verbatim for audit.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Actor string `json:"actor" yaml:"actor"`
	// Decision is the gate outcome selected by a GateDecision action.
	// +optional
	Decision string `json:"decision,omitempty" yaml:"decision,omitempty"`
	// TargetState is the workflow state a ResumeFromTerminal action reopens at,
	// or the branch a GateOverride selects. It is named distinctly from Target
	// (the occurrence binding) because the two are unrelated: one says WHICH
	// occurrence is being answered, the other says WHERE the machine should go.
	// +optional
	TargetState string `json:"targetState,omitempty" yaml:"targetState,omitempty"`
	// Rationale explains why an operator overrode a nondeterministic gate. It
	// is required for GateOverride.
	// +optional
	Rationale string `json:"rationale,omitempty" yaml:"rationale,omitempty"`
	// InstructionAddendum is the one-off instruction text supplied for a
	// StageRerun.
	// +optional
	InstructionAddendum string `json:"instructionAddendum,omitempty" yaml:"instructionAddendum,omitempty"`
}

// GooberRunActionStatus projects how the controller disposed of the action.
type GooberRunActionStatus struct {
	// ObservedGeneration is the .metadata.generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" yaml:"observedGeneration,omitempty"`
	// Phase is the action's disposition.
	// +optional
	Phase GooberRunActionPhase `json:"phase,omitempty" yaml:"phase,omitempty"`
	// AppliedSeq is the journal sequence of the event this action produced.
	// Set iff phase is Applied; it is the idempotency receipt that stops the
	// same action being journalled twice.
	// +optional
	AppliedSeq int64 `json:"appliedSeq,omitempty" yaml:"appliedSeq,omitempty"`
	// RejectionReason is a stable classifier when phase is Rejected.
	// +optional
	RejectionReason string `json:"rejectionReason,omitempty" yaml:"rejectionReason,omitempty"`
	// Message is human-facing detail. It is never parsed.
	// +optional
	Message string `json:"message,omitempty" yaml:"message,omitempty"`
	// Conditions follow standard Kubernetes conventions.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" yaml:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=gract
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Kind",type=string,JSONPath=`.spec.kind`
// +kubebuilder:printcolumn:name="Run",type=string,JSONPath=`.spec.target.runName`
// +kubebuilder:printcolumn:name="Occurrence",type=integer,JSONPath=`.spec.target.occurrenceSeq`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// GooberRunAction is an immutable, occurrence-bound, one-shot human
// intervention against a GooberRun.
type GooberRunAction struct {
	metav1.TypeMeta   `json:",inline" yaml:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty" yaml:"metadata,omitempty"`

	// +kubebuilder:validation:Required
	Spec GooberRunActionSpec `json:"spec" yaml:"spec"`
	// +optional
	Status GooberRunActionStatus `json:"status,omitempty" yaml:"status,omitempty"`
}

// +kubebuilder:object:root=true

// GooberRunActionList is a list of GooberRunAction objects.
type GooberRunActionList struct {
	metav1.TypeMeta `json:",inline" yaml:",inline"`
	metav1.ListMeta `json:"metadata,omitempty" yaml:"metadata,omitempty"`
	Items           []GooberRunAction `json:"items" yaml:"items"`
}
