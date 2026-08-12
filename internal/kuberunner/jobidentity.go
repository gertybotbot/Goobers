package kuberunner

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// Deterministic Job identity.
//
// This is the single mechanism that makes attempt dispatch crash-safe. The
// controller appends its dispatch intent to the journal BEFORE creating the
// Job, so a crash can land between the two. On restart the controller must be
// able to ask "did I already create the Job for this exact attempt?" and get a
// correct answer without a lookup table, a finalizer, or a stored name it may
// never have managed to persist.
//
// It gets that answer by deriving the name from facts that are already durable:
// the run's UID, the machine state, the branch, and the attempt number. The
// same attempt always yields the same name, and a different attempt never does.
// Creation then relies on the API server's own uniqueness guarantee: a second
// create of the same name returns AlreadyExists, which is success, not an error
// to retry around.
//
// The properties the name must have, in priority order:
//
//  1. DETERMINISTIC — same attempt, same name, on any replica, after any crash.
//  2. COLLISION-RESISTANT — two different attempts must not collide, including
//     across runs that reuse a state name and across a deleted-and-recreated CR
//     (hence run UID, not run ID).
//  3. VALID — a DNS-1123 subdomain within Kubernetes' 63-character limit for
//     the derived Pod label values.
//
// Human legibility is explicitly NOT a goal above those three. State names are
// author-controlled free text and can be long, non-ASCII, or nearly identical
// after truncation, so the state contributes through a hash rather than a
// prefix. The run and attempt are recoverable from labels, which is where
// operators should look.

const (
	// JobNamePrefix marks controller-owned attempt Jobs. Short, because the
	// 63-character budget is tight.
	JobNamePrefix = "gr"

	// hashLen is the hex characters taken from each SHA-256. 10 hex characters
	// is 40 bits; with the run and state hashed separately the pair gives 80
	// bits of separation, which is far beyond the collision pressure of one
	// controller's attempt namespace.
	hashLen = 10
)

// Label keys projected onto every attempt Job. Operators and the reconciler
// both use these to map a Job back to its run without parsing the name.
const (
	// LabelManagedBy marks objects this controller owns.
	LabelManagedBy = "app.kubernetes.io/managed-by"
	// LabelRunUID carries the GooberRun UID.
	LabelRunUID = "goobers.dev/run-uid"
	// LabelRunID carries the canonical journal run id.
	LabelRunID = "goobers.dev/run-id"
	// LabelGaggle carries the owning gaggle.
	LabelGaggle = "goobers.dev/gaggle"
	// LabelState carries the workflow state, when it is label-safe.
	LabelState = "goobers.dev/state"
	// LabelStateHash carries the state hash, which is always label-safe and is
	// what the reconciler matches on.
	LabelStateHash = "goobers.dev/state-hash"
	// LabelBranch carries the parallel branch id.
	LabelBranch = "goobers.dev/branch"
	// LabelAttempt carries the attempt number.
	LabelAttempt = "goobers.dev/attempt"

	// ManagedByValue identifies this controller in LabelManagedBy.
	ManagedByValue = "goobers-kuberunner"
)

// JobName returns the deterministic Job name for an attempt.
//
// Shape: gr-<runhash>-<statehash>-a<attempt>, or with a non-root branch,
// gr-<runhash>-<statehash>-b<branch>-a<attempt>.
//
// The branch segment is omitted on the root branch so the common non-parallel
// case stays as short as possible; branch 0 and "no branch" are the same thing,
// so this omission cannot make two distinct attempts collide.
func JobName(attempt AttemptID) string {
	runHash := shortHash(attempt.RunUID)
	stateHash := shortHash(attempt.State)
	if attempt.Branch == 0 {
		return fmt.Sprintf("%s-%s-%s-a%d", JobNamePrefix, runHash, stateHash, attempt.Attempt)
	}
	return fmt.Sprintf("%s-%s-%s-b%d-a%d", JobNamePrefix, runHash, stateHash, attempt.Branch, attempt.Attempt)
}

// StateHash returns the label-safe hash of a workflow state name. It is
// exported because the reconciler selects Jobs by it: the raw state name may be
// unusable as a label value, but its hash always is.
func StateHash(state string) string { return shortHash(state) }

// shortHash returns the first hashLen hex characters of the SHA-256 of s.
func shortHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:hashLen]
}

// AttemptLabels builds the label set for an attempt Job.
//
// The state name is included verbatim ONLY when it is already a legal label
// value; otherwise the key is omitted entirely rather than silently mangled. A
// truncated or character-substituted label looks authoritative and is not, and
// the reconciler never depends on it — LabelStateHash is the machine-readable
// one, LabelState is a human convenience.
func AttemptLabels(gaggle string, attempt AttemptID) map[string]string {
	labels := map[string]string{
		LabelManagedBy: ManagedByValue,
		LabelRunUID:    attempt.RunUID,
		LabelStateHash: StateHash(attempt.State),
		LabelBranch:    fmt.Sprintf("%d", attempt.Branch),
		LabelAttempt:   fmt.Sprintf("%d", attempt.Attempt),
	}
	if isLabelValue(attempt.RunID) {
		labels[LabelRunID] = attempt.RunID
	}
	if isLabelValue(gaggle) {
		labels[LabelGaggle] = gaggle
	}
	if isLabelValue(attempt.State) {
		labels[LabelState] = attempt.State
	}
	return labels
}

// isLabelValue reports whether s is a legal Kubernetes label value: at most 63
// characters, alphanumeric at both ends, with dashes, underscores, and dots
// permitted inside. An empty string is legal as a label value but carries no
// information, so it is refused here.
func isLabelValue(s string) bool {
	if s == "" || len(s) > 63 {
		return false
	}
	if !isAlphanumeric(s[0]) || !isAlphanumeric(s[len(s)-1]) {
		return false
	}
	return strings.IndexFunc(s, func(r rune) bool {
		return !(r == '-' || r == '_' || r == '.' ||
			(r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z'))
	}) < 0
}

func isAlphanumeric(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}
