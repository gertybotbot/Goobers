package kuberunner

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	apiv1 "github.com/goobers/goobers/api/v1alpha1"
)

// The result-publication contract.
//
// A Kubernetes Job is EVIDENCE that an attempt ran. It is never authority for
// what the attempt decided. The gap between the two is closed here: a Job
// publishes a ResultReceipt, and the controller refuses to commit any journal
// transition until that receipt validates against the attempt it was dispatched
// for and against the bytes it claims to describe.
//
// This is deliberately a minimal shape: the receipt is a
// small JSON document the Job writes to a well-known path on a shared volume,
// and the controller reads back. It is an interface (ResultReader) rather than
// a hard-coded filesystem read so that a later slice can swap in an
// artifact-store or broker fetch without touching the reconciler.
//
// What makes it safe is not where the bytes live, it is that:
//
//  1. the receipt names the exact attempt (run UID, state, attempt number,
//     fencing epoch) it belongs to, so a stale Job's late receipt cannot be
//     mistaken for the current attempt's;
//  2. the envelope is digest-addressed — the receipt carries the sha256 of the
//     canonical envelope bytes, and the controller recomputes it;
//  3. every artifact pointer is digest-validated before it is journalled.
//
// A missing, malformed, misaddressed, or digest-mismatched receipt is a HARD
// STOP: the controller does not advance the machine. A successful Job with an
// invalid result is not a successful stage.

// ResultReceiptVersion is the receipt envelope version. It is separate from the
// stage contract version because the receipt is a transport wrapper, not the
// stage result itself.
const ResultReceiptVersion = "goobers.dev/kuberunner/result/v1alpha1"

// ResultFileName is the well-known file an attempt Job writes its receipt to,
// relative to the result mount.
const ResultFileName = "result.json"

// ResultMountPath is where the shared result volume is mounted inside an
// attempt Job's container.
const ResultMountPath = "/goobers/result"

// Errors the validator returns. They are distinct values so the reconciler can
// distinguish "not published yet" (wait) from "published and wrong" (fail
// closed) — conflating the two would either wedge a run forever or advance it
// on garbage.
var (
	// ErrResultMissing means no receipt has been published yet. It is not a
	// failure on its own: a Running Job simply has not finished.
	ErrResultMissing = errors.New("kuberunner: result receipt not published")
	// ErrResultMalformed means the receipt bytes are not a decodable receipt.
	ErrResultMalformed = errors.New("kuberunner: result receipt is malformed")
	// ErrResultMisaddressed means the receipt belongs to a different attempt.
	ErrResultMisaddressed = errors.New("kuberunner: result receipt targets a different attempt")
	// ErrResultDigestMismatch means the envelope bytes do not match the digest
	// the receipt commits to.
	ErrResultDigestMismatch = errors.New("kuberunner: result envelope digest mismatch")
	// ErrResultInvalid means the receipt decoded and addressed correctly but its
	// contents are not a usable stage result.
	ErrResultInvalid = errors.New("kuberunner: result receipt is invalid")
	// ErrResultConflict means the immutable result slot already contains a
	// different valid receipt. Publishers may repeat the same bytes after a
	// restart, but may never replace a previously published outcome.
	ErrResultConflict = errors.New("kuberunner: result receipt conflicts with an existing outcome")
)

// AttemptID names one executable attempt. It is the binding identity shared by
// the deterministic Job name, the dispatched journal event, and the published
// receipt — the three things that must agree before a transition commits.
type AttemptID struct {
	// RunUID is the Kubernetes UID of the GooberRun. A recreated CR is a
	// different occurrence and shares no attempts with its predecessor.
	RunUID string `json:"runUid"`
	// RunID is the canonical journal run identifier.
	RunID string `json:"runId"`
	// State is the workflow machine state being executed.
	State string `json:"state"`
	// Attempt is the 1-based attempt number within the state.
	Attempt int `json:"attempt"`
	// Branch is the parallel branch id (0 is the run's root branch).
	Branch int `json:"branch,omitempty"`
	// FenceEpoch is the monotonic business-claim epoch authorizing this worker.
	// It is zero only for runs that do not target a claimable provider item.
	FenceEpoch int64 `json:"fenceEpoch,omitempty"`
}

// Equal reports whether two attempt identities name the same attempt.
func (a AttemptID) Equal(b AttemptID) bool {
	return a.RunUID == b.RunUID &&
		a.RunID == b.RunID &&
		a.State == b.State &&
		a.Attempt == b.Attempt &&
		a.Branch == b.Branch &&
		a.FenceEpoch == b.FenceEpoch
}

// String renders the attempt for logs and error messages.
func (a AttemptID) String() string {
	return fmt.Sprintf("run=%s/%s state=%s branch=%d attempt=%d fence=%d", a.RunID, a.RunUID, a.State, a.Branch, a.Attempt, a.FenceEpoch)
}

// ResultReceipt is what an attempt Job publishes. Envelope holds the canonical
// JSON encoding of the apiv1.ResultEnvelope the stage produced; EnvelopeDigest
// commits to those exact bytes.
//
// The envelope travels as raw bytes rather than a decoded struct precisely so
// the digest is checkable: re-encoding a decoded struct is not guaranteed to
// reproduce the producer's bytes, and a digest you cannot reproduce is not a
// digest, it is decoration.
type ResultReceipt struct {
	// Schema is the receipt envelope version.
	Schema string `json:"schema"`
	// Attempt is the attempt this receipt reports. It MUST match the attempt
	// the controller dispatched.
	Attempt AttemptID `json:"attempt"`
	// EnvelopeDigest is the sha256 ("sha256:<64-hex>") of Envelope's bytes.
	EnvelopeDigest string `json:"envelopeDigest"`
	// Envelope is the canonical JSON encoding of the stage's ResultEnvelope.
	Envelope json.RawMessage `json:"envelope"`
}

// ResultReader fetches the raw receipt bytes an attempt published. It is the
// seam a later slice replaces with an artifact-store or broker client; the
// validation rules above it do not change.
type ResultReader interface {
	// ReadResult returns the receipt bytes for an attempt. It returns
	// ErrResultMissing (possibly wrapped) when nothing has been published.
	ReadResult(attempt AttemptID) ([]byte, error)
}

// ValidatedResult is a receipt that has passed every check. Only a value of
// this type may be journalled; it cannot be constructed except by
// ValidateResult, so "did we validate this?" is answered by the type system
// rather than by remembering to call a checker.
type ValidatedResult struct {
	// Attempt is the verified attempt identity.
	Attempt AttemptID
	// Envelope is the decoded stage result.
	Envelope apiv1.ResultEnvelope
	// EnvelopeDigest is the verified digest of the envelope bytes.
	EnvelopeDigest string
}

// ValidateResult parses and fully validates raw receipt bytes against the
// attempt the controller believes it dispatched.
//
// Order matters and is deliberate: address first, then digest, then content. A
// misaddressed receipt is rejected before its bytes are trusted enough to be
// digested, and the envelope is only decoded after its digest verifies.
func ValidateResult(want AttemptID, raw []byte) (ValidatedResult, error) {
	if len(raw) == 0 {
		return ValidatedResult{}, ErrResultMissing
	}

	var receipt ResultReceipt
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&receipt); err != nil {
		return ValidatedResult{}, fmt.Errorf("%w: %v", ErrResultMalformed, err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return ValidatedResult{}, fmt.Errorf("%w: trailing JSON value", ErrResultMalformed)
	}

	if receipt.Schema != ResultReceiptVersion {
		return ValidatedResult{}, fmt.Errorf("%w: unknown receipt schema %q (want %q)",
			ErrResultMalformed, receipt.Schema, ResultReceiptVersion)
	}

	// Address check. A stale Job from a previous attempt of the same state can
	// still be alive and can still write; without this it would look exactly
	// like the current attempt reporting success.
	if !receipt.Attempt.Equal(want) {
		return ValidatedResult{}, fmt.Errorf("%w: receipt is for %s, dispatched %s",
			ErrResultMisaddressed, receipt.Attempt, want)
	}

	if len(receipt.Envelope) == 0 {
		return ValidatedResult{}, fmt.Errorf("%w: receipt carries no envelope", ErrResultInvalid)
	}

	// Digest check against the exact published bytes.
	if err := validateDigestSyntax(receipt.EnvelopeDigest); err != nil {
		return ValidatedResult{}, fmt.Errorf("%w: %v", ErrResultMalformed, err)
	}
	if got := apiv1.Digest(receipt.Envelope); got != receipt.EnvelopeDigest {
		return ValidatedResult{}, fmt.Errorf("%w: computed %s, receipt claims %s",
			ErrResultDigestMismatch, got, receipt.EnvelopeDigest)
	}

	var envelope apiv1.ResultEnvelope
	if err := json.Unmarshal(receipt.Envelope, &envelope); err != nil {
		return ValidatedResult{}, fmt.Errorf("%w: envelope: %v", ErrResultMalformed, err)
	}
	if !envelope.Status.IsValid() {
		return ValidatedResult{}, fmt.Errorf("%w: unknown result status %q", ErrResultInvalid, envelope.Status)
	}

	// Every artifact pointer must be structurally valid BEFORE anything is
	// journalled. A pointer that escapes the journal root or carries a bad
	// digest is refused here rather than discovered later by a consumer.
	for i, ptr := range envelope.Artifacts {
		if err := ptr.Validate(); err != nil {
			return ValidatedResult{}, fmt.Errorf("%w: artifact %d (%q): %v", ErrResultInvalid, i, ptr.Path, err)
		}
	}
	if envelope.Transcript != nil {
		if err := envelope.Transcript.Validate(); err != nil {
			return ValidatedResult{}, fmt.Errorf("%w: transcript (%q): %v", ErrResultInvalid, envelope.Transcript.Path, err)
		}
	}

	return ValidatedResult{
		Attempt:        receipt.Attempt,
		Envelope:       envelope,
		EnvelopeDigest: receipt.EnvelopeDigest,
	}, nil
}

// EncodeReceipt builds the bytes an attempt Job should publish. It lives beside
// the validator on purpose: producer and consumer of a digest-addressed format
// must share one encoder, or the digest silently stops reproducing.
func EncodeReceipt(attempt AttemptID, envelope apiv1.ResultEnvelope) ([]byte, error) {
	body, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("kuberunner: encode result envelope: %w", err)
	}
	return json.Marshal(ResultReceipt{
		Schema:         ResultReceiptVersion,
		Attempt:        attempt,
		EnvelopeDigest: apiv1.Digest(body),
		Envelope:       body,
	})
}

// validateDigestSyntax checks the "sha256:<64-hex>" shape without touching any
// content.
func validateDigestSyntax(d string) error {
	const prefix = apiv1.DigestAlgo + ":"
	if len(d) != len(prefix)+64 || d[:len(prefix)] != prefix {
		return fmt.Errorf("digest %q is not %s<64-hex>", d, prefix)
	}
	for i := len(prefix); i < len(d); i++ {
		c := d[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return fmt.Errorf("digest %q has a non-hex character", d)
		}
	}
	return nil
}
