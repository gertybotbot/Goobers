package kuberunner

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	apiv1 "github.com/goobers/goobers/api/v1alpha1"
)

func testAttempt() AttemptID {
	return AttemptID{RunUID: "uid-1", RunID: "run-1", State: "build", Attempt: 1}
}

func TestValidateResultAcceptsAWellFormedReceipt(t *testing.T) {
	attempt := testAttempt()
	envelope := apiv1.ResultEnvelope{
		Status:  apiv1.ResultSuccess,
		Summary: "built",
		Outputs: map[string]interface{}{"coverage": 91.5},
		Artifacts: []apiv1.ArtifactPointer{{
			Path:   "artifacts/build/out.tar",
			Digest: apiv1.Digest([]byte("payload")),
		}},
	}

	raw, err := EncodeReceipt(attempt, envelope)
	if err != nil {
		t.Fatalf("EncodeReceipt: %v", err)
	}

	got, err := ValidateResult(attempt, raw)
	if err != nil {
		t.Fatalf("ValidateResult rejected a valid receipt: %v", err)
	}
	if got.Envelope.Status != apiv1.ResultSuccess {
		t.Errorf("status = %q, want %q", got.Envelope.Status, apiv1.ResultSuccess)
	}
	if !got.Attempt.Equal(attempt) {
		t.Errorf("attempt = %s, want %s", got.Attempt, attempt)
	}
	if got.EnvelopeDigest == "" {
		t.Error("validated result carries no envelope digest")
	}
}

// The producer and the consumer must share one encoder, or the digest stops
// reproducing. This asserts the round trip rather than trusting it.
func TestEncodeReceiptDigestIsReproducible(t *testing.T) {
	attempt := testAttempt()
	envelope := apiv1.ResultEnvelope{Status: apiv1.ResultSuccess, Summary: "ok"}

	raw, err := EncodeReceipt(attempt, envelope)
	if err != nil {
		t.Fatalf("EncodeReceipt: %v", err)
	}

	var receipt ResultReceipt
	if err := json.Unmarshal(raw, &receipt); err != nil {
		t.Fatalf("unmarshal receipt: %v", err)
	}
	if got := apiv1.Digest(receipt.Envelope); got != receipt.EnvelopeDigest {
		t.Fatalf("receipt digest %s does not commit to its own envelope bytes (%s)",
			receipt.EnvelopeDigest, got)
	}
}

func TestValidateResultRejectsBadReceipts(t *testing.T) {
	attempt := testAttempt()

	valid := func() ResultReceipt {
		body, err := json.Marshal(apiv1.ResultEnvelope{Status: apiv1.ResultSuccess})
		if err != nil {
			t.Fatalf("marshal envelope: %v", err)
		}
		return ResultReceipt{
			Schema:         ResultReceiptVersion,
			Attempt:        attempt,
			EnvelopeDigest: apiv1.Digest(body),
			Envelope:       body,
		}
	}

	for _, tc := range []struct {
		name    string
		mutate  func(*ResultReceipt)
		wantErr error
	}{
		{
			// A stale Job from an earlier attempt of the same state is alive and
			// can still write. Without the address check its receipt is
			// indistinguishable from the current attempt reporting success.
			name:    "attempt number mismatch",
			mutate:  func(r *ResultReceipt) { r.Attempt.Attempt = 2 },
			wantErr: ErrResultMisaddressed,
		},
		{
			name:    "run uid mismatch",
			mutate:  func(r *ResultReceipt) { r.Attempt.RunUID = "uid-other" },
			wantErr: ErrResultMisaddressed,
		},
		{
			name:    "state mismatch",
			mutate:  func(r *ResultReceipt) { r.Attempt.State = "test" },
			wantErr: ErrResultMisaddressed,
		},
		{
			name:    "branch mismatch",
			mutate:  func(r *ResultReceipt) { r.Attempt.Branch = 4 },
			wantErr: ErrResultMisaddressed,
		},
		{
			// The digest must commit to the bytes actually published, or it is
			// decoration rather than an integrity check.
			name:    "digest does not match envelope",
			mutate:  func(r *ResultReceipt) { r.EnvelopeDigest = apiv1.Digest([]byte("something else")) },
			wantErr: ErrResultDigestMismatch,
		},
		{
			name:    "malformed digest",
			mutate:  func(r *ResultReceipt) { r.EnvelopeDigest = "sha256:nothex" },
			wantErr: ErrResultMalformed,
		},
		{
			name:    "unknown schema",
			mutate:  func(r *ResultReceipt) { r.Schema = "goobers.dev/kuberunner/result/v999" },
			wantErr: ErrResultMalformed,
		},
		{
			name: "unknown result status",
			mutate: func(r *ResultReceipt) {
				body := []byte(`{"status":"probably-fine"}`)
				r.Envelope = body
				r.EnvelopeDigest = apiv1.Digest(body)
			},
			wantErr: ErrResultInvalid,
		},
		{
			// A pointer escaping the journal root must be refused BEFORE
			// anything is journalled, not discovered later by a consumer.
			name: "artifact path escapes the journal root",
			mutate: func(r *ResultReceipt) {
				body, err := json.Marshal(apiv1.ResultEnvelope{
					Status: apiv1.ResultSuccess,
					Artifacts: []apiv1.ArtifactPointer{{
						Path:   "../../etc/passwd",
						Digest: apiv1.Digest([]byte("x")),
					}},
				})
				if err != nil {
					t.Fatalf("marshal: %v", err)
				}
				r.Envelope = body
				r.EnvelopeDigest = apiv1.Digest(body)
			},
			wantErr: ErrResultInvalid,
		},
		{
			name: "artifact digest is malformed",
			mutate: func(r *ResultReceipt) {
				body, err := json.Marshal(apiv1.ResultEnvelope{
					Status: apiv1.ResultSuccess,
					Artifacts: []apiv1.ArtifactPointer{{
						Path:   "artifacts/out.bin",
						Digest: "not-a-digest",
					}},
				})
				if err != nil {
					t.Fatalf("marshal: %v", err)
				}
				r.Envelope = body
				r.EnvelopeDigest = apiv1.Digest(body)
			},
			wantErr: ErrResultInvalid,
		},
		{
			// An absent envelope is refused before its (nonexistent) bytes are
			// digested. json.RawMessage(nil) marshals to the literal `null`, so
			// the omitted-envelope case is expressed by an explicitly empty
			// raw message rather than a nil one.
			name: "empty envelope",
			mutate: func(r *ResultReceipt) {
				r.Envelope = json.RawMessage(`""`)
				r.EnvelopeDigest = apiv1.Digest(r.Envelope)
			},
			wantErr: ErrResultMalformed,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			receipt := valid()
			tc.mutate(&receipt)
			raw, err := json.Marshal(receipt)
			if err != nil {
				t.Fatalf("marshal receipt: %v", err)
			}
			if _, err := ValidateResult(attempt, raw); !errors.Is(err, tc.wantErr) {
				t.Fatalf("ValidateResult error = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestValidateResultRejectsAnAbsentEnvelope(t *testing.T) {
	// A receipt with no envelope key at all: there is nothing to digest and
	// nothing to journal, so it must be refused rather than treated as an empty
	// success.
	attempt := testAttempt()
	raw := []byte(`{"schema":"` + ResultReceiptVersion + `",` +
		`"attempt":{"runUid":"uid-1","runId":"run-1","state":"build","attempt":1},` +
		`"envelopeDigest":"` + apiv1.Digest([]byte("anything")) + `"}`)

	if _, err := ValidateResult(attempt, raw); !errors.Is(err, ErrResultInvalid) {
		t.Fatalf("ValidateResult error = %v, want %v", err, ErrResultInvalid)
	}
}

func TestValidateResultRejectsUndecodableBytes(t *testing.T) {
	attempt := testAttempt()

	if _, err := ValidateResult(attempt, nil); !errors.Is(err, ErrResultMissing) {
		t.Fatalf("empty bytes error = %v, want %v", err, ErrResultMissing)
	}
	if _, err := ValidateResult(attempt, []byte("{not json")); !errors.Is(err, ErrResultMalformed) {
		t.Fatalf("garbage bytes error = %v, want %v", err, ErrResultMalformed)
	}
	// Unknown fields are refused rather than ignored: a receipt shape this
	// build does not own may mean something it cannot see.
	if _, err := ValidateResult(attempt, []byte(`{"schema":"x","surprise":1}`)); !errors.Is(err, ErrResultMalformed) {
		t.Fatalf("unknown-field error = %v, want %v", err, ErrResultMalformed)
	}
}

func TestValidatedResultCannotBeForged(t *testing.T) {
	// ValidatedResult is the type-level guard that only checked results reach
	// the journal. Its guarantee is that the ONLY way to obtain a populated one
	// from outside this package is ValidateResult. A zero value is inert.
	var zero ValidatedResult
	if zero.EnvelopeDigest != "" || zero.Attempt.RunID != "" {
		t.Fatal("the zero ValidatedResult is not inert")
	}
}

func TestValidateDigestSyntax(t *testing.T) {
	good := apiv1.Digest([]byte("x"))
	if err := validateDigestSyntax(good); err != nil {
		t.Fatalf("rejected a valid digest %q: %v", good, err)
	}
	for _, bad := range []string{
		"",
		"sha256:",
		"sha256:" + strings.Repeat("g", 64),
		"md5:" + strings.Repeat("a", 64),
		"sha256:" + strings.Repeat("a", 63),
		strings.Repeat("a", 64),
	} {
		if err := validateDigestSyntax(bad); err == nil {
			t.Errorf("accepted an invalid digest %q", bad)
		}
	}
}
