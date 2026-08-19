package kuberunner

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	apiv1 "github.com/goobers/goobers/api/v1alpha1"
)

// FSResultReader reads attempt receipts from a shared filesystem.
//
// This is the deliberately minimal transport for this slice: attempts publish
// to a per-attempt path under a root the controller can also read. It is a
// transport, not a trust boundary — nothing here decides whether a result is
// acceptable. All of that lives in ValidateResult, which runs over whatever
// bytes this returns, so replacing this with an object-store or broker client
// later changes no correctness property.
type FSResultReader struct {
	// Root is the directory holding published results.
	Root string
}

// NewFSResultReader builds a reader over root.
func NewFSResultReader(root string) *FSResultReader { return &FSResultReader{Root: root} }

// ResultPath returns the path an attempt publishes to. It is derived from the
// same deterministic attempt identity as the Job name, so the controller can
// locate a result without having recorded anything: after a crash the path is
// recomputable from durable journal facts alone.
func (f *FSResultReader) ResultPath(attempt AttemptID) (string, error) {
	if !apiv1.ValidRunID(attempt.RunID) {
		return "", fmt.Errorf("kuberunner: invalid run id %q", attempt.RunID)
	}
	// JobName is a pure function of the attempt and is always a safe single
	// path segment (lowercase alphanumerics and dashes), so it cannot traverse.
	return filepath.Join(f.Root, attempt.RunID, JobName(attempt), ResultFileName), nil
}

// ReadResult returns the published receipt bytes for an attempt.
func (f *FSResultReader) ReadResult(attempt AttemptID) ([]byte, error) {
	path, err := f.ResultPath(attempt)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrResultMissing, attempt.String())
		}
		return nil, fmt.Errorf("kuberunner: read result: %w", err)
	}
	if len(data) == 0 {
		// A zero-length file is a half-finished publish, not a result. Treating
		// it as missing lets the next reconcile retry instead of failing the
		// attempt on a partially written file.
		return nil, fmt.Errorf("%w: %s (empty file)", ErrResultMissing, attempt.String())
	}
	return data, nil
}

// PublishResult writes an immutable receipt for an attempt. Re-publishing the
// identical deterministic outcome is idempotent; replacing it is refused. A
// hard link installs fully-written bytes without the rename-overwrite window
// that would let two workers race different outcomes into the same slot.
func (f *FSResultReader) PublishResult(attempt AttemptID, envelope apiv1.ResultEnvelope) error {
	path, err := f.ResultPath(attempt)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("kuberunner: create result dir: %w", err)
	}
	data, err := EncodeReceipt(attempt, envelope)
	if err != nil {
		return err
	}
	if err := samePublishedResult(path, data); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	tmp, err := os.CreateTemp(filepath.Dir(path), ".result-*.tmp")
	if err != nil {
		return fmt.Errorf("kuberunner: create result temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	if err := tmp.Chmod(0o644); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("kuberunner: chmod result temp file: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("kuberunner: write result: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("kuberunner: sync result: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("kuberunner: close result: %w", err)
	}
	if err := os.Link(tmpPath, path); err != nil {
		if errors.Is(err, os.ErrExist) {
			return samePublishedResult(path, data)
		}
		return fmt.Errorf("kuberunner: publish result: %w", err)
	}
	return nil
}

func samePublishedResult(path string, want []byte) error {
	got, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if !bytes.Equal(got, want) {
		return ErrResultConflict
	}
	return nil
}
