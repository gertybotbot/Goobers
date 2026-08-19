package kuberunner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Business claims are durable ownership of provider work. They are deliberately
// NOT coordination.k8s.io Leases: leader-election leases answer whether a
// controller process is live, while these records answer which run may mutate a
// provider item. Released records are retained so fencing epochs never regress.
const (
	claimRecordSchema = "goobers.dev/kuberunner/claim/v1alpha1"
	claimDataKey      = "claim.json"
	claimLabel        = "goobers.dev/business-claim"
	defaultClaimTTL   = 2 * time.Minute
	// DefaultClaimNamespace is the single cluster authority namespace used by
	// the operator unless explicitly configured otherwise.
	DefaultClaimNamespace = "goobers-system"
)

var (
	ErrClaimHeld       = errors.New("kuberunner: business claim is held by another run")
	ErrClaimFenceLost  = errors.New("kuberunner: business claim fence is no longer current")
	ErrClaimContention = errors.New("kuberunner: business claim changed concurrently")
)

// ClaimKey is the provider-scoped business identity. External ids are only
// unique inside (gaggle, provider), so all three components are load-bearing.
type ClaimKey struct {
	Gaggle     string `json:"gaggle"`
	Provider   string `json:"provider"`
	ExternalID string `json:"externalId"`
}

func (k ClaimKey) String() string { return k.Gaggle + "/" + k.Provider + "/" + k.ExternalID }

func (k ClaimKey) valid() bool { return k.Gaggle != "" && k.Provider != "" && k.ExternalID != "" }

// ClaimToken is the monotonic fencing authority granted to one run occurrence.
// Epoch is never reused, including after release or expiry.
type ClaimToken struct {
	Key       ClaimKey  `json:"key"`
	RunID     string    `json:"runId"`
	RunUID    string    `json:"runUid"`
	Epoch     int64     `json:"epoch"`
	ExpiresAt time.Time `json:"expiresAt"`
}

func (t ClaimToken) Equal(other ClaimToken) bool {
	return t.Key == other.Key && t.RunID == other.RunID && t.RunUID == other.RunUID && t.Epoch == other.Epoch
}

// ClaimStore is the unsafe-seam fencing interface. Acquire is idempotent for
// the same run occurrence. AssertCurrent and Release fail closed for stale
// epochs; no caller may infer authority merely from a previously held token.
type ClaimStore interface {
	Acquire(context.Context, string, ClaimKey, string, string, time.Duration) (ClaimToken, error)
	Renew(context.Context, string, ClaimToken, time.Duration) (ClaimToken, error)
	AssertCurrent(context.Context, string, ClaimToken) error
	Release(context.Context, string, ClaimToken) error
}

type claimRecord struct {
	Schema    string    `json:"schema"`
	Key       ClaimKey  `json:"key"`
	RunID     string    `json:"runId,omitempty"`
	RunUID    string    `json:"runUid,omitempty"`
	Epoch     int64     `json:"epoch"`
	ExpiresAt time.Time `json:"expiresAt,omitempty"`
	Released  bool      `json:"released,omitempty"`
}

// KubeClaimStore stores one retained, resourceVersion-CAS'd ConfigMap per
// business key. ConfigMaps are durable records here, not liveness Leases.
type KubeClaimStore struct {
	Client client.Client
	// Namespace fixes all records in one authority namespace. If empty, the
	// caller's namespace is used (useful for isolated tests/embedders).
	Namespace string
	Now       func() time.Time
}

func (s *KubeClaimStore) namespace(callerNamespace string) string {
	if s.Namespace != "" {
		return s.Namespace
	}
	return callerNamespace
}

func (s *KubeClaimStore) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

func claimObjectName(key ClaimKey) string {
	sum := sha256.Sum256([]byte(key.String()))
	return "goobers-claim-" + hex.EncodeToString(sum[:12])
}

func (s *KubeClaimStore) Acquire(ctx context.Context, namespace string, key ClaimKey, runID, runUID string, ttl time.Duration) (ClaimToken, error) {
	if !key.valid() || runID == "" || runUID == "" {
		return ClaimToken{}, fmt.Errorf("kuberunner: invalid business claim identity")
	}
	if ttl <= 0 {
		ttl = defaultClaimTTL
	}
	now := s.now()
	namespace = s.namespace(namespace)
	name := claimObjectName(key)
	var cm corev1.ConfigMap
	err := s.Client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &cm)
	if apierrors.IsNotFound(err) {
		record := claimRecord{Schema: claimRecordSchema, Key: key, RunID: runID, RunUID: runUID, Epoch: 1, ExpiresAt: now.Add(ttl)}
		created := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: map[string]string{claimLabel: "true"}}}
		if err := setClaimRecord(created, record); err != nil {
			return ClaimToken{}, err
		}
		if err := s.Client.Create(ctx, created); err != nil {
			if apierrors.IsAlreadyExists(err) {
				return ClaimToken{}, ErrClaimContention
			}
			return ClaimToken{}, fmt.Errorf("create business claim: %w", err)
		}
		return tokenFromRecord(record), nil
	}
	if err != nil {
		return ClaimToken{}, fmt.Errorf("read business claim: %w", err)
	}
	record, err := getClaimRecord(&cm)
	if err != nil {
		return ClaimToken{}, err
	}
	if record.Key != key {
		return ClaimToken{}, fmt.Errorf("kuberunner: claim hash collision for %s", key)
	}
	if !record.Released && record.ExpiresAt.After(now) {
		if record.RunID != runID || record.RunUID != runUID {
			return ClaimToken{}, fmt.Errorf("%w: %s", ErrClaimHeld, record.RunID)
		}
		// Same occurrence: acquisition after a controller restart is renewal,
		// never a new epoch.
		record.ExpiresAt = now.Add(ttl)
	} else {
		record.Epoch++
		record.RunID, record.RunUID = runID, runUID
		record.ExpiresAt, record.Released = now.Add(ttl), false
	}
	if err := setClaimRecord(&cm, record); err != nil {
		return ClaimToken{}, err
	}
	if err := s.Client.Update(ctx, &cm); err != nil {
		if apierrors.IsConflict(err) {
			return ClaimToken{}, ErrClaimContention
		}
		return ClaimToken{}, fmt.Errorf("update business claim: %w", err)
	}
	return tokenFromRecord(record), nil
}

func (s *KubeClaimStore) Renew(ctx context.Context, namespace string, token ClaimToken, ttl time.Duration) (ClaimToken, error) {
	if ttl <= 0 {
		ttl = defaultClaimTTL
	}
	cm, record, err := s.current(ctx, s.namespace(namespace), token)
	if err != nil {
		return ClaimToken{}, err
	}
	record.ExpiresAt = s.now().Add(ttl)
	if err := setClaimRecord(cm, record); err != nil {
		return ClaimToken{}, err
	}
	if err := s.Client.Update(ctx, cm); err != nil {
		if apierrors.IsConflict(err) {
			return ClaimToken{}, ErrClaimContention
		}
		return ClaimToken{}, fmt.Errorf("renew business claim: %w", err)
	}
	return tokenFromRecord(record), nil
}

func (s *KubeClaimStore) AssertCurrent(ctx context.Context, namespace string, token ClaimToken) error {
	_, _, err := s.current(ctx, s.namespace(namespace), token)
	return err
}

func (s *KubeClaimStore) Release(ctx context.Context, namespace string, token ClaimToken) error {
	namespace = s.namespace(namespace)
	var cm corev1.ConfigMap
	if err := s.Client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: claimObjectName(token.Key)}, &cm); err != nil {
		if apierrors.IsNotFound(err) {
			return ErrClaimFenceLost
		}
		return fmt.Errorf("read business claim: %w", err)
	}
	record, err := getClaimRecord(&cm)
	if err != nil {
		return err
	}
	if record.Key != token.Key || record.Epoch != token.Epoch {
		return ErrClaimFenceLost
	}
	// A crash after durable release but before claim.released reaches the run
	// journal retries here. The retained epoch makes that retry recognisable.
	if record.Released {
		return nil
	}
	if record.RunID != token.RunID || record.RunUID != token.RunUID || !record.ExpiresAt.After(s.now()) {
		return ErrClaimFenceLost
	}
	record.Released, record.RunID, record.RunUID = true, "", ""
	record.ExpiresAt = time.Time{}
	if err := setClaimRecord(&cm, record); err != nil {
		return err
	}
	if err := s.Client.Update(ctx, &cm); err != nil {
		if apierrors.IsConflict(err) {
			return ErrClaimContention
		}
		return fmt.Errorf("release business claim: %w", err)
	}
	return nil
}

func (s *KubeClaimStore) current(ctx context.Context, namespace string, token ClaimToken) (*corev1.ConfigMap, claimRecord, error) {
	var cm corev1.ConfigMap
	if err := s.Client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: claimObjectName(token.Key)}, &cm); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, claimRecord{}, ErrClaimFenceLost
		}
		return nil, claimRecord{}, fmt.Errorf("read business claim: %w", err)
	}
	record, err := getClaimRecord(&cm)
	if err != nil {
		return nil, claimRecord{}, err
	}
	if record.Released || !record.ExpiresAt.After(s.now()) || record.Key != token.Key || record.RunID != token.RunID || record.RunUID != token.RunUID || record.Epoch != token.Epoch {
		return nil, claimRecord{}, ErrClaimFenceLost
	}
	return &cm, record, nil
}

func tokenFromRecord(r claimRecord) ClaimToken {
	return ClaimToken{Key: r.Key, RunID: r.RunID, RunUID: r.RunUID, Epoch: r.Epoch, ExpiresAt: r.ExpiresAt}
}

func setClaimRecord(cm *corev1.ConfigMap, record claimRecord) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode business claim: %w", err)
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data[claimDataKey] = string(raw)
	return nil
}

func getClaimRecord(cm *corev1.ConfigMap) (claimRecord, error) {
	var record claimRecord
	if err := json.Unmarshal([]byte(cm.Data[claimDataKey]), &record); err != nil {
		return record, fmt.Errorf("decode business claim: %w", err)
	}
	if record.Schema != claimRecordSchema || record.Epoch < 1 || !record.Key.valid() {
		return record, fmt.Errorf("kuberunner: invalid business claim record")
	}
	return record, nil
}
