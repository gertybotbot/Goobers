//go:build integration

package kuberunner_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	apiv1 "github.com/goobers/goobers/api/v1alpha1"
	"github.com/goobers/goobers/internal/operator"
	"github.com/goobers/goobers/internal/testdep"
)

// These run the generated CRDs against a REAL API server. Schema validation,
// CEL transition rules, and the status subresource are enforced by the
// apiserver, not by the Go types, so a fake client cannot check any of them: a
// spec-immutability rule that silently failed to generate would look perfectly
// healthy in every unit test in this package.
//
// This already earned its keep: it caught controller-gen rendering a bare ''
// in a CEL marker as a Unicode right double quote, which made the apiserver
// reject the whole GooberRunAction CRD at install time.
//
// Only the two runtime CRDs are installed, by explicit file. Installing the
// whole config/crd/bases directory would drag in unrelated pre-existing CRD
// defects (goobers.goobers.dev and workflows.goobers.dev currently exceed the
// apiserver's CEL cost budget), and this test must fail for problems in the
// runtime CRDs rather than for problems it does not own.
//
// Run via `make test-envtest`.

func startEnv(t *testing.T) client.Client {
	t.Helper()
	testdep.RequireEnv(t, "KUBEBUILDER_ASSETS")

	scheme, err := operator.NewScheme()
	if err != nil {
		t.Fatalf("scheme: %v", err)
	}

	bases := filepath.Join("..", "..", "config", "crd", "bases")
	env := &envtest.Environment{
		CRDInstallOptions: envtest.CRDInstallOptions{
			Paths: []string{
				filepath.Join(bases, "goobers.dev_gooberruns.yaml"),
				filepath.Join(bases, "goobers.dev_gooberrunactions.yaml"),
			},
			ErrorIfPathMissing: true,
		},
		Scheme: scheme,
	}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })

	k8s, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	return k8s
}

func validRun(name string) *apiv1.GooberRun {
	return &apiv1.GooberRun{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: apiv1.GooberRunSpec{
			RunID:       "run-" + name,
			Gaggle:      "web",
			JournalRoot: "/var/lib/goobers/runs",
			Workflow: apiv1.PinnedWorkflow{
				Name:    "ship",
				Version: 1,
				Digest:  "sha256:" + strings.Repeat("ab", 32),
			},
			Trigger: apiv1.RunTrigger{Kind: "manual"},
		},
	}
}

func TestIntegrationGooberRunCRDAcceptsAValidSpec(t *testing.T) {
	k8s := startEnv(t)
	ctx := context.Background()

	run := validRun("accepted")
	if err := k8s.Create(ctx, run); err != nil {
		t.Fatalf("create a valid GooberRun: %v", err)
	}

	// The status subresource must exist, or the projection could not be written
	// independently of the immutable spec.
	run.Status.Phase = apiv1.GooberRunRunning
	run.Status.ObservedSeq = 4
	if err := k8s.Status().Update(ctx, run); err != nil {
		t.Fatalf("update status: %v", err)
	}

	var got apiv1.GooberRun
	if err := k8s.Get(ctx, client.ObjectKeyFromObject(run), &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status.Phase != apiv1.GooberRunRunning || got.Status.ObservedSeq != 4 {
		t.Fatalf("status did not round-trip: %+v", got.Status)
	}
}

// Pinned identity must be unretunable in flight. This is enforced by a CEL
// transition rule, so only a real apiserver can prove it.
func TestIntegrationGooberRunSpecIsImmutable(t *testing.T) {
	k8s := startEnv(t)
	ctx := context.Background()

	run := validRun("immutable")
	if err := k8s.Create(ctx, run); err != nil {
		t.Fatalf("create: %v", err)
	}

	run.Spec.Workflow.Digest = "sha256:" + strings.Repeat("cd", 32)
	err := k8s.Update(ctx, run)
	if err == nil {
		t.Fatal("the apiserver accepted an edit to a pinned GooberRun spec")
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("update error = %v, want an Invalid (CEL) rejection", err)
	}
}

func TestIntegrationGooberRunCRDRejectsMalformedSpecs(t *testing.T) {
	k8s := startEnv(t)
	ctx := context.Background()

	for _, tc := range []struct {
		name   string
		mutate func(*apiv1.GooberRun)
	}{
		{"digest is not sha256", func(r *apiv1.GooberRun) {
			r.Spec.Workflow.Digest = "not-a-digest"
		}},
		{"workflow version below 1", func(r *apiv1.GooberRun) {
			r.Spec.Workflow.Version = 0
		}},
		{"unknown trigger kind", func(r *apiv1.GooberRun) {
			r.Spec.Trigger.Kind = "telepathy"
		}},
		{"empty journal root", func(r *apiv1.GooberRun) {
			r.Spec.JournalRoot = ""
		}},
		{"run id with a path separator", func(r *apiv1.GooberRun) {
			r.Spec.RunID = "../escape"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := validRun("bad-" + strings.ReplaceAll(tc.name, " ", "-"))
			tc.mutate(run)
			if err := k8s.Create(ctx, run); err == nil {
				t.Fatalf("the apiserver accepted an invalid spec (%s)", tc.name)
			}
		})
	}
}

// The action CRD's conditional requirements (a rationale for an override, a
// decision for a gate decision) are CEL rules too.
func TestIntegrationGooberRunActionCRDEnforcesItsRules(t *testing.T) {
	k8s := startEnv(t)
	ctx := context.Background()

	target := apiv1.ActionTarget{
		RunName:       "some-run",
		RunUID:        "uid-1",
		OccurrenceSeq: 7,
	}

	valid := &apiv1.GooberRunAction{
		ObjectMeta: metav1.ObjectMeta{Name: "decision", Namespace: "default"},
		Spec: apiv1.GooberRunActionSpec{
			Kind:     apiv1.ActionGateDecision,
			Actor:    "alice",
			Decision: "pass",
			Target:   target,
		},
	}
	if err := k8s.Create(ctx, valid); err != nil {
		t.Fatalf("create a valid action: %v", err)
	}

	// An immutable one-shot fact: editing it in place would rewrite history.
	valid.Spec.Decision = "fail"
	if err := k8s.Update(ctx, valid); err == nil {
		t.Fatal("the apiserver accepted an edit to a GooberRunAction spec")
	}

	for _, tc := range []struct {
		name   string
		action *apiv1.GooberRunAction
	}{
		{"override without a rationale", &apiv1.GooberRunAction{
			ObjectMeta: metav1.ObjectMeta{Name: "no-rationale", Namespace: "default"},
			Spec: apiv1.GooberRunActionSpec{
				Kind: apiv1.ActionGateOverride, Actor: "alice", Target: target,
			},
		}},
		{"gate decision without a decision", &apiv1.GooberRunAction{
			ObjectMeta: metav1.ObjectMeta{Name: "no-decision", Namespace: "default"},
			Spec: apiv1.GooberRunActionSpec{
				Kind: apiv1.ActionGateDecision, Actor: "alice", Target: target,
			},
		}},
		{"resume without a target state", &apiv1.GooberRunAction{
			ObjectMeta: metav1.ObjectMeta{Name: "no-target", Namespace: "default"},
			Spec: apiv1.GooberRunActionSpec{
				Kind: apiv1.ActionResumeFromTerminal, Actor: "alice", Target: target,
			},
		}},
		{"occurrence sequence below 1", &apiv1.GooberRunAction{
			ObjectMeta: metav1.ObjectMeta{Name: "zero-seq", Namespace: "default"},
			Spec: apiv1.GooberRunActionSpec{
				Kind: apiv1.ActionCancel, Actor: "alice",
				Target: apiv1.ActionTarget{RunName: "r", RunUID: "u", OccurrenceSeq: 0},
			},
		}},
		{"unknown action kind", &apiv1.GooberRunAction{
			ObjectMeta: metav1.ObjectMeta{Name: "bad-kind", Namespace: "default"},
			Spec: apiv1.GooberRunActionSpec{
				Kind: "Telepathy", Actor: "alice", Target: target,
			},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := k8s.Create(ctx, tc.action); err == nil {
				t.Fatalf("the apiserver accepted an invalid action (%s)", tc.name)
			}
		})
	}
}
