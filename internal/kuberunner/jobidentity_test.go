package kuberunner

import (
	"strings"
	"testing"
)

// Deterministic Job identity is the property the whole crash-safety argument
// rests on: the controller appends dispatch intent BEFORE creating a Job, so
// after a crash it must be able to recompute the Job's name from durable facts
// alone. If the name were not a pure function of the attempt, that recomputation
// would produce a second Job for one attempt — two executions sharing one
// journal identity.

func TestJobNameIsDeterministic(t *testing.T) {
	attempt := AttemptID{
		RunUID:  "8f14e45f-ceea-467a-9c1e-1c2f3d4b5a60",
		RunID:   "run-abc123",
		State:   "build",
		Attempt: 1,
	}

	first := JobName(attempt)
	for i := 0; i < 100; i++ {
		if got := JobName(attempt); got != first {
			t.Fatalf("JobName is not deterministic: call %d gave %q, first gave %q", i, got, first)
		}
	}

	// A separately-constructed but equal attempt must also agree: determinism
	// across processes, not just across calls on one value.
	twin := AttemptID{
		RunUID:  "8f14e45f-ceea-467a-9c1e-1c2f3d4b5a60",
		RunID:   "run-abc123",
		State:   "build",
		Attempt: 1,
	}
	if got := JobName(twin); got != first {
		t.Fatalf("equal attempts produced different names: %q vs %q", got, first)
	}
}

func TestJobNameSeparatesDistinctAttempts(t *testing.T) {
	base := AttemptID{RunUID: "uid-1", RunID: "run-1", State: "build", Attempt: 1}

	for _, tc := range []struct {
		name  string
		mutit func(AttemptID) AttemptID
	}{
		{"different run uid", func(a AttemptID) AttemptID { a.RunUID = "uid-2"; return a }},
		{"different state", func(a AttemptID) AttemptID { a.State = "test"; return a }},
		{"different attempt", func(a AttemptID) AttemptID { a.Attempt = 2; return a }},
		{"different branch", func(a AttemptID) AttemptID { a.Branch = 3; return a }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, want := JobName(tc.mutit(base)), JobName(base); got == want {
				t.Fatalf("%s collided with the base attempt at %q", tc.name, got)
			}
		})
	}
}

// A deleted-and-recreated CR is a different occurrence and must not inherit its
// predecessor's Jobs. Because the name is derived from the Kubernetes UID
// rather than the run id, that separation is automatic.
func TestJobNameSeparatesRecreatedRunCR(t *testing.T) {
	before := AttemptID{RunUID: "uid-before", RunID: "run-1", State: "build", Attempt: 1}
	after := AttemptID{RunUID: "uid-after", RunID: "run-1", State: "build", Attempt: 1}
	if JobName(before) == JobName(after) {
		t.Fatal("a recreated GooberRun reused its predecessor's Job name")
	}
}

func TestJobNameIsKubernetesLegal(t *testing.T) {
	for _, tc := range []struct {
		name    string
		attempt AttemptID
	}{
		{"simple", AttemptID{RunUID: "uid-1", RunID: "run-1", State: "build", Attempt: 1}},
		{"branch", AttemptID{RunUID: "uid-1", RunID: "run-1", State: "build", Attempt: 7, Branch: 4}},
		{"long state", AttemptID{
			RunUID:  "uid-1",
			RunID:   "run-1",
			State:   strings.Repeat("extremely-long-state-name-", 20),
			Attempt: 12,
		}},
		{"unicode state", AttemptID{RunUID: "uid-1", RunID: "run-1", State: "ビルド/段階 #1", Attempt: 1}},
		{"uppercase state", AttemptID{RunUID: "uid-1", RunID: "run-1", State: "Build_Stage", Attempt: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := JobName(tc.attempt)
			if len(name) > 63 {
				t.Fatalf("job name %q is %d chars, over the 63-char label limit", name, len(name))
			}
			if !isDNS1123Subdomain(name) {
				t.Fatalf("job name %q is not a legal DNS-1123 subdomain", name)
			}
		})
	}
}

// Author-controlled state names must never leak into a label value unchecked.
func TestAttemptLabelsOmitUnsafeValues(t *testing.T) {
	attempt := AttemptID{RunUID: "uid-1", RunID: "run-1", State: "ビルド #1", Attempt: 1}
	labels := AttemptLabels("my-gaggle", attempt)

	if _, present := labels[LabelState]; present {
		t.Fatalf("an unsafe state name was written to %s: %q", LabelState, labels[LabelState])
	}
	// The hash is always present and always safe, which is why the reconciler
	// matches on it rather than on the raw name.
	if got := labels[LabelStateHash]; got != StateHash(attempt.State) {
		t.Fatalf("state hash label = %q, want %q", got, StateHash(attempt.State))
	}
	for key, value := range labels {
		if !isLabelValue(value) {
			t.Fatalf("label %s has an illegal value %q", key, value)
		}
	}
}

func TestAttemptLabelsKeepSafeStateNames(t *testing.T) {
	attempt := AttemptID{RunUID: "uid-1", RunID: "run-1", State: "build", Attempt: 2, Branch: 1}
	labels := AttemptLabels("my-gaggle", attempt)

	for key, want := range map[string]string{
		LabelManagedBy: ManagedByValue,
		LabelRunUID:    "uid-1",
		LabelRunID:     "run-1",
		LabelGaggle:    "my-gaggle",
		LabelState:     "build",
		LabelBranch:    "1",
		LabelAttempt:   "2",
	} {
		if got := labels[key]; got != want {
			t.Errorf("label %s = %q, want %q", key, got, want)
		}
	}
}

// isDNS1123Subdomain is a local check so the test does not depend on the
// validation package's behaviour to state its own requirement.
func isDNS1123Subdomain(s string) bool {
	if s == "" || len(s) > 253 {
		return false
	}
	for _, label := range strings.Split(s, ".") {
		if label == "" {
			return false
		}
		if !isLowerAlphanumeric(label[0]) || !isLowerAlphanumeric(label[len(label)-1]) {
			return false
		}
		for i := 0; i < len(label); i++ {
			if c := label[i]; !isLowerAlphanumeric(c) && c != '-' {
				return false
			}
		}
	}
	return true
}

func isLowerAlphanumeric(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z')
}
