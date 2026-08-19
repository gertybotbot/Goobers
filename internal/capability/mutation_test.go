package capability

import "testing"

func TestMutatesExternalStateClassifiesEveryCanonicalCapability(t *testing.T) {
	wantMutation := map[Capability]bool{
		RepoPush:          true,
		GitHubIssuesWrite: true, GitHubMilestonesWrite: true, GitHubIssuesApprove: true,
		ProviderPRWrite: true, GitHubPRWrite: true, GitHubPRReview: true,
		GitHubBranchDelete: true, GitHubPRMerge: true,
		ADOPRComment: true, ADOPRWrite: true, ADOPRStatus: true, ADOWorkItemsWrite: true,
	}
	for _, capability := range All() {
		if got := MutatesExternalState(string(capability)); got != wantMutation[capability] {
			t.Errorf("MutatesExternalState(%q) = %t, want %t", capability, got, wantMutation[capability])
		}
	}
	if MutatesExternalState("unknown:write") {
		t.Fatal("an unknown capability was classified as an admitted mutation grant")
	}
}
