package main

import (
	"context"
	"time"

	"github.com/goobers/goobers/providers"
)

// mergeProvider is the forge surface merge-pr uses directly: its own
// pre-merge reads (PollPullRequest for live head/base + CI state,
// CompareCommits for the delta-aware SHA-pin check, PullRequestFiles for
// base-movement intersection) plus the post-merge branch cleanup
// (ListPullRequests to detect stacked children, DeleteBranch to remove the
// merged head).
//
// merge-pr previously built *providers.GitHubProvider unconditionally, so on a
// Gitea-routed repo every one of these reads went to api.github.com and the
// stage failed 401 immediately after apply-verdict had already published a
// passing verdict — a green review that could never become a merge.
//
// The capability-gated landing calls (MergePullRequest, DetectMergePolicy,
// EnqueuePullRequest) are NOT here: those go through providers.Dispatcher,
// which refuses an undeclared capability before the backend sees the call.
// Dispatcher takes providers.Provider, which both backends satisfy.
type mergeProvider interface {
	providers.Provider
	providers.CommitComparer
	providers.BranchDeleter
	// AuthenticatedLogin lets a mergeProvider satisfy remediationProvider, which
	// classifyRemoteTutorChanges (reached from merge-pr's tutor-branch path)
	// takes. Both backends implement it.
	AuthenticatedLogin(ctx context.Context) (string, error)
	ListComments(ctx context.Context, repo providers.RepositoryRef, id string) ([]providers.Comment, error)
	UpdateComment(ctx context.Context, repo providers.RepositoryRef, commentID, body string) error
	DeleteComment(ctx context.Context, repo providers.RepositoryRef, commentID string) error
	GetPullRequest(ctx context.Context, repo providers.RepositoryRef, pullID string) (providers.PullRequestSummary, error)
	GetWorkItem(ctx context.Context, repo providers.RepositoryRef, id string) (providers.WorkItem, error)
	UpdateWorkItem(ctx context.Context, req providers.UpdateWorkItemRequest) (providers.WorkItem, error)
	UpdateWorkItemStatus(ctx context.Context, req providers.UpdateWorkItemStatusRequest) (providers.WorkItem, error)
	RepositoryFileContent(ctx context.Context, repo providers.RepositoryRef, path, ref string) ([]byte, error)
	PullRequestMergeable(ctx context.Context, repo providers.RepositoryRef, pullID string) (*bool, error)
	RefCheckState(ctx context.Context, repo providers.RepositoryRef, ref string) (providers.CheckState, error)
	RefCheckStates(ctx context.Context, repo providers.RepositoryRef, refs []string) (map[string]providers.CheckState, error)
	ListRecentlyClosedPullRequests(ctx context.Context, req providers.ListPullRequestsRequest, updatedSince time.Time) ([]providers.PullRequestSummary, error)
	BranchTipSHA(ctx context.Context, repo providers.RepositoryRef, branch string) (string, error)
	UpdateBranch(ctx context.Context, req providers.UpdateBranchRequest) (providers.UpdateBranchResult, error)
	CIFailures(ctx context.Context, repo providers.RepositoryRef, ref string) ([]providers.CIFailureDetail, error)
	ListPullRequestReviewThreads(ctx context.Context, repo providers.RepositoryRef, pullID string) (providers.PullRequestReviewThreads, error)
	SubmitPullRequestReview(ctx context.Context, req providers.PullRequestReviewRequest) (providers.PullRequestReviewResult, error)
}

// Compile-time proof both backends satisfy the surface.
var (
	_ mergeProvider = (*providers.GitHubProvider)(nil)
	_ mergeProvider = (*providers.GiteaProvider)(nil)
)

// mergeStageProvider builds merge-pr's provider for the routed repository,
// dispatching on the repo's own provider kind the way open-pr and
// remediationStageProvider already do. The GitHub arm keeps the conditional-GET
// read cache (a GitHub HTTPClient decorator); the Gitea arm stays uncached,
// matching every other Gitea arm in the tree.
func mergeStageProvider(root string, repo providers.RepositoryRef, token string) (mergeProvider, error) {
	return mergeStageProviderWithRecorder(root, repo, token, sidecarMutationRecorder{kind: "pr"})
}

// mergeStageProviderWithRecorder is mergeStageProvider with an explicit journal
// mutation recorder. The post-merge branch delete must record kind="branch",
// not kind="pr": the run journal's mutation facts are how an operator tells a
// merge from the branch cleanup that followed it, and collapsing both onto the
// PR recorder loses that distinction.
func mergeStageProviderWithRecorder(root string, repo providers.RepositoryRef, token string, recorder providers.MutationRecorder) (mergeProvider, error) {
	switch repo.Provider {
	case providers.ProviderGitea:
		return newGiteaProviderForStage(root, repo, token, providers.WithGiteaMutationRecorder(recorder))
	default:
		return newCachedGitHubProvider(root, token, providers.WithMutationRecorder(recorder)), nil
	}
}
