package main

import (
	"fmt"

	"github.com/goobers/goobers/internal/instance"
	"github.com/goobers/goobers/providers"
)

// The terminal preparer (branch cleanup + run-abort labeling) runs OUTSIDE a
// provider-chain stage: it is the runner's own finalize hook, so it has no
// routed stage env and no providerToken. It therefore resolves both the
// repository it acts on and the backend that reaches it from instance config
// alone — cfg.Repos[0], the same repo buildTerminalBranchDelete has always
// used — rather than from a stage's routed RepositoryRef.
//
// Both entrypoints previously hard-coded providers.NewGitHubProvider. On a
// Gitea instance that sent every terminal branch delete and every
// goobers:run-aborted label to api.github.com with a Gitea token, which is
// exactly the 401 observed live on gerty/goobers-hew: an aborted merge-review
// run journaled run_abort_label_failed after calling api.github.com, leaving
// the PR unlabeled and therefore still eligible for a later independent
// merge-review to approve and auto-merge — the precise failure abortedRunLabel
// exists to prevent.

// terminalRepositoryRef is the repository the terminal preparer acts on,
// carrying the repo's OWN declared provider kind instead of an unconditional
// providers.ProviderGitHub. The kind is load-bearing beyond dispatch: it is
// stamped into the journal's ExternalRef.Provider for branch-cleanup facts, so
// a Gitea instance used to journal every terminal branch ref as "github".
func terminalRepositoryRef(cfg *instance.Config) providers.RepositoryRef {
	if cfg == nil || len(cfg.Repos) == 0 {
		return providers.RepositoryRef{}
	}
	repo := cfg.Repos[0]
	provider := providers.ProviderKind(repo.Provider)
	if provider == "" {
		provider = providers.ProviderGitHub
	}
	return providers.RepositoryRef{
		Provider: provider,
		Owner:    repo.Owner,
		Name:     repo.Name,
	}
}

// terminalGiteaBaseURL returns the configured forge root for a Gitea terminal
// repo. Config validation already requires baseUrl on every Gitea repo, so an
// empty value here is a corrupted/hand-edited config and must fail loudly
// rather than silently degrade to a default host.
func terminalGiteaBaseURL(cfg *instance.Config) (string, error) {
	if cfg == nil || len(cfg.Repos) == 0 {
		return "", fmt.Errorf("no repository configured for terminal provider")
	}
	repo := cfg.Repos[0]
	if repo.BaseURL == "" {
		return "", fmt.Errorf("gitea repo %s/%s has no baseUrl configured", repo.Owner, repo.Name)
	}
	return repo.BaseURL, nil
}
