/*
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package git

import (
	"context"
	"errors"
	"fmt"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"

	"github.com/dapr/components-contrib/configuration/git/auth"
)

const remoteName = "origin"

// cloneFresh clones the repo at url into dir. The clone is bound to the given
// branch and tracks `origin`. Auth is fetched per-call so GitHub App tokens
// rotate naturally.
func cloneFresh(ctx context.Context, dir, url, branch string, depth int, strategy auth.Strategy) (*gogit.Repository, error) {
	method, err := strategy.AuthMethod(ctx)
	if err != nil {
		return nil, fmt.Errorf("auth: %w", err)
	}
	opts := &gogit.CloneOptions{
		URL:           url,
		Auth:          method,
		ReferenceName: plumbing.NewBranchReferenceName(branch),
		SingleBranch:  true,
		Depth:         depth,
	}
	repo, err := gogit.PlainCloneContext(ctx, dir, false, opts)
	if err != nil {
		return nil, fmt.Errorf("clone %s: %w", url, err)
	}
	return repo, nil
}

// fetchIfChanged fetches the configured branch and, if the upstream tip has
// moved, resets the worktree to the new tip. Returns (head, changed, err)
// where `changed` is false when the remote ref is unchanged. Uses go-git's
// own NoErrAlreadyUpToDate signal rather than a separate ls-remote probe;
// this avoids downloading the full ref advertisement on every poll tick.
func fetchIfChanged(ctx context.Context, repo *gogit.Repository, branch string, strategy auth.Strategy) (plumbing.Hash, bool, error) {
	method, err := strategy.AuthMethod(ctx)
	if err != nil {
		return plumbing.ZeroHash, false, fmt.Errorf("auth: %w", err)
	}
	branchRef := plumbing.NewBranchReferenceName(branch)
	remoteRef := plumbing.NewRemoteReferenceName(remoteName, branch)

	fetchSpec := config.RefSpec(fmt.Sprintf("+%s:%s", branchRef, remoteRef))
	fetchErr := repo.FetchContext(ctx, &gogit.FetchOptions{
		RemoteName: remoteName,
		Auth:       method,
		RefSpecs:   []config.RefSpec{fetchSpec},
		Force:      true,
	})
	upToDate := errors.Is(fetchErr, gogit.NoErrAlreadyUpToDate)
	if fetchErr != nil && !upToDate {
		return plumbing.ZeroHash, false, fmt.Errorf("fetch: %w", fetchErr)
	}

	tip, err := repo.Reference(remoteRef, true)
	if err != nil {
		return plumbing.ZeroHash, false, fmt.Errorf("resolve remote ref: %w", err)
	}
	hash := tip.Hash()

	currentHead, err := repo.Head()
	if err == nil && currentHead.Hash() == hash && upToDate {
		return hash, false, nil
	}

	wt, err := repo.Worktree()
	if err != nil {
		return plumbing.ZeroHash, false, fmt.Errorf("worktree: %w", err)
	}
	if err := wt.Reset(&gogit.ResetOptions{Mode: gogit.HardReset, Commit: hash}); err != nil {
		return plumbing.ZeroHash, false, fmt.Errorf("hard reset to %s: %w", hash, err)
	}
	return hash, true, nil
}
