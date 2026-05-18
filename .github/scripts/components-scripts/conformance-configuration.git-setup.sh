#!/bin/sh
# Set up a local bare git upstream used by the configuration.git conformance
# and certification tests. Exports GIT_REMOTE_URL pointing at a file:// URL
# so the tests don't need any network access.

set -e

ROOT=/tmp/dapr-conf-git
rm -rf "$ROOT"
mkdir -p "$ROOT"

git init --bare "$ROOT/upstream.git"

SEED="$ROOT/seed"
git clone "$ROOT/upstream.git" "$SEED"
git -C "$SEED" config user.email "ci@dapr.io"
git -C "$SEED" config user.name "ci"
git -C "$SEED" config commit.gpgsign false
git -C "$SEED" commit --allow-empty -m "initial"
git -C "$SEED" branch -M main
git -C "$SEED" push origin main

# Point the bare repo's HEAD at refs/heads/main. Without this, HEAD remains
# the default `refs/heads/master` (which doesn't exist), and go-git's
# PlainClone treats the upstream as effectively empty — the consumer then
# falls into a fresh-init path, producing a non-shared-history clone that
# can't fast-forward push back.
git --git-dir "$ROOT/upstream.git" symbolic-ref HEAD refs/heads/main

echo "GIT_REMOTE_URL=file://$ROOT/upstream.git" >> "$GITHUB_ENV"
