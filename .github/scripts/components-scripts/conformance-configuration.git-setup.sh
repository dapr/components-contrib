#!/bin/sh
# Set up a local bare git upstream used by the configuration.git conformance
# and certification tests. Exports GIT_UPSTREAM_URL pointing at a file:// URL
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

echo "GIT_UPSTREAM_URL=file://$ROOT/upstream.git" >> "$GITHUB_ENV"
