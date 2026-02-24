# Release Guide 

This document describes how to release Dapr Components Contrib along with associated artifacts.

## Prerequisites

Only maintainers and the release team should run releases.

## Pre-release

Pre-releases use tags like `v1.11.0-rc.0`, `-rc.1`, and so on. They are created through the GitHub Actions workflow.

### Steps

1. Update the Dapr runtime and dashboard versions in workflows or tests if needed. Merge that change to master.

2. Open GitHub Actions and click the **create-release** workflow.

3. Press the **Run workflow** button.
   The workflow will:

   * create the `release-<major>.<minor>` branch
   * create the pre-release tag
   * build the artifacts

4. Test the produced build.

5. If there are issues, fix them in the release branch and trigger the workflow again by creating a new pre-release tag (for example `-rc.1`).

6. Repeat until the build is good.

## Stable Release

Create a stable tag without the rc suffix (for example `v1.11.0`). Ensure the new release is set to `latest` and not a `pre-release`
CI will build and publish the release.

## Patch Releases

Use the existing release branch.
Create a new pre-release tag like `v1.11.1-rc.0`, test it, and when ready tag the stable version `v1.11.1`.
CI will build and publish it.

## Project Release Guidelines

See [this document](https://github.com/dapr/community/blob/master/release-process.md) for the project's release process and guidelines.