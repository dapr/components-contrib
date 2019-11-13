#!/usr/bin/env bash

if ! command -v golangci-lint > /dev/null; then
    echo "downloading golangci-lint"
    curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | sh -s -- -b $(go env GOPATH)/bin v1.16.0

fi

golangci-lint run

if [  "$?" = "1" ] ; then
    echo "golangci-lint ./...  detected problems"
    exit 1
fi

echo "code format is ok!"
exit 0
