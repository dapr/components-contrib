# ------------------------------------------------------------
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
# ------------------------------------------------------------

################################################################################
# Variables                                                                    #
################################################################################

export GO111MODULE ?= on
export GOPROXY ?= https://proxy.golang.org
export GOSUMDB ?= sum.golang.org

GIT_COMMIT  = $(shell git rev-list -1 HEAD)
GIT_VERSION = $(shell git describe --always --abbrev=7 --dirty)
# By default, disable CGO_ENABLED. See the details on https://golang.org/cmd/cgo
CGO         ?= 0

LOCAL_ARCH := $(shell uname -m)
ifeq ($(LOCAL_ARCH),x86_64)
	TARGET_ARCH_LOCAL=amd64
else ifeq ($(shell echo $(LOCAL_ARCH) | head -c 5),armv8)
	TARGET_ARCH_LOCAL=arm64
else ifeq ($(shell echo $(LOCAL_ARCH) | head -c 4),armv)
	TARGET_ARCH_LOCAL=arm
else
	TARGET_ARCH_LOCAL=amd64
endif
export GOARCH ?= $(TARGET_ARCH_LOCAL)

LOCAL_OS := $(shell uname)
ifeq ($(LOCAL_OS),Linux)
   TARGET_OS_LOCAL = linux
else ifeq ($(LOCAL_OS),Darwin)
   TARGET_OS_LOCAL = darwin
else
   TARGET_OS_LOCAL ?= windows
endif
export GOOS ?= $(TARGET_OS_LOCAL)

ifeq ($(GOOS),windows)
BINARY_EXT_LOCAL:=.exe
GOLANGCI_LINT:=golangci-lint.exe
else
BINARY_EXT_LOCAL:=
GOLANGCI_LINT:=golangci-lint
endif

################################################################################
# Target: test                                                                 #
################################################################################
.PHONY: test
test:
	go test ./...

################################################################################
# Target: lint                                                                 #
################################################################################
.PHONY: lint
lint:
	# Due to https://github.com/golangci/golangci-lint/issues/580, we need to add --fix for windows
	$(GOLANGCI_LINT) run --timeout=20m

################################################################################
# Target: go.mod                                                               #
################################################################################
.PHONY: go.mod
go.mod:
	go mod tidy

################################################################################
# Target: check-diff                                                           #
################################################################################
.PHONY: check-diff
check-diff:
	git diff --exit-code ./go.mod # check no changes
