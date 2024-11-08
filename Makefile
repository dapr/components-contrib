# ------------------------------------------------------------
# Copyright 2021 The Dapr Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
CGO ?= 0
DAPR_PACKAGE ?= $(dapr_package)

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
  FINDBIN := where
  BINARY_EXT_LOCAL:=.exe
  GOLANGCI_LINT:=golangci-lint.exe
  # Workaround for https://github.com/golang/go/issues/40795
  BUILDMODE:=-buildmode=exe
else
  FINDBIN := which
  BINARY_EXT_LOCAL:=
  GOLANGCI_LINT:=golangci-lint
endif

# Get linter versions
LINTER_BINARY := $(shell $(FINDBIN) $(GOLANGCI_LINT))
export GH_LINT_VERSION := $(shell grep 'GOLANGCI_LINT_VER:' .github/workflows/components-contrib.yml | xargs | cut -d" " -f2)
ifeq (,$(LINTER_BINARY))
    INSTALLED_LINT_VERSION := "v0.0.0"
else
	INSTALLED_LINT_VERSION=v$(shell $(LINTER_BINARY) version | grep -Eo '([0-9]+\.)+[0-9]+' - | head -1 || "")
endif

# Build tools
ifeq ($(TARGET_OS_LOCAL),windows)
	BUILD_TOOLS_BIN ?= components-contrib-build-tools.exe
	BUILD_TOOLS ?= ./.build-tools/$(BUILD_TOOLS_BIN)
	RUN_BUILD_TOOLS ?= cd .build-tools; go.exe run .
else
	BUILD_TOOLS_BIN ?= components-contrib-build-tools
	BUILD_TOOLS ?= ./.build-tools/$(BUILD_TOOLS_BIN)
	RUN_BUILD_TOOLS ?= cd .build-tools; go run .
endif

################################################################################
# Linter targets                                                               #
################################################################################
.PHONY: verify-linter-installed
verify-linter-installed:
	@if [ -z $(LINTER_BINARY) ]; then \
	  echo "[!] golangci-lint not installed"; \
		echo "[!] You can install it from https://golangci-lint.run/usage/install/"; \
		echo "[!]   or by running"; \
		echo "[!]   curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin $(GH_LINT_VERSION)"; \
		exit 1; \
	fi;

.PHONY: verify-linter-version
verify-linter-version:
	@if [ "$(GH_LINT_VERSION)" != "$(INSTALLED_LINT_VERSION)" ]; then \
	  echo "[!] Your locally installed version of golangci-lint is different from the pipeline"; \
	  echo "[!] This will likely cause linting issues for you locally"; \
	  echo "[!] Yours:  $(INSTALLED_LINT_VERSION)"; \
		echo "[!] Theirs: $(GH_LINT_VERSION)"; \
		echo "[!] Upgrade: curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin $(GH_LINT_VERSION)"; \
                GOLANGCI_LINT=$(go env GOPATH)/bin/$(GOLANGCI_LINT) \
	  sleep 3; \
	fi;


################################################################################
# Target: test                                                                 #
################################################################################
.PHONY: test
test:
	CGO_ENABLED=$(CGO) go test ./... $(COVERAGE_OPTS) $(BUILDMODE) -tags metadata --timeout=15m

################################################################################
# Target: lint                                                                 #
################################################################################
.PHONY: lint
lint: verify-linter-installed verify-linter-version
ifdef LINT_BASE
	@echo "LINT_BASE is set to "$(LINT_BASE)". Linter will only check diff."
	$(GOLANGCI_LINT) run --timeout=20m --max-same-issues 0 --max-issues-per-linter 0 --new-from-rev $(shell git rev-parse $(LINT_BASE))
else
	$(GOLANGCI_LINT) run --timeout=20m --max-same-issues 0 --max-issues-per-linter 0
endif

################################################################################
# Target: modtidy-all                                                          #
################################################################################
MODFILES := $(shell find . -name go.mod)

define modtidy-target
.PHONY: modtidy-$(1)
modtidy-$(1):
	@echo $(shell dirname $(1))
	@cd $(shell dirname $(1)); CGO_ENABLED=$(CGO) go mod tidy -compat=1.20 || { echo "There was an error in running go mod tidy for this file,"; exit 1;}; cd -
endef

define replaceruntime-dapr
.PHONY: replaceruntime-$(1)
replaceruntime-$(1):
	cd $(shell dirname $(1)); go mod edit -replace github.com/dapr/dapr=$(DAPR_PACKAGE); cd -
endef

define dropreplaceruntime-dapr
.PHONY: dropreplaceruntime-$(1)
dropreplaceruntime-$(1):
	cd $(shell dirname $(1)); go mod edit -dropreplace github.com/dapr/dapr; go get github.com/dapr/dapr@master; go mod tidy; cd -
endef

# Generate modtidy target action for each go.mod file
$(foreach MODFILE,$(MODFILES),$(eval $(call modtidy-target,$(MODFILE))))

# Go replace dapr runtime package to tests/.../go.mod.
$(foreach MODFILE,$(MODFILES),$(eval $(call replaceruntime-dapr,$(MODFILE))))

# Go drop replace dapr runtime package to tests/.../go.mod.
$(foreach MODFILE,$(MODFILES),$(eval $(call dropreplaceruntime-dapr,$(MODFILE))))

# Enumerate all generated modtidy targets
# Note that the order of execution matters: root and tests/certification go.mod
# are dependencies in each certification test. This order is preserved by the
# tree walk when finding the go.mod files.
TIDY_MODFILES:=$(foreach ITEM,$(MODFILES),modtidy-$(ITEM))

REPLACERUNTIME_MODFILES:=$(foreach ITEM,$(MODFILES),replaceruntime-$(ITEM))

DROPREPLACERUNTIME_MODFILES:=$(foreach ITEM,$(MODFILES),dropreplaceruntime-$(ITEM))

# Define modtidy-all action trigger to run make on all generated modtidy targets
.PHONY: modtidy-all
modtidy-all: $(TIDY_MODFILES)

# Define replaceruntime-all action trigger to go get replace dapr package specified.
.PHONY: replaceruntime-all
replaceruntime-all: $(REPLACERUNTIME_MODFILES)

# Define dropreplaceruntime-all action trigger to go get dapr package master.
.PHONY: dropreplaceruntime-all
dropreplaceruntime-all: $(DROPREPLACERUNTIME_MODFILES)

################################################################################
# Target: modtidy                                                              #
################################################################################
.PHONY: modtidy
modtidy:
	go mod tidy

################################################################################
# Target: check-mod-diff                                                       #
################################################################################
.PHONY: check-mod-diff
check-mod-diff:
	git diff --exit-code -- '*go.mod' # check no changes
	git diff --exit-code -- '*go.sum' # check no changes

################################################################################
# Target: compile-build-tools                                                  #
################################################################################
.PHONY: compile-build-tools
compile-build-tools:
ifeq (,$(wildcard $(BUILD_TOOLS)))
	cd .build-tools; CGO_ENABLED=$(CGO) GOOS=$(TARGET_OS_LOCAL) GOARCH=$(TARGET_ARCH_LOCAL) go build -o $(BUILD_TOOLS_BIN) .
endif

################################################################################
# Components schema targets                                                    #
################################################################################
.PHONY: component-metadata-schema
component-metadata-schema:
	$(RUN_BUILD_TOOLS) gen-component-schema > ../component-metadata-schema.json

.PHONY: check-component-metadata-schema-diff
check-component-metadata-schema-diff: component-metadata-schema
	git diff --exit-code -- component-metadata-schema.json # check no changes

################################################################################
# Component metadata bundle targets                                            #
################################################################################
.PHONY: bundle-component-metadata
bundle-component-metadata:
	$(RUN_BUILD_TOOLS) bundle-component-metadata > ../component-metadata-bundle.json

################################################################################
# Component metadata check                                                     #
################################################################################
.PHONE: check-component-metadata
check-component-metadata:
	mkdir -p metadataanalyzer
	$(RUN_BUILD_TOOLS) generate-metadata-analyzer-app --outputfile ./metadataanalyzer/main.go
	cd metadataanalyzer && \
	go mod init metadataanalyzer && \
	go get "github.com/dapr/components-contrib@main" && \
	go mod edit -replace "github.com/dapr/components-contrib"="../" && \
	go mod tidy && \
	go build -tags metadata . && \
	rm ./go.mod && rm ./go.sum && rm ./main.go && \
        ./metadataanalyzer ../

################################################################################
# Prettier                                                                     #
################################################################################
.PHONY: prettier-install prettier-check prettier-format
prettier-install:
	npm install --global prettier

prettier-check:
	npx prettier --check "*/**/*.{ts,js,mjs,json}"

prettier-format:
	npx prettier --write "*/**/*.{ts,js,mjs,json}"

################################################################################
# Target: conf-tests                                                           #
################################################################################
.PHONY: conf-tests
conf-tests:
	CGO_ENABLED=$(CGO) go test -v -tags=conftests -count=1 ./tests/conformance
