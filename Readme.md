# Components Contrib

[![Go Report Card](https://goreportcard.com/badge/github.com/dapr/components-contrib)](https://goreportcard.com/report/github.com/dapr/components-contrib)
[![Build Status](https://github.com/dapr/components-contrib/workflows/components-contrib/badge.svg?event=push&branch=master)](https://github.com/dapr/components-contrib/actions?workflow=components-contrib)
[![Gitter](https://badges.gitter.im/Dapr/community.svg)](https://gitter.im/Dapr/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![Join the chat at https://gitter.im/Dapr/components-contrib](https://badges.gitter.im/Dapr/components-contrib.svg)](https://gitter.im/Dapr/components-contrib?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)


The purpose of Components Contrib is to provide open, community driven reusable components for building distributed applications.
These components are being used by the [Dapr](https://github.com/dapr/dapr) project, but are separate and decoupled from it.

Using components developers can interact with bindings, state stores, messaging systems and more without caring about the underlying implementation.

Available component types:

* [Input/Output Bindings](bindings/Readme.md)
* [Pub Sub](pubsub/Readme.md)
* [State Stores](state/Readme.md)
* [Secret Stores](secretstores/Readme.md)
* [Tracing Exporters](exporters/Readme.md)

For documentation on how components are being used in Dapr in a language/platform agnostic way, visit [Dapr Docs](https://github.com/dapr/docs).

## Developing components

### Prerequisites

1. The Go language environment [(instructions)](https://golang.org/doc/install).
   * Make sure that your GOPATH and PATH are configured correctly
   ```bash
   export GOPATH=~/go
   export PATH=$PATH:$GOPATH/bin
   ```

### Clone the repo

```bash
cd $GOPATH/src
mkdir -p github.com/dapr/components-contrib
git clone https://github.com/dapr/components-contrib.git github.com/dapr/components-contrib
```

### Running tests

```bash
make test
```

### Running linting

```bash
make lint
```