# Components Contrib

The purpose of Components Contrib is to provide open, community driven reusable components for building distributed applications.
These components are being used by the [Dapr](https://github.com/dapr/dapr) project, but are separate and decoupled from it.

Using components developers can interact with bindings, state stores, messaging systems and more without caring about the underlying implementation.

Available component types:

* [Input/Output Bindings](bindings/Readme.md)
* [Pub Sub](pubsub/Readme.md)
* [State Stores](state/Readme.md)
* [Secret Stores](secretstores/Readme.md)

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