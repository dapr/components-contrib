# Components Contrib

The purpose of Components Contrib is to provide open, community driven reusable components for building distributed applications.
These components are being used by the [Actions](https://github.com/actionscore/actions) project, but are separate and decoupled from it.

Using components developers can interact with bindings, state stores, messaging systems and more without caring about the underlying implementation.

Available component types:

* [Input/Output Bindings](bindings/Readme.md)
* [Pub Sub](pubsub/Readme.md)
* [State Stores](state/Readme.md)
* [Secret Stores](secretstores/Readme.md)

For documentation on how components are being used in Actions in a language/platform agnostic way, visit [Actions Docs](https://github.com/actionscore/docs).

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
mkdir -p github.com/actionscore/components-contrib
git clone https://github.com/actionscore/components-contrib.git github.com/actionscore/components-contrib
```

### Running tests

```bash
go test ./...
```
