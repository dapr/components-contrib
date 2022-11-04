# Developing new component

This document describes how to build and test new components. The Dapr runtime and all of its components are written in [Go](https://golang.org/). If you are completely new to the language you can [take a tour of its features](https://tour.golang.org/). For building your first component, using an existing one as a template is as a great way to get started.

## Prerequisites

1. [Dapr development environment setup](https://github.com/dapr/dapr/blob/master/docs/development/setup-dapr-development-env.md)
2. [golangci-lint](https://golangci-lint.run/usage/install/#local-installation)

## Clone dapr and component-contrib

We recommend creating a folder for Dapr and clone all repositories in that folder.

```bash
mkdir dapr

# Clone dapr
git clone https://github.com/dapr/dapr.git dapr/dapr

# Clone component-contrib
git clone https://github.com/dapr/components-contrib.git dapr/components-contrib

```

## Writing new component

### Write new component

1. Create your component directory in the right component directory.
2. Copy component files from the reference component to your component directory.
3. Add Go unit test files for your component.
4. Add [conformance tests](/tests/conformance/README.md) for your component.

| Type | Directory | Reference | Docs |
|------|-----------|--------------------------|------|
| State | [components-contrib/state](https://github.com/dapr/components-contrib/tree/master/state) | [Redis](https://github.com/dapr/components-contrib/tree/master/state/redis) | [concept](https://docs.dapr.io/developing-applications/building-blocks/state-management/state-management-overview/), [howto](https://docs.dapr.io/developing-applications/building-blocks/state-management/howto-get-save-state/), [api spec](https://docs.dapr.io/reference/api/state_api/) |
| Pubsub | [components-contrib/pubsub](https://github.com/dapr/components-contrib/tree/master/pubsub) | [Redis](https://github.com/dapr/components-contrib/tree/master/pubsub/redis) | [concept](https://docs.dapr.io/developing-applications/building-blocks/pubsub/pubsub-overview/), [howto](https://docs.dapr.io/developing-applications/building-blocks/pubsub/howto-publish-subscribe/), [api spec](https://docs.dapr.io/reference/api/pubsub_api/) |
| Bindings | [components-contrib/bindings](https://github.com/dapr/components-contrib/tree/master/bindings) | [Kafka](https://github.com/dapr/components-contrib/tree/master/bindings/kafka) | [concept](https://docs.dapr.io/developing-applications/building-blocks/bindings/bindings-overview/), [input howto](https://docs.dapr.io/developing-applications/building-blocks/bindings/howto-triggers/), [output howto](https://docs.dapr.io/developing-applications/building-blocks/bindings/howto-bindings/), [api spec](https://docs.dapr.io/reference/api/bindings_api/) |
| Secret Store | [components-contrib/secretstore](https://github.com/dapr/components-contrib/tree/master/secretstores) | [Kubernetes](https://github.com/dapr/components-contrib/tree/master/secretstores/kubernetes), [Azure Keyvault](https://github.com/dapr/components-contrib/tree/master/secretstores/azure/keyvault) | [concept](https://docs.dapr.io/developing-applications/building-blocks/secrets/secrets-overview/), [howto](https://docs.dapr.io/developing-applications/building-blocks/secrets/howto-secrets/)|
| Middleware | [components-contrib/middleware](https://github.com/dapr/components-contrib/tree/master/middleware) | [Oauth2](https://github.com/dapr/components-contrib/blob/master/middleware/http/oauth2/oauth2_middleware.go) | [concept](https://docs.dapr.io/concepts/middleware-concept/), [howto](https://docs.dapr.io/operations/security/oauth/) |
| Name Resolution | [components-contrib/nameresolution](https://github.com/dapr/components-contrib/tree/master/nameresolution) | [mdns](https://github.com/dapr/components-contrib/blob/master/nameresolution/mdns/mdns.go) | [howto](https://docs.dapr.io/developing-applications/building-blocks/service-invocation/howto-invoke-discover-services/) |

### Running unit-test

```bash
make test
```

### Running linting

```bash
make lint
```

## Validating with Dapr core

1. Make sure you clone the `dapr/dapr` and `dapr/components-contrib` repositories side-by-side, within the same folder.
1. In case of compatibility issues between `dapr/dapr` and `dapr/compoments-contrib` `go.mod` files during build, checkout the latest released version of `dapr/dapr`.
1. Replace `github.com/dapr/components-contrib` with a reference to the locally-cloned `components-contrib`:
   ```bash
   go mod edit -replace github.com/dapr/components-contrib=../components-contrib
   ```
1. Register your components by creating a file (in the `dapr/dapr` repo) in [`cmd/daprd/components`](https://github.com/dapr/dapr/tree/master/cmd/daprd/components), similar to the ones in the folder (one file per component).
1. Build debuggable dapr binary
   ```bash
   make modtidy-all
   make DEBUG=1 build
   ```
1. Replace the installed `daprd` with the test binary (the Dapr CLI will then use the test binary):
   ```bash
   # Back up the current daprd
   cp ~/.dapr/bin/daprd ~/.dapr/bin/daprd.bak
   cp ./dist/darwin_amd64/debug/daprd ~/.dapr/bin
   ```
   > Linux debuggable binary: `./dist/linux_amd64/debug/daprd`
   > Windows debuggable binary: `.\dist\windows_amd64\debug\daprd`
   > macOS (Intel) debuggable binary: `./dist/darwin_amd64/debug/daprd`
   > macOS (Apple Silicon) debuggable binary: `./dist/darwin_arm64/debug/daprd`
1. Prepare your test app (e.g. kafka sample app: https://github.com/dapr/quickstarts/tree/master/bindings/nodeapp/)
1. Create a YAML for the component in './components' under app's directory (e.g. kafka example: https://github.com/dapr/quickstarts/blob/master/bindings/components/kafka_bindings.yaml)
1. Run your test app using Dapr CLI.
1. Make sure your component is loaded successfully in the daprd log.

## Submit your component

1. Create a Pull Request to add your component in [component-contrib](https://github.com/dapr/components-contrib/pulls) repo
1. Get the approval from maintainers
1. Fetch the latest `dapr/dapr` repo
1. Update components-contrib in `dapr/dapr`'s `go.mod` and ensure that `components-contrib` is updated to the latest version
   ```bash
   # In the folder where the dapr/dapr repo was cloned
   go get -u github.com/dapr/components-contrib@master
   make modtidy-all
   ```
1. Register your components by creating a file (in the `dapr/dapr` repo) in [`cmd/daprd/components`](https://github.com/dapr/dapr/tree/master/cmd/daprd/components), similar to the ones in the folder (one file per component).
1. Create a pull request in [dapr/dapr](https://github.com/dapr/dapr/pulls).

## Version 2 and beyond of a component

API versioning of Dapr components follows the same approach as [Go modules](https://blog.golang.org/v2-go-modules) where the unstable version (v0) and first stable version (v1) are contained in the root directory of the component package.  For example v1 of the Redis binding component is located in `bindings/redis`. Code changes may continue in this package provided there are no breaking changes. Breaking changes include:

* Renaming or removing a `metadata` field that the component is currently using
* Adding a required `metadata` field
* Adding an optional field that does not have a backwards-compatible default value
* Making significant changes to the component's behavior that would adversely affect existing users

In most cases, breaking changes can be avoided by using backward compatible `metadata` fields. When breaking changes cannot be avoided, here are the steps for creating the next major version of a component:

1. Create a version subdirectory for the next major version (e.g. `bindings/redis/v2`, `bindings/redis/v3`, etc.)
1. Copy any code into the new subdirectory that should be preserved from the previous version
1. Submit your component as described in the previous section
1. Register your components by creating a **new** file (in the `dapr/dapr` repo) in [`cmd/daprd/components`](https://github.com/dapr/dapr/tree/master/cmd/daprd/components), *without removing the file for the previous version*. This time, append the new major version to the name, e.g. `redis/v2`.
1. Validate your component as described previously
