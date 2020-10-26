# Components Contrib

[![Go Report Card](https://goreportcard.com/badge/github.com/dapr/components-contrib)](https://goreportcard.com/report/github.com/dapr/components-contrib)
[![Build Status](https://github.com/dapr/components-contrib/workflows/components-contrib/badge.svg?event=push&branch=master)](https://github.com/dapr/components-contrib/actions?workflow=components-contrib)
[![Join the chat at https://gitter.im/Dapr/components-contrib](https://badges.gitter.im/Dapr/components-contrib.svg)](https://gitter.im/Dapr/components-contrib?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)


The purpose of Components Contrib is to provide open, community driven reusable components for building distributed applications.
These components are being used by the [Dapr](https://github.com/dapr/dapr) project, but are separate and decoupled from it.

Using components developers can interact with bindings, state stores, messaging systems and more without caring about the underlying implementation.

Available component types:

* [Input/Output Bindings](bindings/Readme.md)
* [Pub Sub](pubsub/Readme.md)
* [State Stores](state/Readme.md)
* [Secret Stores](secretstores/Readme.md)
* [Tracing Exporters](exporters/Readme.md)

For documentation on how components are being used in Dapr in a language/platform agnostic way, visit [Dapr Docs](https://docs.dapr.io).

## Contribution

* [Developing new component](docs/developing-component.md)

## Code of Conduct

Please refer to our [Dapr Community Code of Conduct](https://github.com/dapr/community/blob/master/CODE-OF-CONDUCT.md)
