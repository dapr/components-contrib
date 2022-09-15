# Components Contrib

[![Build Status](https://github.com/dapr/components-contrib/workflows/components-contrib/badge.svg?event=push&branch=master)](https://github.com/dapr/components-contrib/actions?workflow=components-contrib)
[![Discord](https://img.shields.io/discord/778680217417809931)](https://discord.com/channels/778680217417809931/781589820128493598)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://github.com/dapr/components-contrib/blob/master/LICENSE)
[![FOSSA Status](https://app.fossa.com/api/projects/custom%2B162%2Fgithub.com%2Fdapr%2Fcomponents-contrib.svg?type=shield)](https://app.fossa.com/projects/custom%2B162%2Fgithub.com%2Fdapr%2Fcomponents-contrib?ref=badge_shield)

The purpose of Components Contrib is to provide open, community-driven, reusable components for building distributed applications.
These components are being used by the [Dapr](https://github.com/dapr/dapr) project, but are separate and decoupled from it.

Using components developers can interact with bindings, state stores, messaging systems and more, without caring about the underlying implementation.

Available component types:

* [Input/Output Bindings](bindings/README.md)
* [Pub Sub](pubsub/README.md)
* [State Stores](state/README.md)
* [Secret Stores](secretstores/README.md)
* [Name resolvers](nameresolution/README.md)
* [Configuration stores](configuration/README.md)
* [Middlewares](middleware/README.md)

For documentation on how components are being used in Dapr in a language/platform agnostic way, visit [Dapr Docs](https://docs.dapr.io).

## Contribution

* [Developing new component](docs/developing-component.md)

Thanks to everyone who has contributed!

<a href="https://github.com/dapr/components-contrib/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=dapr/components-contrib" />
</a>


## Code of Conduct

Please refer to our [Dapr Community Code of Conduct](https://github.com/dapr/community/blob/master/CODE-OF-CONDUCT.md)
