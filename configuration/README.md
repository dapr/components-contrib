# Configuration

Configuration components provide a way for your applications to receive configuration values, dynamically.

[Learn more about the Configuration building block in Dapr](https://docs.dapr.io/developing-applications/building-blocks/configuration/configuration-api-overview/)

## Implementing a new configuration store

A compliant configuration store needs to implement the `Store` inteface included in the [`store.go`](store.go) file.
