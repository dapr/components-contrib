# Name resolution

Name resolvers provide a common way to interact with different name resolvers, which are used to return the address or IP of other services your applications may connect to.

## Implementing a new Name Resolver

A compliant name resolver needs to implement the `Resolver` inteface included in the [`nameresolution.go`](nameresolution.go) file.
