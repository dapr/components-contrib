# Middlewares

Middleware components provide a way to define middlewares that are executed in a pipeline and modify requests and responses.

[Learn more about middlewares in Dapr](https://docs.dapr.io/developing-applications/middleware/)

## Implementing a new Middleware

A compliant middleware needs to implement the `Middleware` interface included in the [`middleware.go`](middleware.go) file.
