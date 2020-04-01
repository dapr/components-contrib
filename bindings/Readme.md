# Bindings

Bindings provide a common way to trigger an application with events from external systems, or invoke an external system with optional data payloads.
Bindings are great for event-driven, on-demand compute and help reduce boilerplate code.

To get started with bindings visit the [How To Guide](https://github.com/dapr/docs/tree/master/howto#resources-bindings).

To view all the currently supported bindings visit: [Dapr bindings](https://github.com/dapr/docs/tree/master/concepts/bindings#supported-bindings-and-specs).

For detailed binding specs visit [Dapr binding specs](https://github.com/dapr/docs/tree/master/reference/specs/bindings).

## Implementing a new binding

A compliant binding needs to implement one or more interfaces, depending on the type of binding (Input or Output):

Input binding:

```go
type InputBinding interface {
	Init(metadata Metadata) error
	Read(handler func(*ReadResponse) error) error
}
```

Output binding:

```go
type OutputBinding interface {
	Init(metadata Metadata) error
	Write(req *WriteRequest) error
}
```
A spec is also needed in [Dapr docs](https://github.com/dapr/docs/tree/master/reference/specs/bindings).