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

An output binding can be used to invoke an external system and also to return data from it.
Each output binding can decide which operations it supports. This information is communicated to the caller via the `Operations()` method.

```go
type OutputBinding interface {
	Init(metadata Metadata) error
	Invoke(req *InvokeRequest) error
	Operations() []string
}
```
A spec is also needed in [Dapr docs](https://github.com/dapr/docs/tree/master/reference/specs/bindings).