# Bindings

Bindings provide a common way to trigger an application with events from external systems, or invoke an external system with optional data payloads.
Bindings are great for event-driven, on-demand compute and help reduce boilerplate code.

To get started with bindings visit the [How To Guide](https://docs.dapr.io/developing-applications/building-blocks/bindings/howto-bindings/).

To view all the currently supported bindings visit: [Dapr bindings](https://docs.dapr.io/operations/components/setup-bindings/supported-bindings/).

For detailed binding specs visit [Dapr binding specs](https://docs.dapr.io/operations/components/setup-bindings/supported-bindings/).

## Implementing a new binding

A compliant binding needs to implement at least one interface, depending on the type of binding (Input, Output, or both):

Input binding implement the `InputBinding` interface defined in [`input_binding.go`](input_binding.go)

Output binding implement the `OutputBinding` interface defined in [`output_binding.go`](output_binding.go)

An output binding can be used to invoke an external system and also to return data from it.
Each output binding can decide which operations it supports. This information is communicated to the caller via the `Operations()` method.

When creating an Output Binding, a list of `OperationKind` items needs to be returned.
For example, if running a component that takes in a SQL query and returns a result set, the `OperationKind` can be `query`.

While components are not restricted to a list of supported operations, it's best to use common ones if the operation kind falls under that operation definition.
The list of common operations can be found in [`requests.go`](requests.go).

After implementing a binding, the specification docs need to be updated via a Pull Request: [Dapr docs](https://docs.dapr.io/operations/components/setup-bindings/supported-bindings/).
