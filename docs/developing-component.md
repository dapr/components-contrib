# Developing new component

This document describes how to build and test new component.

## Prerequisites

1. [Dapr development environment setup](https://github.com/dapr/dapr/blob/master/docs/development/setup-dapr-development-env.md)

## Clone dapr and component-contrib

```bash
cd $GOPATH/src

# Clone dapr
mkdir -p github.com/dapr/dapr
git clone https://github.com/dapr/dapr.git github.com/dapr/dapr

# Clone component-contrib
mkdir -p github.com/dapr/components-contrib
git clone https://github.com/dapr/components-contrib.git github.com/dapr/components-contrib

```

## Writing new component

### Write new component

1. Create your component directory in the right component directory
2. Copy component files from the refernece component to your component directory
3. Add go unit-test for your component

| Type | Directory | Reference | Docs |
|------|-----------|--------------------------|------|
| State | [components-contrib/state](https://github.com/dapr/components-contrib/tree/master/state) | [Redis](https://github.com/dapr/components-contrib/tree/master/state/redis) | [concept](https://github.com/dapr/docs/blob/master/concepts/state-management), [howto](https://github.com/dapr/docs/tree/master/howto/setup-state-store), [api spec](https://github.com/dapr/docs/blob/master/reference/api/state_api.md) |
| Pubsub | [components-contrib/pubsub](https://github.com/dapr/components-contrib/tree/master/pubsub) | [Redis](https://github.com/dapr/components-contrib/tree/master/pubsub/redis) | [concept](https://github.com/dapr/docs/tree/master/concepts/publish-subscribe-messaging), [howto](https://github.com/dapr/docs/tree/master/howto/setup-pub-sub-message-broker), [api spec](https://github.com/dapr/docs/blob/master/reference/api/pubsub_api.md) |
| Bindings | [components-contrib/bindings](https://github.com/dapr/components-contrib/tree/master/bindings) | [Kafka](https://github.com/dapr/components-contrib/tree/master/bindings/kafka) | [concept](https://github.com/dapr/docs/tree/master/concepts/bindings), [input howto](https://github.com/dapr/docs/tree/master/howto/trigger-app-with-input-binding), [output howto](https://github.com/dapr/docs/tree/master/howto/send-events-with-output-bindings), [api spec](https://github.com/dapr/docs/blob/master/reference/api/bindings_api.md) |
| Secret Store | [components-contrib/secretstore](https://github.com/dapr/components-contrib/tree/master/secretstores) | [Kubernetes](https://github.com/dapr/components-contrib/tree/master/secretstores/kubernetes), [Azure Keyvault](https://github.com/dapr/components-contrib/tree/master/secretstores/azure/keyvault) | [concept](https://github.com/dapr/docs/blob/master/concepts/secrets), [howto](https://github.com/dapr/docs/tree/master/howto/setup-secret-store)|
| Middleware | [components-contrib/middleware](https://github.com/dapr/components-contrib/tree/master/middleware) | [Oauth2](https://github.com/dapr/components-contrib/blob/master/middleware/http/oauth2/oauth2_middleware.go) | [concept](https://github.com/dapr/docs/blob/master/concepts/middleware), [howto](https://github.com/dapr/docs/tree/master/howto/authorization-with-oauth) |
| Exporter | [components-contrib/exporters](https://github.com/dapr/components-contrib/tree/master/exporters) | [Zipkin](https://github.com/dapr/components-contrib/blob/master/exporters/zipkin/zipkin_exporter.go) | [concept](https://github.com/dapr/docs/tree/master/concepts/observability), [howto](https://github.com/dapr/docs/tree/master/howto/diagnose-with-tracing) |
| Service Discovery | [components-contrib/servicediscovery](https://github.com/dapr/components-contrib/tree/master/servicediscovery) | [mdns](https://github.com/dapr/components-contrib/blob/master/servicediscovery/mdns/mdns.go) | [howto](https://github.com/dapr/docs/tree/master/howto/invoke-and-discover-services) |

### Running unit-test

```bash
make test
```

### Running linting

```bash
make lint
```

## Validating with Dapr core

1. Make sure you clone dapr and component-contrib repos under $GOPATH/src/github.com/dapr
2. Replace github.com/dapr/components-contrib reference to the local component-contrib
```bash
go mod edit -replace github.com/dapr/components-contrib=../components-contrib
```
3. Import your component to dapr [main.go](https://github.com/dapr/dapr/blob/d17e9243b308e830649b0bf3af5f6e84fd543baf/cmd/daprd/main.go#L79)
4. Register your component in dapr [main.go](https://github.com/dapr/dapr/blob/d17e9243b308e830649b0bf3af5f6e84fd543baf/cmd/daprd/main.go#L153-L226)(e.g. binding)
5. Build debuggable dapr binary
```bash
make DEBUG=1 build
```
6. Replace the installed daprd with the test binary (then dapr cli will use the test binary)
```bash
# Back up the current daprd
mv /usr/local/bin/daprd /usr/local/bin/daprd.bak
cp ./dist/darwin_amd64/debug/daprd ~/.dapr/bin
```
> Linux Debuggable Binary: ./dist/linux_amd64/debug/daprd
> Windows Debuggable Binary: .\dist\windows_amd64\debug\daprd
7. Prepare your test app (e.g. kafka sample app: https://github.com/dapr/samples/blob/master/5.bindings/nodeapp/)
8. Create yaml for bindings in './components' under app’s directory (e.g. kafka example : https://github.com/dapr/samples/blob/master/5.bindings/deploy/kafka_bindings.yaml)
9. Run your test app using dapr cli
10. Make sure your component is loaded successfully in daprd log

## Submit your component

1. Create a pullrequest to add your component in [component-contrib](https://github.com/dapr/components-contrib/pulls) repo
2. Get the approval from maintainers
3. Fetch the latest dapr/dapr repo
4. Update component-contrib go mod and ensure that component-contrib is updated to the latest version
```bash
go get -u github.com/dapr/components-contrib@master
go mod tidy
```
5. Import your component to Dapr [main.go](https://github.com/dapr/dapr/blob/d17e9243b308e830649b0bf3af5f6e84fd543baf/cmd/daprd/main.go#L79)
6. Register your component in Dapr [main.go](https://github.com/dapr/dapr/blob/d17e9243b308e830649b0bf3af5f6e84fd543baf/cmd/daprd/main.go#L153-L226)
7. Create a pullrequest in [Dapr](https://github.com/dapr/dapr/pulls)
