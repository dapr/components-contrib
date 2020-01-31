# Developing new component

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
| State | [components-contrib/state](https://github.com/dapr/components-contrib/tree/master/state) | [Redis](https://github.com/dapr/components-contrib/tree/master/state/redis) | [concept](https://github.com/dapr/docs/blob/master/concepts/state-management/state-management.md), [howto](https://github.com/dapr/docs/tree/master/howto/setup-state-store), [api spec](https://github.com/dapr/docs/blob/master/reference/api/state.md) |
| Pubsub | [components-contrib/pubsub](https://github.com/dapr/components-contrib/tree/master/pubsub) | [Redis](https://github.com/dapr/components-contrib/tree/master/pubsub/redis) | [concept](https://github.com/dapr/docs/tree/master/concepts/publish-subscribe-messaging), [howto](https://github.com/dapr/docs/tree/master/howto/setup-pub-sub-message-broker), [api spec](https://github.com/dapr/docs/blob/master/reference/api/pubsub.md) |
| Bindings | [components-contrib/bindings](https://github.com/dapr/components-contrib/tree/master/bindings) | [Kafka](https://github.com/dapr/components-contrib/tree/master/bindings/kafka) | [concept](https://github.com/dapr/docs/tree/master/concepts/bindings), [input howto](https://github.com/dapr/docs/tree/master/howto/trigger-app-with-input-binding), [output howto](https://github.com/dapr/docs/tree/master/howto/send-events-with-output-bindings), [api spec](https://github.com/dapr/docs/blob/master/reference/api/bindings.md) |
| Secret Store | [components-contrib/secretstore](https://github.com/dapr/components-contrib/tree/master/secretstores) | [Kubernetes](https://github.com/dapr/components-contrib/tree/master/secretstores/kubernetes), [Azure Keyvault](https://github.com/dapr/components-contrib/tree/master/secretstores/azure/keyvault) | [concept](https://github.com/dapr/docs/blob/master/concepts/components/secrets.md), [howto](https://github.com/dapr/docs/tree/master/howto/setup-secret-store)|
| Middleware | [components-contrib/middleware](https://github.com/dapr/components-contrib/tree/master/middleware) | [Oauth2](https://github.com/dapr/components-contrib/blob/master/middleware/http/oauth2/oauth2_middleware.go) | [concept](https://github.com/dapr/docs/blob/master/concepts/middleware/middleware.md), [howto](https://github.com/dapr/docs/tree/master/howto/authorization-with-oauth) |
| Exporter | [components-contrib/exporters](https://github.com/dapr/components-contrib/tree/master/exporters) | [Zipkin](https://github.com/dapr/components-contrib/blob/master/exporters/zipkin/zipkin_exporter.go) | [concept](https://github.com/dapr/docs/tree/master/concepts/distributed-tracing), [howto](https://github.com/dapr/docs/tree/master/howto/diagnose-with-tracing) |
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
go mod edit -replace github.com/dapr/component-contrib=../component-contrib
```

1. Import your component to dapr [main.go](https://github.com/dapr/dapr/blob/d17e9243b308e830649b0bf3af5f6e84fd543baf/cmd/daprd/main.go#L79)

2. Register your component in dapr [main.go](https://github.com/dapr/dapr/blob/d17e9243b308e830649b0bf3af5f6e84fd543baf/cmd/daprd/main.go#L153-L226)(e.g. binding)

3. Build dapr code locally

```bash
make DEBUG=1 build
```

6. Replace the installed daprd with the test binary (then dapr cli will use the test binary)

```bash
# copy .\dist\windows_amd64\debug\daprd c:\dapr
# cp ./dist/linux_amd64/debug/daprd /usr/local/bin

# if you're using MacOS
cp ./dist/darwin_amd64/debug/daprd /usr/local/bin
```

1. Prepare your test app (e.g. kafka sample app: https://github.com/dapr/samples/blob/master/5.bindings/nodeapp/)

2. Create yaml for bindings in './components' under app’s directory (e.g. kafka example : https://github.com/dapr/samples/blob/master/5.bindings/deploy/kafka_bindings.yaml)

3. Run your test app using dapr cli

4.  Make sure your binding component is loaded successfully in daprd log
