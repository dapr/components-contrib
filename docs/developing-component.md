# Developing new component

This document describes how to build and test new component.

## Prerequisites

1. [Dapr development environment setup](https://github.com/dapr/dapr/blob/master/docs/development/setup-dapr-development-env.md)

## Clone dapr and component-contrib
First fork the https://github.com/dapr/dapr and https://github.com/dapr/components-contrib repos on GitHub, to your own account or org.

```bash
cd $GOPATH/src

# Clone your fork of dapr
mkdir -p github.com/dapr/dapr
git clone https://github.com/{YOUR_GITHUB}/dapr.git github.com/dapr/dapr

# Clone our fork of component-contrib
mkdir -p github.com/dapr/components-contrib
git clone https://github.com/{YOUR_GITHUB}/components-contrib.git github.com/dapr/components-contrib

```

## Writing new component

### Write new component

1. Create your component directory in the right component directory
2. Copy component files from the reference component to your component directory
3. Add a go unit-test for your component

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

1. Make sure you have clonen dapr and component-contrib repos under $GOPATH/src/github.com/dapr
2. Replace github.com/dapr/components-contrib reference to the local component-contrib path on your filesytem. Run this command on the github.com/dapr/dapr directory. 
```bash
go mod edit -replace github.com/dapr/components-contrib=../components-contrib
```
This will modify the `github.com/dapr/dapr/go.mod` file

1. Import your component within daprd in the dapr/dapr project [main.go](https://github.com/dapr/dapr/blob/d17e9243b308e830649b0bf3af5f6e84fd543baf/cmd/daprd/main.go#L79)
2. Register your component in the same daprd [main.go](https://github.com/dapr/dapr/blob/d17e9243b308e830649b0bf3af5f6e84fd543baf/cmd/daprd/main.go#L153-L226) (e.g. binding, middleware etc). Follow the pattern used by other components
3. Build debuggable `daprd` binary
```bash
make DEBUG=1 build
```
6. Replace the installed `daprd` with the test binary (then dapr CLI will use the test binary)
```bash
# Back up the current daprd
mv /usr/local/bin/daprd /usr/local/bin/daprd.bak
# Copy in debug version just built
cp ./dist/darwin_amd64/debug/daprd /usr/local/bin
```
> Note. On most Linux systems you will need to prefix both commands with `sudo`    
> Linux Debuggable Binary: **./dist/linux_amd64/debug/daprd**  
> Windows Debuggable Binary: *.\dist\windows_amd64\debug\daprd**
1. Prepare a test app that will use the new component (e.g. kafka sample app: https://github.com/dapr/samples/blob/master/5.bindings/nodeapp/).
2. Create yaml for bindings in './components' under app’s directory (e.g. kafka example : https://github.com/dapr/samples/blob/master/5.bindings/deploy/kafka_bindings.yaml)
3. Run your test app using dapr CLI, e.g. with `dapr run`
4. Make sure your component is loaded successfully in daprd log

## Submit your component

1. Create a pull request to add your component in [component-contrib](https://github.com/dapr/components-contrib/pulls) repo
2. Get the approval from maintainers
3. Fetch the latest dapr/dapr repo
4. Update component-contrib go mod and ensure that component-contrib is updated to the latest version
```bash
go get -u github.com/dapr/components-contrib
```
5. Remove the replace in `dapr/dapr/go.mod`
```
go mod edit -dropreplace github.com/dapr/components-contrib
```
6. Create a pull request in [Dapr](https://github.com/dapr/dapr/pulls)
