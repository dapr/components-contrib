# Consul Name Resolution

The consul name resolution component gives the ability to register and resolve other "daprized" services registered on a consul estate. It is flexible in that it allows for complex to minimal configurations driving the behaviour on init and resolution.

## How To Use

### Dapr Registration

If you are using the dapr sidecar to register your service to consul then you will need the following configuration:

```yaml
apiVersion: dapr.io/v1alpha1
kind: Configuration
metadata:
  name: appconfig
spec:
  nameResolution:
    component: "consul"
    configuration:
      selfRegister: true
```

### External Registration

If consul service registration is managed externally from dapr you need to ensure that the dapr-to-dapr internal grpc port is added to the service metadata under `DAPR_PORT` (this key is configurable) and that the consul service Id matches the dapr app Id. The config should then look as follows:

```yaml
apiVersion: dapr.io/v1alpha1
kind: Configuration
metadata:
  name: appconfig
spec:
  nameResolution:
    component: "consul"
```


## Behaviour

On init the consul component will either validate the connection to the configured (or default) agent or register the service if configured to do so. The name resolution interface does not cater for an "on shutdown" pattern so please consider this if using Dapr to register services to consul as it will not deregister services.

The component resolves target apps by filtering healthy services and looks for a `DAPR_PORT` in the metadata (key is configurable) in order to retrieve the Dapr sidecar port. Consul service.meta is used over service.port so as to not interfere with existing consul estates.


## Configuration Spec

As of writing the configuration spec is fixed to v1.3.0 of the consul api

| Name          | Type              | Description      |
| :------------ |------------------:| :----------------|
| Client  | [*api.Config](https://pkg.go.dev/github.com/hashicorp/consul/api@v1.3.0#Config) | Configures client connection to the consul agent. If blank it will use the sdk defaults, which in this case is just an address of `127.0.0.1:8500` |
| QueryOptions  | [*api.QueryOptions](https://pkg.go.dev/github.com/hashicorp/consul/api@v1.3.0#QueryOptions) | Configures query used for resolving healthy services, if blank it will default to `UseCache` as `true` |
| Checks | [[]*api.AgentServiceCheck](https://pkg.go.dev/github.com/hashicorp/consul/api@v1.3.0#AgentServiceCheck) | Configures health checks if/when registering. If blank it will default to a single health check on the Dapr sidecar health endpoint |
| Tags | `[]string` | Configures any tags to include if/when registering services |
| Meta | `map[string]string` | Configures any additional metadata to include if/when registering services |
| DaprPortMetaKey | `string` | The key used for getting the Dapr sidecar port from consul service metadata during service resolution, it will also be used to set the Dapr sidecar port in metadata during registration. If blank it will default to `DAPR_PORT` |
| SelfRegister | `bool` | Controls if Dapr will register the service to consul. The name resolution interface does not cater for an "on shutdown" pattern so please consider this if using Dapr to register services to consul as it will not deregister services. |
| AdvancedRegistration | [*api.AgentServiceRegistration](https://pkg.go.dev/github.com/hashicorp/consul/api@v1.3.0#AgentServiceRegistration) | Gives full control of service registration through configuration. If configured the component will ignore any configuration of Checks, Tags, Meta and SelfRegister. |

## Samples Configurations

### Basic

The minimum configuration needed is the following:

```yaml
apiVersion: dapr.io/v1alpha1
kind: Configuration
metadata:
  name: appconfig
spec:
  nameResolution:
    component: "consul"
```

### Registration with additional customizations

Enabling `SelfRegister` it is then possible to customize the checks, tags and meta

```yaml
apiVersion: dapr.io/v1alpha1
kind: Configuration
metadata:
  name: appconfig
spec:
  nameResolution:
    component: "consul"
    configuration:
      client:
        address: "127.0.0.1:8500"
      selfRegister: true
      checks:
        - name: "Dapr Health Status"
          checkID: "daprHealth:${APP_ID}"
          interval: "15s"
          http: "http://${HOST_ADDRESS}:${DAPR_HTTP_PORT}/v1.0/healthz"
        - name: "Service Health Status"
          checkID: "serviceHealth:${APP_ID}"
          interval: "15s"
          http: "http://${HOST_ADDRESS}:${APP_PORT}/health"
      tags:
        - "dapr"
        - "v1"
        - "${OTHER_ENV_VARIABLE}"
      meta:
        DAPR_METRICS_PORT: "${DAPR_METRICS_PORT}"
        DAPR_PROFILE_PORT: "${DAPR_PROFILE_PORT}"
      daprPortMetaKey: "DAPR_PORT"        
      queryOptions:
        useCache: true
        filter: "Checks.ServiceTags contains dapr"
```

### Advanced registration

Configuring the advanced registration gives you full control over all the properties possible when registering.

```yaml
apiVersion: dapr.io/v1alpha1
kind: Configuration
metadata:
  name: appconfig
spec:
  nameResolution:
    component: "consul"
    configuration:
      client:
          address: "127.0.0.1:8500"
      selfRegister: false
      queryOptions:
        useCache: true
      daprPortMetaKey: "DAPR_PORT"
      advancedRegistration:
        name: "${APP_ID}"
        port: ${APP_PORT}
        address: "${HOST_ADDRESS}"
        check:
          name: "Dapr Health Status"
          checkID: "daprHealth:${APP_ID}"
          interval: "15s"
          http: "http://${HOST_ADDRESS}:${DAPR_HTTP_PORT}/v1.0/healthz"
        meta:
          DAPR_METRICS_PORT: "${DAPR_METRICS_PORT}"
          DAPR_PROFILE_PORT: "${DAPR_PROFILE_PORT}"
        tags:
          - "dapr"
```