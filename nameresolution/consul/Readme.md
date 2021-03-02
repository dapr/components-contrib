# Consul Name Resolution

The consul name resolution component gives the ability to register and resolve other "daprized" services register on a consul estate. It is flexible in that it allows for complex to minimal configurations driving the behaviour on init and resolution.

## Behaviour

On init the consul component will either validate the connection to the configured (or default) agent or register the service if configured to do so. The name resolution interface does not cater for a on shutdown pattern so please consider this if using Dapr to register services to consul.

The component resolves target apps by filtering healthy services and looks for a `DAPR_PORT` in the metadata (key is configurable) in order to retreive the Dapr sidecar port. Consul service.meta is used over service.port so as to not interfere with existing consul estates.


## Configuration Spec

| Name          | Type              | Description      |
| :------------ |------------------:| :----------------|
| ClientConfig  | [*api.Config](https://pkg.go.dev/github.com/hashicorp/consul/api#Config) | Configures client connection to the consul agent. If blank it will use the sdk defaults, which in this case is just an address of `127.0.0.1:8500` |
| QueryOptions  | [*api.QueryOptions](https://pkg.go.dev/github.com/hashicorp/consul/api#QueryOptions) | Configures query used for resolving healthy services, if blank it will default to `UseCache` as `true` and will generate a filter that matches on each configured tag |
| Checks | [[]*api.AgentServiceCheck](https://pkg.go.dev/github.com/hashicorp/consul/api#AgentServiceCheck) | Configures health checks if/when registering. If blank it will default to a single healt check on the Dapr sidecar health endpoint |
| Tags | `[]string` | Used for filtering services during service resolution and is also included in registration if enabled. If blank it will default to a single tag `dapr` |
| Meta | `map[string]string` | Configures any additional metadata to include if/when registering services |
| DaprPortMetaKey | `string` | The key used for getting the Dapr sidecar port from consul service metadata during service resolution, it will also be used to set the Dapr sidecar port in metadata during registration. If blank it will default to `DAPR_PORT` |
| SelfRegister | `bool` | Controls if Dapr will register the service to consul. The name resolution interface does not cater for a "on shutdown" pattern so please consider this if using Dapr to register services to consul. |
| AdvancedRegistration | [*api.AgentServiceRegistration](https://pkg.go.dev/github.com/hashicorp/consul/api#AgentServiceRegistration) | Gives full control of service registration through configuration. If configured the component will ignore any configuration of Checks, Tags, Meta and SelfRegister. |

## Samples Configurations

### Basic

The minimum configuration needed is the following:

```yaml
spec:
  nameResolution:
    component: "consul"
}
```

### Registration with additional customizations

Enabling `SelfRegister` it is then possible to customize the checks, tags and meta

```yaml
spec:
  nameResolution:
    component: "consul"
    configuration:
      clientConfig:
        address: "127.0.0.1:8500"
      selfRegister: true
      checks:
        - name: "Dapr Health Status"
          checkID: "daprHealth:${APP_ID}"
          interval: "15s",
          http: "http://${HOST_ADDRESS}:${DAPR_HTTP_PORT}/v1.0/healthz"
        - name: "Service Health Status"
          checkID: "serviceHealth:${APP_ID}"
          interval: "15s",
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
}
```

### Advanced registration

Configuring the advanced registration gives you full control over all the properties possible when registering. When using advanced registration the query options will not default and must be configured as well.

```yaml
spec:
  nameResolution:
    component: "consul"
    configuration:
      clientConfig:
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
          interval: "15s",
          http: "http://${HOST_ADDRESS}:${DAPR_HTTP_PORT}/v1.0/healthz"
        meta:
          DAPR_METRICS_PORT: "${DAPR_METRICS_PORT}"
          DAPR_PROFILE_PORT: "${DAPR_PROFILE_PORT}"
        tags:
          - "dapr"
```