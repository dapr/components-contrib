# Structured Format Name Resolution

The Structured Format name resolver provides a flexible way to define services and their instances in a structured format via JSON or YAML configuration files, suitable for scenarios where explicit declaration of service topology is required.

## Configuration Format

To use the Structured Format name resolver, create a configuration in your Dapr environment:

```yaml
apiVersion: dapr.io/v1alpha1
kind: Configuration
metadata:
  name: appconfig
spec:
  nameResolution:
    component: "structuredformat"
    configuration:
      structuredType: "jsonString"
      stringValue: '{"appInstances":{"myapp":[{"domain":"","ipv4":"127.0.0.1","ipv6":"","port":4433,"extendedInfo":{"hello":"world"}}]}}'
```

## Configuration Fields

| Field   | Required | Details | Example |
|---------|----------|---------|---------|
| structuredType  | Y | Structured type: jsonString, yamlString, jsonFile, yamlFile. | jsonString |
| stringValue  | N | This field must be configured when structuredType is set to jsonString or yamlString. | {"appInstances":{"myapp":[{"domain":"","ipv4":"127.0.0.1","ipv6":"","port":4433,"extendedInfo":{"hello":"world"}}]}} |
| filePath  | N | This field must be configured when structuredType is set to jsonFile or yamlFile. | /path/to/yamlfile.yaml |


## Examples

```yaml
apiVersion: dapr.io/v1alpha1
kind: Configuration
metadata:
  name: appconfig
spec:
  nameResolution:
    component: "structuredformat"
    configuration:
      structuredType: "jsonString"
      stringValue: '{"appInstances":{"myapp":[{"domain":"","ipv4":"127.0.0.1","ipv6":"","port":4433,"extendedInfo":{"hello":"world"}}]}}'
```

```yaml
apiVersion: dapr.io/v1alpha1
kind: Configuration
metadata:
  name: appconfig
spec:
  nameResolution:
    component: "structuredformat"
    configuration:
      structuredType: "yamlString"
      stringValue: |
        appInstances:
          myapp:
           - domain: ""
             ipv4: "127.0.0.1"
             ipv6: ""
             port: 4433
             extendedInfo:
               hello: world
```

- Service ID "myapp" → "127.0.0.1:4433"


## Notes

- Empty service IDs are not allowed and will result in an error
- Accessing a non-existent service will also result in an error
- The structured format string must be provided in the configuration
- The program selects the first available address according to the priority order: domain → IPv4 → IPv6, and appends the port to form the final target address

