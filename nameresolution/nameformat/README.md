# Name Format Name Resolution

The Name Format name resolver provides a flexible way to resolve service names using a configurable format string with placeholders. This is useful in scenarios where you want to map service names to predictable DNS names following a specific pattern.

## Configuration Format

To use the Name Format name resolver, create a configuration in your Dapr environment:

```yaml
apiVersion: dapr.io/v1alpha1
kind: Configuration
metadata:
  name: appconfig
spec:
  nameResolution:
    component: "nameformat"
    configuration:
      format: "service-{appid}.default.svc.cluster.local"  # Replace with your desired format pattern
```

## Configuration Fields

| Field   | Required | Details | Example |
|---------|----------|---------|---------|
| format  | Y | The format string to use for name resolution. Must contain the `{appid}` placeholder which will be replaced with the actual service name. | `"service-{appid}.default.svc.cluster.local"` |

## Examples

When configured with `format: "service-{appid}.default.svc.cluster.local"`, the resolver will transform service names as follows:

- Service ID "myapp" → "service-myapp.default.svc.cluster.local"
- Service ID "frontend" → "service-frontend.default.svc.cluster.local"


## Notes

- Empty service IDs are not allowed and will result in an error
- The format string must be provided in the configuration
- The format string must contain at least one `{appid}` placeholder 