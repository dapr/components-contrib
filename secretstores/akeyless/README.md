# Akeyless Secret Store

This component provides a Dapr secret store implementation for [Akeyless](https://www.akeyless.io/), a cloud-native secrets management platform.

## Configuration

The Akeyless secret store component supports the following configuration options:

| Field | Required | Description | Example |
|-------|----------|-------------|---------|
| `gatewayUrl` | No | The Akeyless Gateway URL. If not provided, uses the default Akeyless cloud URL. | `https://your-gateway.akeyless.io` |
| `token` | Yes | The Akeyless authentication token. | `your-akeyless-token` |

## Example Configuration

```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: akeyless-secretstore
spec:
  type: secretstores.akeyless
  version: v1
  metadata:
  - name: gatewayUrl
    value: "https://your-gateway.akeyless.io"
  - name: token
    value: "your-akeyless-token"
```

## Usage

Once configured, you can retrieve secrets using the Dapr secrets API:

```bash
# Get a single secret
curl http://localhost:3500/v1.0/secrets/akeyless-secretstore/my-secret

# Get all secrets
curl http://localhost:3500/v1.0/secrets/akeyless-secretstore
```

## Features

- **GetSecret**: Retrieve individual secrets by name
- **BulkGetSecret**: Retrieve all secrets from the Akeyless vault
- **Authentication**: Supports Akeyless token-based authentication
- **Custom Gateway**: Supports custom Akeyless Gateway URLs

## Requirements

- Akeyless account and authentication token
- Akeyless Go SDK v5 (automatically included as a dependency)

## Authentication

The component uses Akeyless token-based authentication. You can obtain a token from your Akeyless dashboard or by using the Akeyless CLI.

For more information about Akeyless authentication, see the [Akeyless documentation](https://docs.akeyless.io/).
