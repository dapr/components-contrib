# Akeyless Secret Store

This component provides a Dapr secret store implementation for [Akeyless](https://www.akeyless.io/), a cloud-native secrets management platform.

## Configuration

The Akeyless secret store component supports the following configuration options:

| Field | Required | Description | Example |
|-------|----------|-------------|---------|
| `gatewayUrl` | No | The Akeyless Gateway URL. Default is https://api.akeyless.io. | `https://your-gateway.akeyless.io` |
| `accessId` | Yes | The Akeyless authentication access ID. | `p-123456780wm` |
| `jwt` | No | If using an OAuth2.0/JWT access ID, specify the JSON Web Token | `eyJ...` |
| `accessKey` | No | If using an API Key access ID, specify the API key | `ABCD123...=` |

## Example Configuration: API Key

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
  - name: accessId
    value: "p-1234Abcdam"
  - name: accessKey
    value: "ABCD1233...="
```


## Example Configuration: JWT

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
  - name: accessId
    value: "p-1234Abcdom"
  - name: jwt
    value: "eyJ"
```

## Example Configuration: AWS IAM

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
  - name: accessId
    value: "p-1234Abcdwm"
```

## Usage

Once configured, you can retrieve secrets using the Dapr secrets API:

```bash
# Get a single secret
curl http://localhost:3500/v1.0/secrets/akeyless-secretstore/my-secret

# Get all secrets
curl http://localhost:3500/v1.0/secrets/akeyless-secretstore/bulk
```

## Features

- **GetSecret**: Retrieve individual secrets by name
- **BulkGetSecret**: Retrieve all secrets from the Akeyless vault
