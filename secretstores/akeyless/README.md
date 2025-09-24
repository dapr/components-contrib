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

We currently support the following [Authentication Methods](https://docs.akeyless.io/docs/access-and-authentication-methods):

- [API Key](https://docs.akeyless.io/docs/api-key)
- [OAuth2.0/JWT](https://docs.akeyless.io/docs/oauth20jwt)
- [AWS IAM](https://docs.akeyless.io/docs/aws-iam)

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
    value: "eyJ....."
```

## Example Configuration: AWS IAM

```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: akeyless
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
curl http://localhost:3500/v1.0/secrets/akeyless/my-secret

# Get all secrets
curl http://localhost:3500/v1.0/secrets/akeyless/bulk
```

## Features

- **GetSecret**: Retrieve an individual static secret by name.
- **BulkGetSecret**: Retrieve an all static secrets.
