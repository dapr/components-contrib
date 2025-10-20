# Akeyless Secret Store

This component provides a Dapr secret store implementation for [Akeyless](https://www.akeyless.io/), a cloud-native secrets management platform.

## Configuration

- [API Key](https://docs.akeyless.io/docs/api-key)
- [OAuth2.0/JWT](https://docs.akeyless.io/docs/oauth20jwt)
- [AWS IAM](https://docs.akeyless.io/docs/aws-iam)
- [Kubernetes](https://docs.akeyless.io/docs/kubernetes-auth)

### Authentication

The Akeyless secret store component supports the following configuration options:

| Field | Required | Description | Example |
|-------|----------|-------------|---------|
| `gatewayUrl` | No | The Akeyless Gateway URL. Default is https://api.akeyless.io. | `https://your-gateway.akeyless.io` |
| `accessId` | Yes | The Akeyless authentication access ID. | `p-123456780wm` |
| `jwt` | No | If using an OAuth2.0/JWT access ID, specify the JSON Web Token | `eyJ...` |
| `accessKey` | No | If using an API Key access ID, specify the API key | `ABCD123...=` |
| `k8sAuthConfigName` | No | If using the k8s auth method, specify the name of the k8s auth config. | `k8s-auth-config` |
| `k8sGatewayUrl` | No | The gateway URL that where the k8s auth config is located. | `http://gw.akeyless.svc.cluster.local:8000` |
| `k8sServiceAccountToken` | No | If using the k8s auth method, specify the service account token. If not specified,
      we will try to read it from the default service account token file. | `eyJ...` |

We currently support the following [Authentication Methods](https://docs.akeyless.io/docs/access-and-authentication-methods):


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

## Example Configuration: Kubernetes

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
    value: "https://gw.akeyless.svc.cluster.local"
  - name: accessId
    value: "p-1234Abcdwm"
  - name: k8sAuthConfigName
    value: "us-east-1-prod-akeyless-k8s-conf"
  - name: k8sGatewayUrl
    value: https://gw.akeyless.svc.cluster.local
```

## Usage

Once configured, you can retrieve secrets using the Dapr secrets API/SDK:

```bash
# Get a single secret
curl http://localhost:3500/v1.0/secrets/akeyless/my-secret

# Get all secrets
curl http://localhost:3500/v1.0/secrets/akeyless/bulk
```

## Features

- Supports static, dynamic and rotated secrets.
- **GetSecret**: Retrieve an individual value secret by path. 
- **BulkGetSecret**: Retrieve an all secrets from the root path.
