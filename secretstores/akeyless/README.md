# Akeyless Secret Store

This component provides a Dapr secret store implementation for [Akeyless](https://www.akeyless.io/), a cloud-native secrets management platform.

## Configuration

The Akeyless Dapr Secret Store component only supports the following [Authentication Methods](https://docs.akeyless.io/docs/access-and-authentication-methods):

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



## Examples

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

## Response Formats

The Akeyless secret store returns different response formats depending on the secret type:

### Static Secrets
Static secrets return their value directly as a string:

```json
{
  "my-static-secret": "secret-value"
}
```

### Dynamic Secrets
Dynamic secrets return a JSON string containing the credentials. The exact structure depends on the target system:

**MySQL Dynamic Secret:**
```json
{
  "my-mysql-secret": "{\"user\":\"generated_username\",\"password\":\"generated_password\",\"ttl_in_minutes\":\"60\",\"id\":\"username\"}"
}
```

**Azure AD Dynamic Secret:**
```json
{
  "my-azure-secret": "{\"user\":{\"id\":\"user_id\",\"displayName\":\"user_name\",\"mail\":\"email@domain.com\"},\"secret\":{\"keyId\":\"secret_key_id\",\"displayName\":\"secret_name\",\"tenantId\":\"tenant_id\"},\"ttl_in_minutes\":\"60\",\"id\":\"user_id\",\"msg\":\"User has been added successfully...\"}"
}
```

**GCP Dynamic Secret:**
```json
{
  "my-gcp-secret": "{\"encoded_key\":\"base64_encoded_service_account_key\",\"ttl_in_minutes\":\"60\",\"id\":\"service_account_name\"}"
}
```

### Rotated Secrets
Rotated secrets return a JSON object containing all available fields:

```json
{
  "my-rotated-secret": "{\"value\":{\"username\":\"rotated_user\",\"password\":\"rotated_password\",\"application_id\":\"1234567890\"}}"
}
```

**Note:** The exact fields in dynamic and rotated secret responses vary by target system and configuration. Applications should parse the JSON string to extract the specific credentials they need.
