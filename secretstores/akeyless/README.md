<!--
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Akeyless Secret Store

This component provides a Dapr secret store implementation for [Akeyless](https://www.akeyless.io/), a cloud-native secrets management platform.

## Configuration

The Akeyless Dapr Secret Store component supports the following [Authentication Methods](https://docs.akeyless.io/docs/access-and-authentication-methods):

- [API Key](https://docs.akeyless.io/docs/api-key)
- [OAuth2.0/JWT](https://docs.akeyless.io/docs/oauth20jwt)
- [AWS IAM](https://docs.akeyless.io/docs/aws-iam)
- [Kubernetes](https://docs.akeyless.io/docs/kubernetes-auth)

### Connection settings

| Field | Required | Description | Example |
|-------|----------|-------------|---------|
| `gatewayUrl` | No | The Akeyless Gateway API URL. Default is https://api.akeyless.io. | `https://gw.akeyless.svc.cluster.local:8000/api/v2` |
| `gatewayTlsCa` | No | The `base64`-encoded PEM certificate of the Akeyless Gateway. Use this when connecting to a gateway with a self-signed or custom CA certificate. The Akeyless client will be set to a 30 second timeout. | `LS0tLS1CRUdJTi...` |

### BulkGetSecret metadata

| Field | Required | Description | Example |
|-------|----------|-------------|---------|
| `path` | No | Path prefix for BulkGetSecret. Defaults to `/`. | `/my/org` |
| `secrets_type` | No | Comma-separated secret types: `static`, `dynamic`, `rotated`, or `all`. Defaults to `all`. | `static,dynamic` |

### API Key authentication

| Field | Required | Description | Example |
|-------|----------|-------------|---------|
| `accessId` | Yes | The Akeyless access ID for API key authentication. | `p-123456780am` |
| `accessKey` | Yes | The API key paired with the access ID. | `ABCD123...=` |

### JWT authentication

| Field | Required | Description | Example |
|-------|----------|-------------|---------|
| `accessId` | Yes | The Akeyless access ID for JWT authentication. | `p-123456780om` |
| `jwt` | Yes | The JSON Web Token to authenticate with. | `eyJ...` |

### AWS IAM authentication

| Field | Required | Description | Example |
|-------|----------|-------------|---------|
| `accessId` | Yes | The Akeyless access ID for AWS IAM authentication. | `p-123456780wm` |

### Kubernetes authentication

| Field | Required | Description | Example |
|-------|----------|-------------|---------|
| `accessId` | Yes | The Akeyless access ID for Kubernetes authentication. | `p-123456780km` |
| `k8sAuthConfigName` | Yes | Name of the Kubernetes Auth Method configured in the Akeyless Gateway (via the Akeyless Console or API). This is the Akeyless-side auth method that maps your Kubernetes cluster to the access ID — not a kubeconfig path or Kubernetes ConfigMap. | `k8s-auth-config` |
| `k8sGatewayUrl` | No | Gateway URL where the Kubernetes auth method is configured. Defaults to `gatewayUrl`. | `http://gw.akeyless.svc.cluster.local:8000` |
| `k8sServiceAccountToken` | No | Kubernetes service account token. When not specified, the component reads from `/var/run/secrets/kubernetes.io/serviceaccount/token`. | `eyJ...` |

## Examples

### API Key

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
  - name: gatewayTlsCa
    value: "LS0tLS1CRUdJTi...."
  - name: accessId
    value: "p-1234Abcdam"
  - name: accessKey
    value: "ABCD1233...="
```

### JWT

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
    value: "http://unified.akeyless.svc.cluster.local:8000/api/v2"
  - name: accessId
    value: "p-1234Abcdom"
  - name: jwt
    value: "eyJ....."
```

### AWS IAM

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
    value: "http://unified.akeyless.svc.cluster.local:8000/api/v2"
  - name: accessId
    value: "p-1234Abcdwm"
```

### Kubernetes

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
    value: "http://unified.akeyless.svc.cluster.local:8000/api/v2"
  - name: accessId
    value: "p-1234Abcdkm"
  - name: k8sAuthConfigName
    value: "us-east-1-prod-akeyless-k8s-conf"
  - name: k8sGatewayUrl
    value: https://gw.akeyless.svc.cluster.local
```

## Usage

Once configured, you can retrieve secrets using the Dapr secrets API:

```bash
# Get a single secret
curl http://localhost:3500/v1.0/secrets/akeyless/my-secret

# Get all secrets (static, dynamic, rotated) from root (/) path
curl http://localhost:3500/v1.0/secrets/akeyless/bulk

# Get all static secrets
curl http://localhost:3500/v1.0/secrets/akeyless/bulk?metadata.secrets_type=static

# Get all static and dynamic secrets from a specific path (/my/org)
curl http://localhost:3500/v1.0/secrets/akeyless/bulk?metadata.secrets_type=static,dynamic&metadata.path=/my/org
```

Or using the Dapr SDK. The example below retrieves all static secrets from path `/path/to/department`.
```go
log.Println("Starting test application")
client, err := dapr.NewClient()
if err != nil {
  log.Printf("Error creating Dapr client: %v\n", err)
  panic(err)
}
log.Println("Dapr client created successfully")
const daprSecretStore = "akeyless"

defer client.Close()
ctx := context.Background()
akeylessBulkMetadata := map[string]string{
  "path":         "/path/to/department",
  "secrets_type": "static",
}
secrets, err := client.GetBulkSecret(ctx, daprSecretStore, akeylessBulkMetadata)
if err != nil {
  log.Printf("Error fetching secrets: %v\n", err)
  panic(err)
}
log.Printf("Found %d secrets: ", len(secrets))
for secretName, secretValue := range secrets {
  log.Printf("Secret: %s, Value: %s", secretName, secretValue)
}
```

## Features

- Supports static, dynamic and rotated secrets.
- **GetSecret**: Retrieve an individual value secret by path. 
- **BulkGetSecret**: Retrieve all secrets from a specified path (or `/` by default) recursively.

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
