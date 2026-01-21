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
| `gatewayUrl` | No | The Akeyless Gateway API URL. Default is https://api.akeyless.io. | `https://gw.akeyless.svc.cluster.local:8000/api/v2` |
| `gatewayTlsCa` | No | The `base64`-encoded PEM certificate of the Akeyless Gateway. Use this when connecting to a gateway with a self-signed or custom CA certificate. The Akeyless client will be set to a 30 second timeout. | `LS0tLS1CRUdJTi...` |
| `accessId` | Yes | The Akeyless authentication access ID. | `p-123456780wm` |
| `jwt` | No | If using an OAuth2.0/JWT access ID, specify the JSON Web Token | `eyJ...` |
| `accessKey` | No | If using an API Key access ID, specify the API key | `ABCD123...=` |
| `k8sAuthConfigName` | No | If using the k8s auth method, specify the name of the k8s auth config. | `k8s-auth-config` |
| `k8sGatewayUrl` | No | The gateway URL that where the k8s auth config is located. | `http://gw.akeyless.svc.cluster.local:8000` |
| `k8sServiceAccountToken` | No | If using the k8s auth method, specify the service account token. If not specified,
      we will try to read it from the default service account token file `/var/run/secrets/kubernetes.io/serviceaccount/token`. | `eyJ...` |



## Examples

We currently support the following [Authentication Methods](https://docs.akeyless.io/docs/access-and-authentication-methods):


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
  - name: gatewayTlsCa
    value: "LS0tLS1CRUdJTi...."
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
    value: "http://unified.akeyless.svc.cluster.local:8000/api/v2"
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
    value: "http://unified.akeyless.svc.cluster.local:8000/api/v2"
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

# Get all secrets static secrets
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
