# Infisical secret store

This component provides a Dapr secret store implementation for [Infisical](https://infisical.com) using the official [Infisical Go SDK](https://github.com/Infisical/go-sdk).

## Component format

```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: infisical-secretstore
spec:
  type: secretstores.infisical
  version: v1
  metadata:
    - name: projectId
      value: "<infisical-project-id>"
    - name: environment
      value: "dev"
    - name: apiUrl
      value: "https://app.infisical.com"
    - name: clientId
      secretKeyRef:
        name: infisical-auth
        key: client-id
    - name: clientSecret
      secretKeyRef:
        name: infisical-auth
        key: client-secret
```

## Metadata fields

| Field | Required | Details |
| --- | --- | --- |
| `projectId` | Conditionally | Infisical project/workspace ID. Use this or `workspaceId`. |
| `workspaceId` | Conditionally | Alias for `projectId`. |
| `environment` | Yes | Environment slug, for example `dev` or `prod`. |
| `apiUrl` | No | Infisical API/app URL (defaults to the SDK default). |
| `accessToken` | Conditionally | Access token authentication. Use this, or use `clientId` + `clientSecret`. |
| `clientId` | Conditionally | Universal Auth client ID (required with `clientSecret` when `accessToken` is not set). |
| `clientSecret` | Conditionally | Universal Auth client secret (required with `clientId` when `accessToken` is not set). |
| `secretPath` | No | Optional path when retrieving/listing secrets. |
| `includeImports` | No | Include imported secrets. |
| `expandSecretReferences` | No | Expand secret references before returning values. |

## Per-request metadata

`GetSecret` and `BulkGetSecret` accept optional request metadata overrides for:
- `projectId` or `workspaceId`
- `environment`
- `secretPath`
- `includeImports`
- `type` (for `GetSecret`)

`BulkGetSecret` additionally accepts:
- `expandSecretReferences`

## Troubleshooting

- `missing required metadata: projectId (or workspaceId)` means both project/workspace metadata values are missing.
- `missing required metadata: environment` means the target environment was not configured.
- `missing required metadata: either accessToken, or both clientId and clientSecret` means authentication metadata is incomplete.
