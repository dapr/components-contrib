# Redis State Store certification testing

This project aims to test the Redis State Store component under various conditions.

## Test plan

## Basic Test for CRUD operations:
1. Able to create and test connection.
2. Able to do set, fetch, update and delete.
3. Negative test to fetch record with key, that is not present.

## Test save or update data with different TTL settings:
1. TTL not expiring
2. TTL not a valid number
3. Provide a TTL of 1 second:
a. Fetch this record just after saving
b. Sleep for 2 seconds
c. Try to fetch again after a gap of 2 seconds, record shouldn't be found

## Component must reconnect when server or network errors are encountered

## Infra test:
1- When redis goes down and then comes back up - client is able to connect

## eTag related:
a. Insert a Key-Value pair, eTag will be 1
b. Update Value v2 for this Key with eTag equal to 1 - new eTag wil be 2.
c. Try to Update v3 for this Key with eTag equal to 4 - value should not get updated.
d. Get and validate eTag for it should be 2 only.

## Transaction related, like Upsert:
Upsert in Multi function, using 3 keys with updating values and TTL for 2 of the keys, down in the order.

## enableTLS set to true & enableTLS not integer:
Testing by creating component with ignoreErrors: true and then trying to use it, by trying to save, which should error out as state store never got configured successfully. 

## OIDC private_key_jwt authentication (`useOIDC`):
Redis cannot validate JWTs itself, so the test runs a mock OAuth2 token endpoint that cryptographically verifies the client assertion (RS256 signature against the client certificate, `kid` header, `aud` as a JSON string, `resource` and `scope` form parameters) before issuing tokens, and configures the Redis ACL to require those tokens as the `AUTH` password:
1. Unauthenticated access is rejected after the ACL is applied.
2. CRUD operations succeed authenticated with the initial access token.
3. The identity provider rotates to a second token; the background refresh fetches it and re-authenticates the connection pool (verified by continued CRUD operations and the second token request).
4. The initial token is revoked and the component's connections are killed; CRUD operations still succeed, proving new connections authenticate with the refreshed token rather than the one from init.
5. The client assertion wire format is asserted on the identity provider side.
6. A component configured with both `useOIDC` and `redisPassword` fails to initialize.

## Entra ID authentication (`useEntraID`):
The test serves a fake Azure App Service managed identity endpoint (`IDENTITY_ENDPOINT` and `IDENTITY_HEADER`), so the Azure SDK credential chain runs end to end without Azure. The endpoint issues JWTs whose `oid` claim is the Redis ACL username, and the ACL requires the issued token as the password:
1. CRUD operations succeed authenticated as the object ID with the initial token.
2. The endpoint rotates to a second token and the background refresh fetches it.
3. The initial token is revoked and the component's connections are killed; CRUD operations still succeed, proving new connections authenticate with the refreshed token (dapr/components-contrib#3554).
4. The component's connections are authenticated as the object ID, and the token was requested for the `https://redis.azure.com` resource.

## Private CA (`caCert`):
Redis serves TLS only (`docker-compose-tls.yml`), with a server certificate issued by a CA the test generates. A client that trusts only the system roots is refused, so each case tests the CA alone:
1. With `enableTLS` and `caCert` set to that CA, save, get and delete succeed.
2. With `enableTLS` but without `caCert`, the server certificate fails verification and the state store is not configured.
3. With `caCert` but without `enableTLS`, the component refuses the configuration and the state store is not configured.
