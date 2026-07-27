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
4. The client assertion wire format is asserted on the identity provider side.
5. A component configured with both `useOIDC` and `redisPassword` fails to initialize.