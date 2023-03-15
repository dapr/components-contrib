# Rate Limiter HTTP Middleware certification

The purpose of this module is to provide tests that certify the Rate Limiter HTTP Middleware as a stable component

## Test Plan

1. Ensure that bearer tokens in the `Authorization` header are validated correctly, in two separate sidecars:
   - Tokens must begin with prefix `Bearer ` (case-insensitive)
   - JWTs must be present in the header and correctly-formatted
   - Do not validate invalid tokens: tokens that are expired, not yet valid ("nbf" claim), for the wrong issuer, or for the wrong audience
   - Ensure we allow some clock skew when validating time validity bounds
   - JWTs signed with `"alg": "none"` should be rejected ([context](https://auth0.com/blog/critical-vulnerabilities-in-json-web-token-libraries/))
2. Ensure the OpenID Connect document and JWKS keybag are fetched correctly:
   - If no JWKS URL is passed explicitly, the component should fetch the OpenID Configuration document depending on the value of the `issuer` metadata property
   - The `issuer` property in the OpenID Configuration document must match the `issuer` metadata property
   - Simulate failures in fetching the OpenID Configuration document or the JWKS keybag
