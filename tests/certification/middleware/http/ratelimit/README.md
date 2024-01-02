# Rate Limiter HTTP Middleware certification

The purpose of this module is to provide tests that certify the Rate Limiter HTTP Middleware as a stable component

## Test Plan

- Test rate-limiting against one sidecar configured with 10 rps limit:
    - Sending less than 10 rps should never trigger the rate-limiter
    - Sending more than 10 rps should be rate-limited to ~10 rps
- Rate-limiting per IP:
    - Specify a source IP with `X-Forwarded-For` and confirm requests are rate-limited per each IP
- Test rate-limiting against multiple sidecars, each configured with 10 rps limit:
    - Rate-limits are applied for each sidecar independently
