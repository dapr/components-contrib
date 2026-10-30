# AWS S3 Binding certification testing

This project aims to test the AWS S3 binding component under various conditions.

## Test plan

### Functional tests

- Create request:
  - key/Filename specified and missing
  - With ForcePathStyle True/False
  - Verifies automatic base64 decode option

- Get request:
  - Successful Get Request

- Delete request:
  - Successful deletion

- List request:
  - Successful List Request

### Running the tests

The GitHub Actions certification workflow configures the required test infrastructure.

The existing certification workflow also runs this same suite against Floci
using `components/floci/` and `DAPR_TEST_COMPONENT_PROFILE=floci`. The live-AWS
defaults are unchanged. Configure the intended backend before running the suite
locally.
