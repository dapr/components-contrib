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

This must be run in the GitHub Actions Workflow configured for test infrastructure setup.
