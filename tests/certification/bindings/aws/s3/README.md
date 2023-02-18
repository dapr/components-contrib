# AWS S3 Binding certification testing

This project aims to test the AWS S3 binding component under various conditions.

## Test plan

### Functional tests

- Delete request:
  - Successful deletion
  - File does not exist
  
- Get request:
  - Successful Get Request
  - Item does not exist

- Create request:
  - Filename specified
  - With ForcePathStyle True/False
  - Verifies automatic base64 decode option

- List request:
  - basic
  - filter results by specifying a prefix

### Running the tests

This must be run in the GitHub Actions Workflow configured for test infrastructure setup.
