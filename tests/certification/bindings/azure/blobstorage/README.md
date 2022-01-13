# Azure Blobstorage Binding certification testing

This project aims to test the Azure Blobstorage binding component under various conditions.

## Test plan

### Authentication tests

* Authenticate with Azure Active Directory using Service Principal Client Secret
* Authenticate with Storage Account Key

### Operations tests

- Successful delete
- Delete File does not exist
- Successful Get
- Item does not exist
- No filename specified
- Filename specified
- Correct MD5 Hash Provided
- Create request with Invalid MD5 hash specified
- File already exists (overwrites content)
- List include custom metadata
- List include soft-deleted blobs
- List include snapshots
- List specify max results and use marker to retrieve more results
- List user prefix filtering
- Creating public blobs works

- TODO: Delete
  - No "deleteSnapshots" option despite snapshots
  - Verify snaphotOptions include/only

  - name: getBlobRetryCount
    value: <integer>

### Running the tests

This must be run in the GitHub Actions Workflow configured for test infrastructure setup.

If you have access to an Azure subscription you can run this locally on Mac or Linux after running `setup-azure-conf-test.sh` in `.github/infrastructure/conformance/azure` and then sourcing the generated bash rc file.