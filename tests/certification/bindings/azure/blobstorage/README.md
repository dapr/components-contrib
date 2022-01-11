# Azure Blobstorage Binding certification testing

This project aims to test the Azure Blobstorage binding component under various conditions.

## Test plan

### Authentication tests

* Authenticate with Azure Active Directory using Service Principal Client Secret
* Authenticate with Storage Account Key

### Other tests

- Successful delete
- Delete File does not exist
- Successful Get
- Item does not exist
- No filename specified
- Filename specified
- MD5 Hash Provided
- File already exists (overwrites content)


- TODO: List
  - List
  - Various options...
- TODO: Delete
  - No "deleteSnapshots" option despite snapshots
  - Verify snaphotOptions include/only
- TODO: Create
  - Incorrect MD5 Hash Provided
  - Incorrect Content Type provided
  - name: getBlobRetryCount
    value: <integer>
  - name: publicAccessLevel
    value: <publicAccessLevel>

### Running the tests

This must be run in the GitHub Actions Workflow configured for test infrastructure setup.

If you have access to an Azure subscription you can run this locally on Mac or Linux after running `setup-azure-conf-test.sh` in `.github/infrastructure/conformance/azure` and then sourcing the generated bash rc file.