# SQL Server certification testing

This project aims to test the SQL Server State Store component under various conditions.

## Test plan

### SQL Injection

* Not prone to SQL injection on write
* Not prone to SQL injection on read
* Not prone to SQL injection on delete

### Indexed Properties

* Verifies Indices are created for each indexed property in component metadata
* Verifies JSON data properties are parsed and written to dedicated database columns

### Custom Properties

* Verifies the use of custom tablename (default is states)
* Verifies the use of a custom schema (default is dbo)

### Connection to different SQL Server types

* Verifies connection handling with Azure SQL Server
* Verifies connection handling with SQL Server in Docker to represent self hosted SQL Server options

### Other tests

* Client reconnects (if applicable) upon network interruption


### Running the tests

This must be run in the GitHub Actions Workflow configured for test infrastructure setup.

If you have access to an Azure subscription you can run this locally on Mac or Linux after running `setup-azure-conf-test.sh` in `.github/infrastructure/conformance/azure` and then sourcing the generated bash rc file.