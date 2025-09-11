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

## TTLs and cleanups

1. Correctly parse the `cleanupIntervalInSeconds` metadata property:
   - No value uses the default value (3600 seconds)
   - A positive value sets the interval to the given number of seconds
   - A zero or negative value disables the cleanup
2. The cleanup method deletes expired records and updates the metadata table with the last time it ran
3. The cleanup method doesn't run if the last iteration was less than `cleanupIntervalInSeconds` or if another process is doing the cleanup

### Other tests

* Client reconnects (if applicable) upon network interruption


### Running the tests

This must be run in the GitHub Actions Workflow configured for test infrastructure setup.

If you have access to an Azure subscription you can run this locally on Mac or Linux after running `setup-azure-conf-test.sh` in `.github/infrastructure/conformance/azure` and then sourcing the generated bash rc file.
