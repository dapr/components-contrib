# Azure KeyVault certification testing

This project aims to test the Azure KeyVault Secret Store component under various conditions.

## Test plan

### Active Directory Authentication tests

* Authenticate with Azure Active Directory using Service Principal Certificate
* Authenticate with Azure Active Directory using Service Principal Client Secret
* TODO: Authenticate with Azure Active Directory using Azure Managed Identity (also known as Managed Service Identity)

### Other tests

* Client reconnects (if applicable) upon network interruption: Not required as no connection is being maintained.


### Running the tests

This must be run in the GitHub Actions Workflow configured for test infrastructure setup.

If you have access to an Azure subscription you can run this locally on Mac or Linux after running `setup-azure-conf-test.sh` in `.github/infrastructure/conformance/azure` and then sourcing the generated bash rc file.