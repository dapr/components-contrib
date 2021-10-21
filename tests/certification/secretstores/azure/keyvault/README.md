# Azure KeyVault certifcation testing

This project aims to test the Azure KeyVault State Store component under various conditions.

## Test plan

### Basic conformance tests

* Currently handled by TestSecretStoreConformance

### Authentication tests

* Authenticate with Azure Active Directory using Service Principal Certificate
    * Already handled by TestSecretStoreConformance/azure.keyvault.certificate
* Authenticate with Azure Active Directory using Service Principal Client Secret
    * Already handled by TestSecretStoreConformance/azure.keyvault.serviceprincipal
* Authenticate with Azure Active Directory using Azure Managed Identity (also known as Managed Service Identity)