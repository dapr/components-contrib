# Azure Conformance Tests Infrastructure

This folder contains helper scripts for maintainers to set up resources needed for conformance testing of Azure components.

> If you are running on Windows, all automation described in this document will need to be executed under [Git Bash](https://www.atlassian.com/git/tutorials/git-bash.)

## Prerequisites

To use the automation in this folder, you will need:

- A user account with [Owner permissions](https://docs.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#owner) to assign RBAC roles and manage all Azure resources under an [Azure Subscription](https://docs.microsoft.com/en-us/azure/cost-management-billing/manage/create-subscription#:~:text=Create%20a%20subscription%20in%20the%20Azure%20portal%201,the%20form%20for%20each%20type%20of%20billing%20account.).

- The [Azure CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli) installed.

## Setting up Azure resources for conformance tests

The `setup-azure-conf-test.sh` script will create and configure all the Azure resources necessary for the Azure conformance tests. You will need to login the Azure CLI first, and set the target account you want to run the deployment under:

```bash
az login
az account set -s "Name of Test Deployment Subscription"
./setup-azure-conf-test.sh --user="myazurealias@contoso.com" --location="EastUS"
```

> If you plan to run the `bindings.azure.eventgrid` test, you may also want to provide the `--ngrok-token` parameter at this point, or you will have to manually populate this value yourself in the test Key Vault or your local environment variable before your run the test.

For more details on the parameters that the script supports, run:

```bash
./setup-azure-conf-test.sh --help
```

By default, the script will prefix all resources it creates with your user alias, unless you provide it with an alternative `--prefix` string. It will generate several additional artifacts for your use after deployment to an output directory that defaults to `$HOME/azure-conf-test` unless otherwise specified by `--outpath` when running the script. Based on the example command for running the script, these would be:

- `myazurealias-azure-conf-test.json` is a copy of the Azure Resource Manager (ARM) template compiled from the `.bicep` files in this folder, and used to generate all the resources. It can be modified and re-deployed if desired for your own purposes.
- `myazurealias-teardown-conf-test.sh` is a script that will teardown all of the Azure resources that were created, including the test service principals and key vault.
- `myazurealias-conf-test-config.rc` contains all the environment variables needed to run the Azure conformance tests locally. **This file also contains various credentials used to access the resources.**
- `AzureKeyVaultSecretStoreCert.pfx` is a local copy of the cert for the Service Principal used in the `secretstore.azure.keyvault` conformance test. The path to this is referenced as part of the environment variables in the `*-conf-test-config.rc`.
- `AZURE_CREDENTIALS` contains the credentials for the Service Principal you can use to run the conformance test GitHub workflow against the created Azure resources.

### Deploying for use in GitHub workflows

If you are running the script to enable running the conformance test workflow in your fork of dapr/components-contrib, you will also need to run the `allow-github-ips-in-azuresql.py` script to allow the ports used by GitHub Actions through the test Azure SQL Server's firewall.

The script coalesces the IP addresses published by the GitHub meta API endpoint and adds them as firewall rules to the target SQL server, for example:

```bash
python3 allow-github-ips-in-azuresql.py --outpath ~/sql_firewall_settings --sqlserver "${AzureSqlServerName}" --resource-group "${AzureResourceGroupName}"
```

This script will also allow you to generate the template for adding the firewall rules without deploying them with the `--no-deployment` flag, so you can inspect the rules first as needed:

```bash
python3 allow-github-ips-in-azuresql.py --outpath ~/sql_firewall_settings --no-deployment
```

For more details on the parameters, run the script with the `--help` flag.

## Running Azure conformance tests locally

1. Apply all the environment variables needed to run the Azure conformance test from your device, by sourcing the generated `*-conf-test-config.rc` file. For example:

    ```bash
    source ~/azure-conf-test/myazurealias-conf-test-config.rc
    ```

2. Follow the [instructions for running individual conformance tests](../../../../tests/conformance/README.md#running-conformance-tests).

    > The `bindings.azure.eventgrid` test and others may require additional setup before running the conformance test, such as setting up non-Azure resources like an Ngrok endpoint. See [conformance.yml](../../../../.github/workflows/conformance.yml) for details.

    > The `state.azure.sql` test expects that the SQL Server firewall port has been opened to the test client. The GitHub workflow relies on all IPs used by GitHub Actions being allowed via the `allow-github-ips-in-azuresql.py` script, but when running locally, you will need to open the port to your client IP as indicated in the initial test failure message.
    >
    > ```bash
    > # Capture the blocked IP from the test failure
    > TEST_OUTPUT="$(go test -v -tags=conftests -count=1 ./tests/conformance -run=TestStateConformance/azure.sql)"
    > BLOCKED_IP=$(echo "$TEST_OUTPUT" | grep -Po "Client with IP address '\K[^']*")
    >
    > # Login to the account with Contributor access to the SQL server instance
    > az login
    > az sql server firewall-rule create --resource-group "$AzureResourceGroupName" --server "$AzureSqlServerName" -n "AllowTestClientIP" --start-ip-address "$BLOCKED_IP" --end-ip-address "$BLOCKED_IP"
    > ```

## Running Azure conformance tests via GitHub workflows

1. Fork the `dapr/components-contrib` repo.
2. In your fork on GitHub, [add the secrets](https://docs.github.com/en/actions/reference/encrypted-secrets#creating-encrypted-secrets-for-a-repository):

   - `AZURE_CREDENTIALS`: copy and paste the contents of the `AZURE_CREDENTIALS` file from the output folder of the script.
   - `AZURE_KEYVAULT`: copy and paste the value of the `AzureKeyVaultName` variable from the `*-conf-test-config.rc` file in the output folder of the script. This is usually of the form `<prefix>-conf-test-kv` where the _prefix_ string is the user name portion of the Azure account UPN by default, or the value specified using the `--prefix` parameter when running the script.

3. Under the Actions tab, enable the "Components Conformance Tests" workflow and [manually trigger the workflow](https://docs.github.com/en/actions/managing-workflow-runs/manually-running-a-workflow#running-a-workflow).

## Tearing down Azure resources after use

Run the teardown `<prefix>-teardown-conf-test.sh` script generated to the output path for `setup-azure-conf-test.sh`. Using the defaults from the setup example, this would be:

```bash
az login
az account set -s "Name of Test Deployment Subscription"
~/azure-conf-test/myazurealias-teardown-conf-test.sh
```

> ⚠ This will delete the resource group containing all the resources, remove the service principals that were created and purge the test Key Vault. **Purging the Key Vault is not a recoverable action, so do not run this script if you have modified it for other purposes.**
