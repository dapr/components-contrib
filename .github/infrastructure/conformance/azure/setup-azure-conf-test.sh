#!/usr/bin/env bash
# ------------------------------------------------------------
# Copyright 2021 The Dapr Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ------------------------------------------------------------

set -e

##==============================================================================
##
## Process command-line options:
##
##==============================================================================

ERR=0
for opt in "$@"
do
  case $opt in
    -\? | -h | --help)
        SHOW_USAGE=1
        ;;

    --location=*)
        DEPLOY_LOCATION="${opt#*=}"
        ;;

    --ngrok-token=*)
        NGROK_TOKEN="${opt#*=}"
        ;;

    --admin-id=*)
        ADMIN_ID="${opt#*=}"
        ;;

    --outpath=*)
        OUTPUT_PATH="${opt#*=}"
        ;;

    --user=*)
        ADMIN_UPN="${opt#*=}"
        ;;

    --prefix=*)
        PREFIX="${opt#*=}"
        ;;

    --credentials=*)
        CREDENTIALS_PATH="${opt#*=}"
        ;;
    *)
        echo "$0: Unknown option: $opt"
        ERR=1
        SHOW_USAGE=1
        ;;
  esac
done

if [[ -z ${ADMIN_UPN} && -z ${SHOW_USAGE} ]]; then
    echo "$0: --user must be specified"
    ERR=1
    SHOW_USAGE=1
fi

##==============================================================================
##
## Display help
##
##==============================================================================

if [[ ${SHOW_USAGE} -eq 1 ]]; then
    cat<<EOF
OVERVIEW:
Sets up Azure resources needed for conformance tests and populates the secrets
needed for the conformance.yml GitHub workflow to run. Also generates a .rc file
that can be used to set environment variables for the conformance test to be run
on the local device. The script aims to be idempotent for the same inputs and
can be rerun on failure, except that any auto-generated Service Principal
credentials will be rotated on rerun.

PREREQUISITES:
This script requires that the Azure CLI is installed, and the user is already
logged in and has already selected the subscription under which the new resources
will be deployed. For example:

    $ az login
    $ az account set -s "My Test Subscription"

USAGE:
    $ ./setup-azure-conf-test.sh --user=<Azure user UPN> [--credentials="..."] \
[--location="..."] [--prefix="..."] [--outpath="..."] [--ngrok-token="..."]

OPTIONS:
    -h, --help      Print this help message.
    --user          The UPN for the Azure user in the current subscription who
                    will own all created resources, e.g. "myalias@contoso.com".
    --credentials   Optional. The path to a file containing Azure credentials of
                    the Service Principal (SP) that will be running conformance
                    tests. The file should be the JSON output when the SP was
                    created with the Azure CLI 'az ad sp create-for-rbac'
                    command. If not specified, a new SP will be created, and its
                    credentials saved in the AZURE_CREDENTIALS file under the
                    --outpath.
    --location      Optional. The location for the Azure deployment. Defaults to
                    "WestUS2" if not specified.
    --prefix        Optional. 3-15 character string to prefix all created
                    resources. Defaults to the user name of the provided --user
                    UPN if not specified.
    --outpath       Optional. Path to write resulting config and resource files
                    into. Defaults to "~/azure-conf-test" if not specified.
    --ngrok-token   Optional. The Authtoken from your ngrok account, as found at
                    https://dashboard.ngrok.com/get-started/your-authtoken. Only
                    needed for setting AzureEventGridNgrokToken in the KeyVault,
                    which is used by the GitHub workflow for the EventGrid
                    bindings conformance test.
    --admin-id      Optional. Sets the Object ID of the admin user to the given
                    value. Useful if querying the directory using the UPN is
                    failing.
EOF
    exit $ERR
fi

##==============================================================================
##
## Setup default parameters
##
##==============================================================================

if [[ -z ${PREFIX} ]]; then
    PREFIX="$(echo "${ADMIN_UPN}" | sed -E 's/@.*|\.//g')"
    echo "INFO: Using user name as resource prefix: \"${PREFIX}\""
fi
if [[ -z ${OUTPUT_PATH} ]]; then
    OUTPUT_PATH="$HOME/azure-conf-test"
    echo "INFO: Using default output path: \"${OUTPUT_PATH}\""
fi
if [[ -z ${DEPLOY_LOCATION} ]]; then
    DEPLOY_LOCATION="WestUS2"
    echo "INFO: Using default deployment location: \"${DEPLOY_LOCATION}\""
fi
if [[ -z ${NGROK_TOKEN} ]]; then
    echo "WARN: --ngrok-token is not specified, will not set AzureEventGridNgrokToken used by GitHub workflow for bindings.azure.eventgrid conformance test."
fi
if [[ -z ${CREDENTIALS_PATH} ]]; then
    echo "INFO: --credentials is not specified, will generate a new service principal and credentials ..."
fi

echo
echo "Starting setup-azure-conf-test with the following parameters:"
echo "ADMIN_UPN=${ADMIN_UPN}"
echo "PREFIX=${PREFIX}"
echo "DEPLOY_LOCATION=${DEPLOY_LOCATION}"
echo "OUTPUT_PATH=${OUTPUT_PATH}"
echo "NGROK_TOKEN=${NGROK_TOKEN}"
echo "CREDENTIALS_PATH=${CREDENTIALS_PATH}"
echo "ADMIN_ID=${ADMIN_ID}"

##==============================================================================
##
## Setup Azure environment
##
##==============================================================================

# Constant environment variable names defined by tests or GitHub workflow
ACR_VAR_NAME="AzureContainerRegistryName"

CERTIFICATION_SERVICE_PRINCIPAL_CLIENT_SECRET_VAR_NAME="AzureCertificationServicePrincipalClientSecret"
CERTIFICATION_SERVICE_PRINCIPAL_CLIENT_ID_VAR_NAME="AzureCertificationServicePrincipalClientId"
CERTIFICATION_TENANT_ID_VAR_NAME="AzureCertificationTenantId"
CERTIFICATION_SUBSCRIPTION_ID_VAR_NAME="AzureCertificationSubscriptionId"

COSMOS_DB_VAR_NAME="AzureCosmosDB"
COSMOS_DB_COLLECTION_VAR_NAME="AzureCosmosDBCollection"
COSMOS_DB_MASTER_KEY_VAR_NAME="AzureCosmosDBMasterKey"
COSMOS_DB_URL_VAR_NAME="AzureCosmosDBUrl"
COSMOS_DB_TABLE_API_VAR_NAME="AzureCosmosDBTableAPI"
COSMOS_DB_TABLE_API_URL_VAR_NAME="AzureCosmosDBTableAPIUrl"
COSMOS_DB_TABLE_API_MASTER_KEY_VAR_NAME="AzureCosmosDBTableAPIMasterKey"

EVENT_GRID_ACCESS_KEY_VAR_NAME="AzureEventGridAccessKey"
EVENT_GRID_CLIENT_ID_VAR_NAME="AzureEventGridClientId"
EVENT_GRID_CLIENT_SECRET_VAR_NAME="AzureEventGridClientSecret"
EVENT_GRID_NGROK_TOKEN_VAR_NAME="AzureEventGridNgrokToken"
EVENT_GRID_SCOPE_VAR_NAME="AzureEventGridScope"
EVENT_GRID_SUB_ID_VAR_NAME="AzureEventGridSubscriptionId"
EVENT_GRID_TENANT_ID_VAR_NAME="AzureEventGridTenantId"
EVENT_GRID_TOPIC_ENDPOINT_VAR_NAME="AzureEventGridTopicEndpoint"

EVENT_HUBS_BINDINGS_CONNECTION_STRING_VAR_NAME="AzureEventHubsBindingsConnectionString"
EVENT_HUBS_BINDINGS_CONSUMER_GROUP_VAR_NAME="AzureEventHubsBindingsConsumerGroup"
EVENT_HUBS_BINDINGS_CONTAINER_VAR_NAME="AzureEventHubsBindingsContainer"
EVENT_HUBS_BINDINGS_NAMESPACE_VAR_NAME="AzureEventHubsBindingsNamespace"
EVENT_HUBS_BINDINGS_HUB_VAR_NAME="AzureEventHubsBindingsHub"
EVENT_HUBS_PUBSUB_CONNECTION_STRING_VAR_NAME="AzureEventHubsPubsubConnectionString"
EVENT_HUBS_PUBSUB_CONSUMER_GROUP_VAR_NAME="AzureEventHubsPubsubConsumerGroup"
EVENT_HUBS_PUBSUB_CONTAINER_VAR_NAME="AzureEventHubsPubsubContainer"
EVENT_HUBS_PUBSUB_NAMESPACE_VAR_NAME="AzureEventHubsPubsubNamespace"
EVENT_HUBS_PUBSUB_HUB_VAR_NAME="AzureEventHubsPubsubHub"
EVENT_HUBS_PUBSUB_NAMESPACE_CONNECTION_STRING_VAR_NAME="AzureEventHubsPubsubNamespaceConnectionString"
CERTIFICATION_EVENT_HUBS_PUBSUB_TOPICACTIVE_CONNECTION_STRING_VAR_NAME="AzureEventHubsPubsubTopicActiveConnectionString"
CERTIFICATION_EVENT_HUBS_PUBSUB_TOPICMULTI1_VAR_NAME="AzureEventHubsPubsubTopicMulti1Name"
CERTIFICATION_EVENT_HUBS_PUBSUB_TOPICMULTI2_VAR_NAME="AzureEventHubsPubsubTopicMulti2Name"

IOT_HUB_NAME_VAR_NAME="AzureIotHubName"
IOT_HUB_EVENT_HUB_CONNECTION_STRING_VAR_NAME="AzureIotHubEventHubConnectionString"
IOT_HUB_BINDINGS_CONSUMER_GROUP_VAR_NAME="AzureIotHubBindingsConsumerGroup"
IOT_HUB_PUBSUB_CONSUMER_GROUP_VAR_NAME="AzureIotHubPubsubConsumerGroup"

KEYVAULT_CERT_NAME="AzureKeyVaultCert"
KEYVAULT_CLIENT_ID_VAR_NAME="AzureKeyVaultClientId"
KEYVAULT_SERVICE_PRINCIPAL_CLIENT_SECRET_VAR_NAME="AzureKeyVaultServicePrincipalClientSecret"
KEYVAULT_SERVICE_PRINCIPAL_CLIENT_ID_VAR_NAME="AzureKeyVaultServicePrincipalClientId"
KEYVAULT_TENANT_ID_VAR_NAME="AzureKeyVaultTenantId"
KEYVAULT_NAME_VAR_NAME="AzureKeyVaultName"

RESOURCE_GROUP_NAME_VAR_NAME="AzureResourceGroupName"

SERVICE_BUS_CONNECTION_STRING_VAR_NAME="AzureServiceBusConnectionString"
SERVICE_BUS_NAMESPACE_VAR_NAME="AzureServiceBusNamespace"

SQL_SERVER_NAME_VAR_NAME="AzureSqlServerName"
SQL_SERVER_DB_NAME_VAR_NAME="AzureSqlServerDbName"
SQL_SERVER_CONNECTION_STRING_VAR_NAME="AzureSqlServerConnectionString"

AZURE_DB_POSTGRES_CONNSTRING_VAR_NAME="AzureDBPostgresConnectionString"
AZURE_DB_POSTGRES_CLIENT_ID_VAR_NAME="AzureDBPostgresClientId"
AZURE_DB_POSTGRES_CLIENT_SECRET_VAR_NAME="AzureDBPostgresClientSecret"
AZURE_DB_POSTGRES_TENANT_ID_VAR_NAME="AzureDBPostgresTenantId"

STORAGE_ACCESS_KEY_VAR_NAME="AzureBlobStorageAccessKey"
STORAGE_ACCOUNT_VAR_NAME="AzureBlobStorageAccount"
STORAGE_CONTAINER_VAR_NAME="AzureBlobStorageContainer"
STORAGE_QUEUE_VAR_NAME="AzureBlobStorageQueue"

AZURE_APP_CONFIG_NAME_VAR_NAME="AzureAppConfigName"

# Derived variables
if [[ -z "${ADMIN_ID}" ]]; then
    # If the user did not pass an admin ID, look it up in the directory
    ADMIN_ID="$(az ad user list --filter "userPrincipalName eq '${ADMIN_UPN}'" --query "[].id" --output tsv)"
    if [[ -z "${ADMIN_ID}" ]]; then
        echo "Could not find user with upn ${ADMIN_UPN}"
        exit 1
    fi
fi
SUB_ID="$(az account show --query "id" --output tsv)"
TENANT_ID="$(az account show --query "tenantId" --output tsv)"
DEPLOY_NAME="${PREFIX}-azure-conf-test"

# Setup output path
mkdir -p "${OUTPUT_PATH}"

# Configure Azure CLI to install azure-iot and other extensions without prompts
az config set extension.use_dynamic_install=yes_without_prompt

# Create Service Principals for use with the conformance tests
CERT_AUTH_SP_NAME="${PREFIX}-akv-conf-test-sp"
az ad sp create-for-rbac --name "${CERT_AUTH_SP_NAME}" --years 1
CERT_AUTH_SP_ID="$(az ad sp list --display-name "${CERT_AUTH_SP_NAME}" --query "[].id" --output tsv)"
echo "Created Service Principal for cert auth: ${CERT_AUTH_SP_NAME}"

if [[ -n ${CREDENTIALS_PATH} ]]; then
    SDK_AUTH_SP_INFO="$(cat ${CREDENTIALS_PATH})"
    SDK_AUTH_SP_APPID="$(echo "${SDK_AUTH_SP_INFO}" | jq -r '.clientId')"
    SDK_AUTH_SP_CLIENT_SECRET="$(echo "${SDK_AUTH_SP_INFO}" | jq -r '.clientSecret')"
    if [[ -z ${SDK_AUTH_SP_APPID} || -z ${SDK_AUTH_SP_CLIENT_SECRET} ]]; then
        echo "Invalid credentials JSON file. Contents should match output of 'az ad sp create-for-rbac' command."
        exit 1
    fi
    SDK_AUTH_SP_NAME="$(az ad sp show --id "${SDK_AUTH_SP_APPID}" --query "appDisplayName" --output tsv)"
    SDK_AUTH_SP_ID="$(az ad sp show --id "${SDK_AUTH_SP_APPID}" --query "id" --output tsv)"
    echo "Using Service Principal from ${CREDENTIALS_PATH} for SDK Auth: ${SDK_AUTH_SP_NAME} (ID: ${SDK_AUTH_SP_ID})"
else
    SDK_AUTH_SP_NAME="${PREFIX}-conf-test-runner-sp"
    SDK_AUTH_SP_INFO="$(az ad sp create-for-rbac --name "${SDK_AUTH_SP_NAME}" --sdk-auth --years 1)"
    SDK_AUTH_SP_APPID="$(echo "${SDK_AUTH_SP_INFO}" | jq -r '.clientId')"
    SDK_AUTH_SP_CLIENT_SECRET="$(echo "${SDK_AUTH_SP_INFO}" | jq -r '.clientSecret')"
    SDK_AUTH_SP_ID="$(az ad sp list --display-name "${SDK_AUTH_SP_NAME}" --query "[].id" --output tsv)"
    echo "${SDK_AUTH_SP_INFO}"
    echo "Created Service Principal for SDK Auth: ${SDK_AUTH_SP_NAME} (ID: ${SDK_AUTH_SP_ID})"
    AZURE_CREDENTIALS_FILENAME="${OUTPUT_PATH}/AZURE_CREDENTIALS"
    echo "${SDK_AUTH_SP_INFO}" > "${AZURE_CREDENTIALS_FILENAME}"
fi

# Generate new password for SQL Server admin
SQL_SERVER_ADMIN_PASSWORD=$(openssl rand -base64 32)

# Build the bicep template and deploy to Azure
az bicep install
ARM_TEMPLATE_FILE="${OUTPUT_PATH}/${PREFIX}-azure-conf-test.json"
echo "Building conf-test-azure.bicep to ${ARM_TEMPLATE_FILE} ..."
az bicep build --file conf-test-azure.bicep --outfile "${ARM_TEMPLATE_FILE}"

echo "Creating azure deployment ${DEPLOY_NAME} in ${DEPLOY_LOCATION} and resource prefix ${PREFIX}-* ..."
az deployment sub create \
  --name "${DEPLOY_NAME}" \
  --location "${DEPLOY_LOCATION}" \
  --template-file "${ARM_TEMPLATE_FILE}" \
  -p namePrefix="${PREFIX}" \
  -p adminId="${ADMIN_ID}" \
  -p certAuthSpId="${CERT_AUTH_SP_ID}" \
  -p sdkAuthSpId="${SDK_AUTH_SP_ID}" \
  -p sdkAuthSpName="${SDK_AUTH_SP_NAME}" \
  -p rgLocation="${DEPLOY_LOCATION}" \
  -p sqlServerAdminPassword="${SQL_SERVER_ADMIN_PASSWORD}"

echo "Sleeping for 5s to allow created ARM deployment info to propagate to query endpoints ..."
sleep 5

# Query the deployed resource names from the bicep deployment outputs
echo "Querying deployed resource names ..."
RESOURCE_GROUP_NAME="$(az deployment sub show --name "${DEPLOY_NAME}" --query "properties.outputs.confTestRgName.value" --output tsv)"
echo "INFO: RESOURCE_GROUP_NAME=${RESOURCE_GROUP_NAME}"
SERVICE_BUS_NAME="$(az deployment sub show --name "${DEPLOY_NAME}" --query "properties.outputs.serviceBusName.value" --output tsv)"
echo "INFO: SERVICE_BUS_NAME=${SERVICE_BUS_NAME}"
KEYVAULT_NAME="$(az deployment sub show --name "${DEPLOY_NAME}" --query "properties.outputs.keyVaultName.value" --output tsv)"
echo "INFO: KEYVAULT_NAME=${KEYVAULT_NAME}"
STORAGE_NAME="$(az deployment sub show --name "${DEPLOY_NAME}" --query "properties.outputs.storageName.value" --output tsv)"
echo "INFO: STORAGE_NAME=${STORAGE_NAME}"
COSMOS_DB_NAME="$(az deployment sub show --name "${DEPLOY_NAME}" --query "properties.outputs.cosmosDbName.value" --output tsv)"
echo "INFO: COSMOS_DB_NAME=${COSMOS_DB_NAME}"
COSMOS_DB_SQL_NAME="$(az deployment sub show --name "${DEPLOY_NAME}" --query "properties.outputs.cosmosDbSqlName.value" --output tsv)"
echo "INFO: COSMOS_DB_SQL_NAME=${COSMOS_DB_SQL_NAME}"
COSMOS_DB_TABLE_API_NAME="$(az deployment sub show --name "${DEPLOY_NAME}" --query "properties.outputs.cosmosDbTableAPIName.value" --output tsv)"
echo "INFO: COSMOS_DB_TABLE_API_NAME=${COSMOS_DB_TABLE_API_NAME}"
COSMOS_DB_CONTAINER_NAME="$(az deployment sub show --name "${DEPLOY_NAME}" --query "properties.outputs.cosmosDbSqlContainerName.value" --output tsv)"
echo "INFO: COSMOS_DB_CONTAINER_NAME=${COSMOS_DB_CONTAINER_NAME}"
EVENT_GRID_TOPIC_NAME="$(az deployment sub show --name "${DEPLOY_NAME}" --query "properties.outputs.eventGridTopicName.value" --output tsv)"
echo "INFO: EVENT_GRID_TOPIC_NAME=${EVENT_GRID_TOPIC_NAME}"
EVENT_HUBS_NAMESPACE="$(az deployment sub show --name "${DEPLOY_NAME}" --query "properties.outputs.eventHubsNamespace.value" --output tsv)"
echo "INFO: EVENT_HUBS_NAMESPACE=${EVENT_HUBS_NAMESPACE}"
EVENT_HUB_BINDINGS_NAME="$(az deployment sub show --name "${DEPLOY_NAME}" --query "properties.outputs.eventHubBindingsName.value" --output tsv)"
echo "INFO: EVENT_HUB_BINDINGS_NAME=${EVENT_HUB_BINDINGS_NAME}"
EVENT_HUB_BINDINGS_POLICY_NAME="$(az deployment sub show --name "${DEPLOY_NAME}" --query "properties.outputs.eventHubBindingsPolicyName.value" --output tsv)"
echo "INFO: EVENT_HUB_BINDINGS_POLICY_NAME=${EVENT_HUB_BINDINGS_POLICY_NAME}"
EVENT_HUBS_BINDINGS_CONSUMER_GROUP_NAME="$(az deployment sub show --name "${DEPLOY_NAME}" --query "properties.outputs.eventHubBindingsConsumerGroupName.value" --output tsv)"
echo "INFO: EVENT_HUBS_BINDINGS_CONSUMER_GROUP_NAME=${EVENT_HUBS_BINDINGS_CONSUMER_GROUP_NAME}"
EVENT_HUB_PUBSUB_NAME="$(az deployment sub show --name "${DEPLOY_NAME}" --query "properties.outputs.eventHubPubsubName.value" --output tsv)"
echo "INFO: EVENT_HUB_PUBSUB_NAME=${EVENT_HUB_PUBSUB_NAME}"
EVENT_HUB_PUBSUB_POLICY_NAME="$(az deployment sub show --name "${DEPLOY_NAME}" --query "properties.outputs.eventHubPubsubPolicyName.value" --output tsv)"
echo "INFO: EVENT_HUB_PUBSUB_POLICY_NAME=${EVENT_HUB_PUBSUB_POLICY_NAME}"
EVENT_HUBS_PUBSUB_CONSUMER_GROUP_NAME="$(az deployment sub show --name "${DEPLOY_NAME}" --query "properties.outputs.eventHubPubsubConsumerGroupName.value" --output tsv)"
echo "INFO: EVENT_HUBS_PUBSUB_CONSUMER_GROUP_NAME=${EVENT_HUBS_PUBSUB_CONSUMER_GROUP_NAME}"

#begin : certifications pubsub.azure.eventhubs

EVENT_HUBS_PUB_SUB_NAMESPACE_POLICY_NAME="$(az deployment sub show --name "${DEPLOY_NAME}" --query "properties.outputs.eventHubsNamespacePolicyName.value" --output tsv)"
echo "INFO: EVENT_HUBS_PUB_SUB_NAMESPACE_POLICY_NAME=${EVENT_HUBS_PUB_SUB_NAMESPACE_POLICY_NAME}"

CERTIFICATION_EVENT_HUB_PUB_SUB_TOPICACTIVE_NAME="$(az deployment sub show --name "${DEPLOY_NAME}" --query "properties.outputs.certificationEventHubPubsubTopicActiveName.value" --output tsv)"
echo "INFO: CERTIFICATION_EVENT_HUB_PUB_SUB_TOPICACTIVE_NAME=${CERTIFICATION_EVENT_HUB_PUB_SUB_TOPICACTIVE_NAME}"
CERTIFICATION_EVENT_HUB_PUB_SUB_TOPICACTIVE_POLICY_NAME="$(az deployment sub show --name "${DEPLOY_NAME}" --query "properties.outputs.certificationEventHubPubsubTopicActivePolicyName.value" --output tsv)"
echo "INFO: CERTIFICATION_EVENT_HUB_PUB_SUB_TOPICACTIVE_POLICY_NAME=${CERTIFICATION_EVENT_HUB_PUB_SUB_TOPICACTIVE_POLICY_NAME}"

CERTIFICATION_EVENT_HUB_PUB_SUB_TOPICMULTI1_NAME="$(az deployment sub show --name "${DEPLOY_NAME}" --query "properties.outputs.certificationEventHubPubsubTopicMulti1Name.value" --output tsv)"
echo "INFO: CERTIFICATION_EVENT_HUB_PUB_SUB_TOPICMULTI1_NAME=${CERTIFICATION_EVENT_HUB_PUB_SUB_TOPICMULTI1_NAME}"
CERTIFICATION_EVENT_HUB_PUB_SUB_TOPICMULTI2_NAME="$(az deployment sub show --name "${DEPLOY_NAME}" --query "properties.outputs.certificationEventHubPubsubTopicMulti2Name.value" --output tsv)"
echo "INFO: CERTIFICATION_EVENT_HUB_PUB_SUB_TOPICMULTI2_NAME=${CERTIFICATION_EVENT_HUB_PUB_SUB_TOPICMULTI2_NAME}"
#end

AZURE_APP_CONFIG_NAME="$(az deployment sub show --name "${DEPLOY_NAME}" --query "properties.outputs.appconfigName.value" --output tsv)"

IOT_HUB_NAME="$(az deployment sub show --name "${DEPLOY_NAME}" --query "properties.outputs.iotHubName.value" --output tsv)"
echo "INFO: IOT_HUB_NAME=${IOT_HUB_NAME}"
IOT_HUB_BINDINGS_CONSUMER_GROUP_FULLNAME="$(az deployment sub show --name "${DEPLOY_NAME}" --query "properties.outputs.iotHubBindingsConsumerGroupName.value" --output tsv)"
echo "INFO: IOT_HUB_BINDINGS_CONSUMER_GROUP_FULLNAME=${IOT_HUB_BINDINGS_CONSUMER_GROUP_FULLNAME}"
IOT_HUB_PUBSUB_CONSUMER_GROUP_FULLNAME="$(az deployment sub show --name "${DEPLOY_NAME}" --query "properties.outputs.iotHubPubsubConsumerGroupName.value" --output tsv)"
echo "INFO: IOT_HUB_PUBSUB_CONSUMER_GROUP_FULLNAME=${IOT_HUB_PUBSUB_CONSUMER_GROUP_FULLNAME}"
SQL_SERVER_NAME="$(az deployment sub show --name "${DEPLOY_NAME}" --query "properties.outputs.sqlServerName.value" --output tsv)"
echo "INFO: SQL_SERVER_NAME=${SQL_SERVER_NAME}"
SQL_SERVER_ADMIN_NAME="$(az deployment sub show --name "${DEPLOY_NAME}" --query "properties.outputs.sqlServerAdminName.value" --output tsv)"
echo "INFO: SQL_SERVER_ADMIN_NAME=${SQL_SERVER_ADMIN_NAME}"
# Azure Container Registry is not currently needed.
# If needed again, look at https://github.com/dapr/components-contrib/tree/a8133088467fc29e1929a5dab396b11cf123a38b/.github/infrastructure

# Give the service principal used by the SDK write access to the entire resource group
MSYS_NO_PATHCONV=1 az role assignment create --assignee "${SDK_AUTH_SP_ID}" --role "Contributor" --scope "/subscriptions/${SUB_ID}/resourceGroups/${RESOURCE_GROUP_NAME}"

# Create Identity if it doesn't exist
# We use the standard name "azure-managed-identity" for the identity so we can easily query for it later using the CLI
if az identity show -g ${RESOURCE_GROUP_NAME} -n azure-managed-identity --query id -otsv; then
    echo "Reusing Identity azure-managed-identity"
    MANAGED_IDENTITY_SP="$(az identity show -g ${RESOURCE_GROUP_NAME} -n azure-managed-identity --query principalId -otsv)"
else
    echo "Creating Identity azure-managed-identity"
    MANAGED_IDENTITY_SP="$(az identity create -g ${RESOURCE_GROUP_NAME} -n azure-managed-identity --location ${DEPLOY_LOCATION} --query principalId -otsv)"
    # This identity can later be injected into services for managed identity authentication
fi

MANAGED_IDENTITY_ID="$(az identity show -g ${RESOURCE_GROUP_NAME} -n azure-managed-identity --query id -otsv)"
echo "Created Identity ${MANAGED_IDENTITY_ID}"

# Example to inject the identity into a supported Azure service (may be necessary in integration tests):
# az container create -g ${RESOURCE_GROUP_NAME} -n testcontainer --image golang:latest --command-line "tail -f /dev/null" --assign-identity $MANAGED_IDENTITY_ID

echo "Granting identity azure-managed-identity permissions to access the Key Vault ${KEYVAULT_NAME}"
az keyvault set-policy --name "${KEYVAULT_NAME}" -g "${RESOURCE_GROUP_NAME}" --secret-permissions get list --certificate-permissions get list --key-permissions all --object-id "${MANAGED_IDENTITY_SP}"
# Other tests verifying managed identity will want to grant permission like so:
# MSYS_NO_PATHCONV=1 az role assignment create --assignee-object-id "${MANAGED_IDENTITY_SP}" --assignee-principal-type ServicePrincipal --role "Azure Service Bus Data Owner" --scope "/subscriptions/${SUB_ID}/resourceGroups/${RESOURCE_GROUP_NAME}/providers/Microsoft.ServiceBus/namespaces/${SERVICE_BUS_NAME}"

# Creating service principal for service principal authentication with KeyVault
AKV_SPAUTH_SP_NAME="${PREFIX}-akv-spauth-conf-test-sp"
echo "Creating service principal ${AKV_SPAUTH_SP_NAME} for use with KeyVault ${KEYVAULT_NAME}"
{ read AKV_SPAUTH_SP_CLIENT_ID ; read AKV_SPAUTH_SP_CLIENT_SECRET ; } <  <(az ad sp create-for-rbac --name ${AKV_SPAUTH_SP_NAME} --years 1 --query "[appId,password]" -otsv)

# Give the service principal read access to the KeyVault Secrets
AKV_SPAUTH_SP_OBJECTID="$(az ad sp show --id ${AKV_SPAUTH_SP_CLIENT_ID} --query id -otsv)"
az keyvault set-policy --name "${KEYVAULT_NAME}" -g "${RESOURCE_GROUP_NAME}" --secret-permissions get list --certificate-permissions get list --key-permissions all --object-id "${AKV_SPAUTH_SP_OBJECTID}"

# Update service principal credentials and roles for created resources
echo "Creating ${CERT_AUTH_SP_NAME} certificate ..."
az ad sp credential reset --id "${CERT_AUTH_SP_ID}" --create-cert --cert "${KEYVAULT_CERT_NAME}" --keyvault "${KEYVAULT_NAME}"

# Add an EventGrid role to the SDK auth Service Principal so that it can be reused for the EventGrid binding conformance tests.
EVENT_GRID_SCOPE="/subscriptions/${SUB_ID}/resourceGroups/${RESOURCE_GROUP_NAME}/providers/Microsoft.EventGrid/topics/${EVENT_GRID_TOPIC_NAME}"
# MSYS_NO_PATHCONV is needed to prevent MSYS in Git Bash from converting the scope string to a local filesystem path and is benign on Linux
echo "Assigning \"EventGrid EventSubscription Contributor\" role to ${SDK_AUTH_SP_NAME} in scope \"${EVENT_GRID_SCOPE}\"..."
MSYS_NO_PATHCONV=1 az role assignment create --assignee "${SDK_AUTH_SP_ID}" --role "EventGrid EventSubscription Contributor" --scope "${EVENT_GRID_SCOPE}"

# The following lines have been commented out because not all steps can be performed with the Azure CLI yet, so we need to use PowerShell instead
# However, saving the code here to get a headstart for the future
# Creates Azure Event Grid service principal if not exists
# echo "Creating service principal for Microsoft.EventGrid"
# # Note this appId is a "constant"
# EVENTGRID_SP_ID=$(az ad sp list --filter "appId eq '4962773b-9cdb-44cf-a8bf-237846a00ab7'" --query "[].id" --output tsv)
# if [[ -z "${EVENTGRID_SP_ID}" ]]; then
#     az ad sp create --id "4962773b-9cdb-44cf-a8bf-237846a00ab7"
#     EVENTGRID_SP_ID=$(az ad sp list --filter "appId eq '4962773b-9cdb-44cf-a8bf-237846a00ab7'" --query "[].id" --output tsv)
#     echo "Service Principal Microsoft.EventGrid created with ID ${EVENTGRID_SP_ID}"
# else
#     echo "Service Principal Microsoft.EventGrid already exists with ID ${EVENTGRID_SP_ID}"
# fi
#
# # Assign the required app role to the SDK auth Service Principal so it can be used with EventGrid binding conformance tests.
# echo "Assigning \"AzureEventGridSecureWebhookSubscriber\" app role to app ${SDK_AUTH_SP_NAME} (${SDK_AUTH_SP_APPID}) if it doesn't already exist"
# az ad app show --id "${SDK_AUTH_SP_APPID}" | \
#     jq -er '.appRoles | map(select(.value == "AzureEventGridSecureWebhookSubscriber"))[0] | .id' && \
#     echo "App role already exists" || \
#     az ad app update --id "${SDK_AUTH_SP_APPID}" --app-roles '[{"allowedMemberTypes":["User","Application"],"description":"Azure Event Grid Role","displayName":"AzureEventGridSecureWebhookSubscriber","isEnabled":true,"value":"AzureEventGridSecureWebhookSubscriber"}]'

# Add Contributor role to the SDK auth Service Principal so it can add devices to the Azure IoT Hub for tests.
IOT_HUB_SCOPE="/subscriptions/${SUB_ID}/resourceGroups/${RESOURCE_GROUP_NAME}/providers/Microsoft.Devices/IotHubs/${IOT_HUB_NAME}"
echo "Assigning \"Contributor\" role to ${SDK_AUTH_SP_NAME} in scope \"${IOT_HUB_SCOPE}\"..."
MSYS_NO_PATHCONV=1 az role assignment create --assignee "${SDK_AUTH_SP_ID}" --role "Contributor" --scope "${IOT_HUB_SCOPE}"

# Add SQL Server Contributor role to the SDK auth Service Principal so it can update firewall rules to run sqlserver state conformance tests.
SQL_SERVER_SCOPE="/subscriptions/${SUB_ID}/resourceGroups/${RESOURCE_GROUP_NAME}/providers/Microsoft.Sql/servers/${SQL_SERVER_NAME}"
echo "Assigning \"Contributor\" role to ${SDK_AUTH_SP_NAME} in scope \"${SQL_SERVER_SCOPE}\"..."
MSYS_NO_PATHCONV=1 az role assignment create --assignee "${SDK_AUTH_SP_ID}" --role "Contributor" --scope "${SQL_SERVER_SCOPE}"

##==============================================================================
##
## Create output files for environment config and teardown of conformance tests
##
##==============================================================================

# Create script for teardown of created azure resources.
TEARDOWN_SCRIPT_NAME="${OUTPUT_PATH}/${PREFIX}-teardown-conf-test.sh"
tee "${TEARDOWN_SCRIPT_NAME}" > /dev/null \
<< EOF
#!/usr/bin/env bash
# ------------------------------------------------------------
# Copyright 2021 The Dapr Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ------------------------------------------------------------
set +e
echo "Deleting deployment ${DEPLOY_NAME} ..."
az deployment sub delete --name "${DEPLOY_NAME}"
echo "Deleting resource group ${RESOURCE_GROUP_NAME} ..."
az group delete --name "${RESOURCE_GROUP_NAME}" --yes
echo "Purging key vault ${KEYVAULT_NAME} ..."
az keyvault purge --name "${KEYVAULT_NAME}"
echo "Deleting service principal ${CERT_AUTH_SP_NAME} ..."
az ad sp delete --id "${CERT_AUTH_SP_ID}"
echo "Deleting service principal ${AKV_SPAUTH_SP_NAME} ..."
az ad sp delete --id "${AKV_SPAUTH_SP_OBJECTID}"
EOF

# Only remove the test runner Service Principal if it was not pre-existing
if [[ -z ${CREDENTIALS_PATH} ]]; then
    echo "echo \"Deleting service principal ${SDK_AUTH_SP_NAME} ...\"" >> "${TEARDOWN_SCRIPT_NAME}"
    echo "az ad sp delete --id \"${SDK_AUTH_SP_ID}\"" >> "${TEARDOWN_SCRIPT_NAME}"
fi
echo "echo \"INFO: ${PREFIX}-teardown-conf-test completed.\"" >> "${TEARDOWN_SCRIPT_NAME}"

chmod +x "${TEARDOWN_SCRIPT_NAME}"
echo "INFO: Created ${TEARDOWN_SCRIPT_NAME}."

# Initialize an environment variable file that can used with `source` for local execution of conformance tests.
ENV_CONFIG_FILENAME="${OUTPUT_PATH}/${PREFIX}-conf-test-config.rc"
tee "${ENV_CONFIG_FILENAME}" > /dev/null \
<< 'EOF'
#!/usr/bin/env bash
# ------------------------------------------------------------
# Copyright 2021 The Dapr Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ------------------------------------------------------------

if [[ -n ${NGROK_ENDPOINT} ]]; then
    export AzureEventGridSubscriberEndpoint="${NGROK_ENDPOINT}/api/events"
else
    echo "WARN: NGROK_ENDPOINT is not defined, AzureEventGridSubscriberEndpoint cannot be set for local testing of TestBindingsConformance/azure.eventgrid"
fi
EOF
chmod +x "${ENV_CONFIG_FILENAME}"
echo "INFO: Created ${ENV_CONFIG_FILENAME}."

##==============================================================================
##
## Populate Key Vault and config file with conformance test settings
##
##==============================================================================

# ---------------------------------
# Populate Key Vault test settings
# ---------------------------------
echo "Configuring Key Vault test settings ..."

KEYVAULT_CERT_FILE="${OUTPUT_PATH}/${KEYVAULT_CERT_NAME}.pfx"
if [ -e "${KEYVAULT_CERT_FILE}" ]; then
    rm "${KEYVAULT_CERT_FILE}"
fi
az keyvault secret download --vault-name "${KEYVAULT_NAME}" --name "${KEYVAULT_CERT_NAME}" --encoding base64 --file "${KEYVAULT_CERT_FILE}"
echo export ${KEYVAULT_CERT_NAME}=\"${KEYVAULT_CERT_FILE}\" >> "${ENV_CONFIG_FILENAME}"
# Note that the credential reset of the cert auth Service Principal has already pushed its cert to the Key Vault.

echo export ${KEYVAULT_NAME_VAR_NAME}=\"${KEYVAULT_NAME}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${KEYVAULT_NAME_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${KEYVAULT_NAME}"

KEYVAULT_TENANT_ID="$(az ad sp list --display-name "${CERT_AUTH_SP_NAME}" --query "[].appOwnerOrganizationId" --output tsv)"
echo export ${KEYVAULT_TENANT_ID_VAR_NAME}=\"${KEYVAULT_TENANT_ID}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${KEYVAULT_TENANT_ID_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${KEYVAULT_TENANT_ID}"

KEYVAULT_CLIENT_ID="$(az ad sp list --display-name "${CERT_AUTH_SP_NAME}" --query "[].appId" --output tsv)"
echo export ${KEYVAULT_CLIENT_ID_VAR_NAME}=\"${KEYVAULT_CLIENT_ID}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${KEYVAULT_CLIENT_ID_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${KEYVAULT_CLIENT_ID}"

KEYVAULT_SERVICE_PRINCIPAL_CLIENT_ID=${AKV_SPAUTH_SP_CLIENT_ID}
echo export ${KEYVAULT_SERVICE_PRINCIPAL_CLIENT_ID_VAR_NAME}=\"${KEYVAULT_SERVICE_PRINCIPAL_CLIENT_ID}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${KEYVAULT_SERVICE_PRINCIPAL_CLIENT_ID_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${KEYVAULT_SERVICE_PRINCIPAL_CLIENT_ID}"

KEYVAULT_SERVICE_PRINCIPAL_CLIENT_SECRET=${AKV_SPAUTH_SP_CLIENT_SECRET}
echo export ${KEYVAULT_SERVICE_PRINCIPAL_CLIENT_SECRET_VAR_NAME}=\"${KEYVAULT_SERVICE_PRINCIPAL_CLIENT_SECRET}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${KEYVAULT_SERVICE_PRINCIPAL_CLIENT_SECRET_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${KEYVAULT_SERVICE_PRINCIPAL_CLIENT_SECRET}"

# ------------------------------------
# Populate Blob Storage test settings
# ------------------------------------
echo "Configuring Blob Storage test settings ..."

echo export ${STORAGE_ACCOUNT_VAR_NAME}=\"${STORAGE_NAME}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${STORAGE_ACCOUNT_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${STORAGE_NAME}"

STORAGE_CONTAINER_NAME="${PREFIX}-conf-test-container"
echo export ${STORAGE_CONTAINER_VAR_NAME}=\"${STORAGE_CONTAINER_NAME}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${STORAGE_CONTAINER_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${STORAGE_CONTAINER_NAME}"

STORAGE_QUEUE_NAME="${PREFIX}-conf-test-queue"
echo export ${STORAGE_QUEUE_VAR_NAME}=\"${STORAGE_QUEUE_NAME}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${STORAGE_QUEUE_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${STORAGE_QUEUE_NAME}"

STORAGE_ACCESS_KEY="$(az storage account keys list --account-name "${STORAGE_NAME}" --query "[?keyName=='key1'].value" --output tsv)"
echo export ${STORAGE_ACCESS_KEY_VAR_NAME}=\"${STORAGE_ACCESS_KEY}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${STORAGE_ACCESS_KEY_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${STORAGE_ACCESS_KEY}"

# --------------------------------
# Populate CosmosDB test settings
# --------------------------------
echo "Configuring CosmosDB test settings ..."

echo export ${COSMOS_DB_VAR_NAME}=\"${COSMOS_DB_SQL_NAME}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${COSMOS_DB_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${COSMOS_DB_SQL_NAME}"

# Note that CosmosDB maps SQL DB containers to collections
echo export ${COSMOS_DB_COLLECTION_VAR_NAME}=\"${COSMOS_DB_CONTAINER_NAME}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${COSMOS_DB_COLLECTION_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${COSMOS_DB_CONTAINER_NAME}"

COSMOS_DB_URL="$(az cosmosdb list --query "[?name=='${COSMOS_DB_NAME}'].documentEndpoint" --output tsv)"
echo export ${COSMOS_DB_URL_VAR_NAME}=\"${COSMOS_DB_URL}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${COSMOS_DB_URL_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${COSMOS_DB_URL}"

COSMOS_DB_MASTER_KEY="$(az cosmosdb keys list --name "${COSMOS_DB_NAME}" --resource-group "${RESOURCE_GROUP_NAME}" --query "primaryMasterKey" --output tsv)"
echo export ${COSMOS_DB_MASTER_KEY_VAR_NAME}=\"${COSMOS_DB_MASTER_KEY}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${COSMOS_DB_MASTER_KEY_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${COSMOS_DB_MASTER_KEY}"

# --------------------------------
# Populate CosmosDB test settings
# --------------------------------
echo "Configuring CosmosDB Table API test settings ..."

echo export ${COSMOS_DB_TABLE_API_VAR_NAME}=\"${COSMOS_DB_TABLE_API_NAME}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${COSMOS_DB_TABLE_API_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${COSMOS_DB_TABLE_API_NAME}"

COSMOS_DB_TABLE_API_URL="$(az cosmosdb list --query "[?name=='${COSMOS_DB_TABLE_API_NAME}'].documentEndpoint" --output tsv)"
echo export ${COSMOS_DB_TABLE_API_URL_VAR_NAME}=\"${COSMOS_DB_TABLE_API_URL}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${COSMOS_DB_TABLE_API_URL_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${COSMOS_DB_TABLE_API_URL}"

COSMOS_DB_TABLE_API_MASTER_KEY="$(az cosmosdb keys list --name "${COSMOS_DB_TABLE_API_NAME}" --resource-group "${RESOURCE_GROUP_NAME}" --query "primaryMasterKey" --output tsv)"
echo export ${COSMOS_DB_TABLE_API_MASTER_KEY_VAR_NAME}=\"${COSMOS_DB_TABLE_API_MASTER_KEY}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${COSMOS_DB_TABLE_API_MASTER_KEY_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${COSMOS_DB_TABLE_API_MASTER_KEY}"


# ----------------------------------
# Populate Event Grid test settings
# ----------------------------------
echo "Configuring Event Grid test settings ..."

EVENT_GRID_ACCESS_KEY="$(az eventgrid topic key list --name "${EVENT_GRID_TOPIC_NAME}" --resource-group "${RESOURCE_GROUP_NAME}" --query "key1" --output tsv)"
echo export ${EVENT_GRID_ACCESS_KEY_VAR_NAME}=\"${EVENT_GRID_ACCESS_KEY}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${EVENT_GRID_ACCESS_KEY_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${EVENT_GRID_ACCESS_KEY}"

SDK_AUTH_SP_APP_ID="$(az ad sp list --display-name "${SDK_AUTH_SP_NAME}" --query "[].appId" --output tsv)"
echo export ${EVENT_GRID_CLIENT_ID_VAR_NAME}=\"${SDK_AUTH_SP_APP_ID}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${EVENT_GRID_CLIENT_ID_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${SDK_AUTH_SP_APP_ID}"

echo export ${EVENT_GRID_CLIENT_SECRET_VAR_NAME}=\"${SDK_AUTH_SP_CLIENT_SECRET}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${EVENT_GRID_CLIENT_SECRET_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${SDK_AUTH_SP_CLIENT_SECRET}"

if [[ -n ${NGROK_TOKEN} ]]; then
    echo export ${EVENT_GRID_NGROK_TOKEN_VAR_NAME}=\"${NGROK_TOKEN}\" >> "${ENV_CONFIG_FILENAME}"
    az keyvault secret set --name "${EVENT_GRID_NGROK_TOKEN_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${NGROK_TOKEN}"
else
    echo "WARN: NGROK_TOKEN not specified, AzureEventGridNgrokToken secret needs to be manually added to ${KEYVAULT_NAME} before running the GitHub conformance test workflow."
fi

echo export ${EVENT_GRID_SCOPE_VAR_NAME}=\"${EVENT_GRID_SCOPE}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${EVENT_GRID_SCOPE_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${EVENT_GRID_SCOPE}"

echo export ${EVENT_GRID_SUB_ID_VAR_NAME}=\"${SUB_ID}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${EVENT_GRID_SUB_ID_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${SUB_ID}"

echo export ${EVENT_GRID_TENANT_ID_VAR_NAME}=\"${TENANT_ID}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${EVENT_GRID_TENANT_ID_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${TENANT_ID}"

EVENT_GRID_TOPIC_ENDPOINT="$(az eventgrid topic list --query "[?name=='${EVENT_GRID_TOPIC_NAME}'].endpoint" --output tsv)"
echo export ${EVENT_GRID_TOPIC_ENDPOINT_VAR_NAME}=\"${EVENT_GRID_TOPIC_ENDPOINT}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${EVENT_GRID_TOPIC_ENDPOINT_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${EVENT_GRID_TOPIC_ENDPOINT}"

# -----------------------------------
# Populate Service Bus test settings
# -----------------------------------
echo "Configuring Service Bus test settings ..."
SERVICE_BUS_CONNECTION_STRING="$(az servicebus namespace authorization-rule keys list --name RootManageSharedAccessKey --namespace-name "${SERVICE_BUS_NAME}" --resource-group "${RESOURCE_GROUP_NAME}" --query "primaryConnectionString" --output tsv)"
echo export ${SERVICE_BUS_CONNECTION_STRING_VAR_NAME}=\"${SERVICE_BUS_CONNECTION_STRING}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${SERVICE_BUS_CONNECTION_STRING_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${SERVICE_BUS_CONNECTION_STRING}"
SERVICE_BUS_NAMESPACE="${SERVICE_BUS_NAME}.servicebus.windows.net"
echo export ${SERVICE_BUS_NAMESPACE_VAR_NAME}=\"${SERVICE_BUS_NAMESPACE}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${SERVICE_BUS_NAMESPACE_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${SERVICE_BUS_NAMESPACE}"

# ----------------------------------
# Populate SQL Server test settings
# ----------------------------------
echo "Configuring SQL Server test settings ..."

# Not specific to SQL server, but this is currently only consumed by setting SQL server firewall rules 
echo export ${RESOURCE_GROUP_NAME_VAR_NAME}=\"${RESOURCE_GROUP_NAME}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${RESOURCE_GROUP_NAME_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${RESOURCE_GROUP_NAME}"

echo export ${SQL_SERVER_NAME_VAR_NAME}=\"${SQL_SERVER_NAME}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${SQL_SERVER_NAME_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${SQL_SERVER_NAME}"

# Export a default value for DB name to be used when running conformance test locally.
# This is not added to the keyvault as the conformance.yml workflow generates a unique DB name each time.
echo export ${SQL_SERVER_DB_NAME_VAR_NAME}=\"${PREFIX}SqlDb\" >> "${ENV_CONFIG_FILENAME}"

# Note that `az sql db show-connection-string` does not currently support a `go` --client type, so we construct our own here.
SQL_SERVER_CONNECTION_STRING="Server=${SQL_SERVER_NAME}.database.windows.net;port=1433;User ID=${SQL_SERVER_ADMIN_NAME};Password=${SQL_SERVER_ADMIN_PASSWORD};Encrypt=true;"
echo export ${SQL_SERVER_CONNECTION_STRING_VAR_NAME}=\"${SQL_SERVER_CONNECTION_STRING}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${SQL_SERVER_CONNECTION_STRING_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${SQL_SERVER_CONNECTION_STRING}"

# ----------------------------------
# Populate Azure Database for PostgreSQL test settings
# ----------------------------------
echo "Configuring Azure Database for PostgreSQL test settings ..."

AZURE_DB_POSTGRES_CONNSTRING="host=${PREFIX}-conf-test-pg.postgres.database.azure.com user=${SDK_AUTH_SP_NAME} port=5432 connect_timeout=30 database=dapr_test"
echo export ${AZURE_DB_POSTGRES_CONNSTRING_VAR_NAME}=\"${AZURE_DB_POSTGRES_CONNSTRING}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${AZURE_DB_POSTGRES_CONNSTRING_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${AZURE_DB_POSTGRES_CONNSTRING}"

echo export ${AZURE_DB_POSTGRES_CLIENT_ID_VAR_NAME}=\"${SDK_AUTH_SP_APPID}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${AZURE_DB_POSTGRES_CLIENT_ID_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${SDK_AUTH_SP_APPID}"

echo export ${AZURE_DB_POSTGRES_CLIENT_SECRET_VAR_NAME}=\"${SDK_AUTH_SP_CLIENT_SECRET}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${AZURE_DB_POSTGRES_CLIENT_SECRET_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${SDK_AUTH_SP_CLIENT_SECRET}"

echo export ${AZURE_DB_POSTGRES_TENANT_ID_VAR_NAME}=\"${TENANT_ID}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${AZURE_DB_POSTGRES_TENANT_ID_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${TENANT_ID}"

# ----------------------------------
# Populate Event Hubs test settings
# ----------------------------------
echo "Configuring Event Hub test settings ..."

EVENT_HUBS_BINDINGS_CONNECTION_STRING="$(az eventhubs eventhub authorization-rule keys list --name "${EVENT_HUB_BINDINGS_POLICY_NAME}" --namespace-name "${EVENT_HUBS_NAMESPACE}" --eventhub-name "${EVENT_HUB_BINDINGS_NAME}" --resource-group "${RESOURCE_GROUP_NAME}" --query "primaryConnectionString" --output tsv)"
echo export ${EVENT_HUBS_BINDINGS_CONNECTION_STRING_VAR_NAME}=\"${EVENT_HUBS_BINDINGS_CONNECTION_STRING}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${EVENT_HUBS_BINDINGS_CONNECTION_STRING_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${EVENT_HUBS_BINDINGS_CONNECTION_STRING}"

echo export ${EVENT_HUBS_BINDINGS_NAMESPACE_VAR_NAME}=\"${EVENT_HUBS_NAMESPACE}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${EVENT_HUBS_BINDINGS_NAMESPACE_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${EVENT_HUBS_NAMESPACE}"

echo export ${EVENT_HUBS_BINDINGS_HUB_VAR_NAME}=\"${EVENT_HUB_BINDINGS_NAME}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${EVENT_HUBS_BINDINGS_HUB_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${EVENT_HUB_BINDINGS_NAME}"

echo export ${EVENT_HUBS_BINDINGS_CONSUMER_GROUP_VAR_NAME}=\"${EVENT_HUBS_BINDINGS_CONSUMER_GROUP_NAME}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${EVENT_HUBS_BINDINGS_CONSUMER_GROUP_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${EVENT_HUBS_BINDINGS_CONSUMER_GROUP_NAME}"

EVENT_HUBS_BINDINGS_CONTAINER_NAME="${PREFIX}-eventhubs-bindings-container"
echo export ${EVENT_HUBS_BINDINGS_CONTAINER_VAR_NAME}=\"${EVENT_HUBS_BINDINGS_CONTAINER_NAME}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${EVENT_HUBS_BINDINGS_CONTAINER_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${EVENT_HUBS_BINDINGS_CONTAINER_NAME}"

EVENT_HUBS_PUBSUB_CONNECTION_STRING="$(az eventhubs eventhub authorization-rule keys list --name "${EVENT_HUB_PUBSUB_POLICY_NAME}" --namespace-name "${EVENT_HUBS_NAMESPACE}" --eventhub-name "${EVENT_HUB_PUBSUB_NAME}" --resource-group "${RESOURCE_GROUP_NAME}" --query "primaryConnectionString" --output tsv)"
echo export ${EVENT_HUBS_PUBSUB_CONNECTION_STRING_VAR_NAME}=\"${EVENT_HUBS_PUBSUB_CONNECTION_STRING}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${EVENT_HUBS_PUBSUB_CONNECTION_STRING_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${EVENT_HUBS_PUBSUB_CONNECTION_STRING}"

EVENT_HUBS_PUBSUB_NAMESPACE_CONNECTION_STRING="$(az eventhubs namespace authorization-rule keys list --name "${EVENT_HUBS_PUB_SUB_NAMESPACE_POLICY_NAME}" --namespace-name "${EVENT_HUBS_NAMESPACE}" --resource-group "${RESOURCE_GROUP_NAME}" --query "primaryConnectionString" --output tsv)"
echo export ${EVENT_HUBS_PUBSUB_NAMESPACE_CONNECTION_STRING_VAR_NAME}=\"${EVENT_HUBS_PUBSUB_NAMESPACE_CONNECTION_STRING}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${EVENT_HUBS_PUBSUB_NAMESPACE_CONNECTION_STRING_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${EVENT_HUBS_PUBSUB_NAMESPACE_CONNECTION_STRING}"

CERTIFICATION_EVENT_HUBS_PUBSUB_TOPICACTIVE_CONNECTION_STRING="$(az eventhubs eventhub authorization-rule keys list --name "${CERTIFICATION_EVENT_HUB_PUB_SUB_TOPICACTIVE_POLICY_NAME}" --namespace-name "${EVENT_HUBS_NAMESPACE}" --eventhub-name "${CERTIFICATION_EVENT_HUB_PUB_SUB_TOPICACTIVE_NAME}" --resource-group "${RESOURCE_GROUP_NAME}" --query "primaryConnectionString" --output tsv)"
echo export ${CERTIFICATION_EVENT_HUBS_PUBSUB_TOPICACTIVE_CONNECTION_STRING_VAR_NAME}=\"${CERTIFICATION_EVENT_HUBS_PUBSUB_TOPICACTIVE_CONNECTION_STRING}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${CERTIFICATION_EVENT_HUBS_PUBSUB_TOPICACTIVE_CONNECTION_STRING_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${CERTIFICATION_EVENT_HUBS_PUBSUB_TOPICACTIVE_CONNECTION_STRING}"

echo export ${CERTIFICATION_EVENT_HUBS_PUBSUB_TOPICMULTI1_VAR_NAME}=\"${CERTIFICATION_EVENT_HUB_PUB_SUB_TOPICMULTI1_NAME}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${CERTIFICATION_EVENT_HUBS_PUBSUB_TOPICMULTI1_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${CERTIFICATION_EVENT_HUB_PUB_SUB_TOPICMULTI1_NAME}"

echo export ${CERTIFICATION_EVENT_HUBS_PUBSUB_TOPICMULTI2_VAR_NAME}=\"${CERTIFICATION_EVENT_HUB_PUB_SUB_TOPICMULTI2_NAME}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${CERTIFICATION_EVENT_HUBS_PUBSUB_TOPICMULTI2_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${CERTIFICATION_EVENT_HUB_PUB_SUB_TOPICMULTI2_NAME}"

echo export ${EVENT_HUBS_PUBSUB_NAMESPACE_VAR_NAME}=\"${EVENT_HUBS_NAMESPACE}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${EVENT_HUBS_PUBSUB_NAMESPACE_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${EVENT_HUBS_NAMESPACE}"

echo export ${EVENT_HUBS_PUBSUB_HUB_VAR_NAME}=\"${EVENT_HUB_PUBSUB_NAME}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${EVENT_HUBS_PUBSUB_HUB_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${EVENT_HUB_PUBSUB_NAME}"

echo export ${EVENT_HUBS_PUBSUB_CONSUMER_GROUP_VAR_NAME}=\"${EVENT_HUBS_PUBSUB_CONSUMER_GROUP_NAME}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${EVENT_HUBS_PUBSUB_CONSUMER_GROUP_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${EVENT_HUBS_PUBSUB_CONSUMER_GROUP_NAME}"

EVENT_HUBS_PUBSUB_CONTAINER_NAME="${PREFIX}-eventhubs-pubsub-container"
echo export ${EVENT_HUBS_PUBSUB_CONTAINER_VAR_NAME}=\"${EVENT_HUBS_PUBSUB_CONTAINER_NAME}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${EVENT_HUBS_PUBSUB_CONTAINER_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${EVENT_HUBS_PUBSUB_CONTAINER_NAME}"

# ------------------------------
# Populate Azure App config info
# ------------------------------
echo export ${AZURE_APP_CONFIG_NAME_VAR_NAME}=\"${AZURE_APP_CONFIG_NAME}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${AZURE_APP_CONFIG_NAME_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${AZURE_APP_CONFIG_NAME}"
# ----------------------------------
# Populate IoT Hub test settings
# ----------------------------------
echo "Configuring IoT Hub test settings ..."

echo export ${IOT_HUB_NAME_VAR_NAME}=\"${IOT_HUB_NAME}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${IOT_HUB_NAME_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${IOT_HUB_NAME}"

IOT_HUB_EVENT_HUB_CONNECTION_STRING="$(az iot hub connection-string show -n ${IOT_HUB_NAME} --default-eventhub --policy-name service --query connectionString --output tsv)"
echo export ${IOT_HUB_EVENT_HUB_CONNECTION_STRING_VAR_NAME}=\"${IOT_HUB_EVENT_HUB_CONNECTION_STRING}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${IOT_HUB_EVENT_HUB_CONNECTION_STRING_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${IOT_HUB_EVENT_HUB_CONNECTION_STRING}"

IOT_HUB_BINDINGS_CONSUMER_GROUP_NAME="$(basename ${IOT_HUB_BINDINGS_CONSUMER_GROUP_FULLNAME})"
echo export ${IOT_HUB_BINDINGS_CONSUMER_GROUP_VAR_NAME}=\"${IOT_HUB_BINDINGS_CONSUMER_GROUP_NAME}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${IOT_HUB_BINDINGS_CONSUMER_GROUP_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${IOT_HUB_BINDINGS_CONSUMER_GROUP_NAME}"

IOT_HUB_PUBSUB_CONSUMER_GROUP_NAME="$(basename ${IOT_HUB_PUBSUB_CONSUMER_GROUP_FULLNAME})"
echo export ${IOT_HUB_PUBSUB_CONSUMER_GROUP_VAR_NAME}=\"${IOT_HUB_PUBSUB_CONSUMER_GROUP_NAME}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${IOT_HUB_PUBSUB_CONSUMER_GROUP_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${IOT_HUB_PUBSUB_CONSUMER_GROUP_NAME}"

# -----------------------------------------------------------------------
# CERTIFICATION TESTS: Create service principal and grant resource access
# ------------------------------------------------------------------------
CERTIFICATION_SPAUTH_SP_NAME="${PREFIX}-certification-spauth-conf-test-sp"
{ read CERTIFICATION_SPAUTH_SP_CLIENT_ID ; read CERTIFICATION_SPAUTH_SP_CLIENT_SECRET ; } <  <(az ad sp create-for-rbac --name ${CERTIFICATION_SPAUTH_SP_NAME} --years 1 --query "[appId,password]" -otsv)
CERTIFICATION_SPAUTH_SP_PRINCIPAL_ID="$(az ad sp list --display-name "${CERTIFICATION_SPAUTH_SP_NAME}" --query "[].id" --output tsv)"

# Give the service principal used for certification test access to the relevant data plane resources
# Cosmos DB
az cosmosdb sql role assignment create --account-name ${COSMOS_DB_NAME} --resource-group "${RESOURCE_GROUP_NAME}" --role-definition-name "Cosmos DB Built-in Data Contributor" --scope "/subscriptions/${SUB_ID}/resourceGroups/${RESOURCE_GROUP_NAME}/providers/Microsoft.DocumentDB/databaseAccounts/${COSMOS_DB_NAME}" --principal-id "${CERTIFICATION_SPAUTH_SP_PRINCIPAL_ID}"
# Storage
az role assignment create --assignee "${CERTIFICATION_SPAUTH_SP_PRINCIPAL_ID}" --role "Storage Blob Data Owner" --scope "/subscriptions/${SUB_ID}/resourceGroups/${RESOURCE_GROUP_NAME}/providers/Microsoft.Storage/storageAccounts/${STORAGE_NAME}"
az role assignment create --assignee "${CERTIFICATION_SPAUTH_SP_PRINCIPAL_ID}" --role "Storage Table Data Reader" --scope "/subscriptions/${SUB_ID}/resourceGroups/${RESOURCE_GROUP_NAME}/providers/Microsoft.Storage/storageAccounts/${STORAGE_NAME}"
# Event Hubs
az role assignment create --assignee "${CERTIFICATION_SPAUTH_SP_PRINCIPAL_ID}" --role "Azure Event Hubs Data Owner" --scope "/subscriptions/${SUB_ID}/resourceGroups/${RESOURCE_GROUP_NAME}/providers/Microsoft.EventHub/namespaces/${EVENT_HUBS_NAMESPACE}"
# IOT hub used in eventhubs certification test
az role assignment create --assignee "${CERTIFICATION_SPAUTH_SP_PRINCIPAL_ID}" --role "Owner" --scope "/subscriptions/${SUB_ID}/resourceGroups/${RESOURCE_GROUP_NAME}/providers/Microsoft.Devices/IotHubs/${IOT_HUB_NAME}"
az role assignment create --assignee "${CERTIFICATION_SPAUTH_SP_PRINCIPAL_ID}" --role "IoT Hub Data Contributor" --scope "/subscriptions/${SUB_ID}/resourceGroups/${RESOURCE_GROUP_NAME}/providers/Microsoft.Devices/IotHubs/${IOT_HUB_NAME}"
# Azure Service Bus
ASB_ID=$(az servicebus namespace show --resource-group "${RESOURCE_GROUP_NAME}" --name "${SERVICE_BUS_NAME}" --query "id" -otsv)
az role assignment create --assignee "${CERTIFICATION_SPAUTH_SP_PRINCIPAL_ID}" --role "Azure Service Bus Data Owner" --scope "${ASB_ID}"
# Azure App Config
az role assignment create --assignee "${CERTIFICATION_SPAUTH_SP_PRINCIPAL_ID}" --role "App Configuration Data Owner" --scope "/subscriptions/${SUB_ID}/resourceGroups/${RESOURCE_GROUP_NAME}/providers/Microsoft.AppConfiguration/configurationStores/${AZURE_APP_CONFIG_NAME}"

# Now export the service principal information
CERTIFICATION_TENANT_ID="$(az ad sp list --display-name "${CERTIFICATION_SPAUTH_SP_NAME}" --query "[].appOwnerOrganizationId" --output tsv)"
echo export ${CERTIFICATION_SERVICE_PRINCIPAL_CLIENT_ID_VAR_NAME}=\"${CERTIFICATION_SPAUTH_SP_CLIENT_ID}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${CERTIFICATION_SERVICE_PRINCIPAL_CLIENT_ID_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${CERTIFICATION_SPAUTH_SP_CLIENT_ID}"
echo export ${CERTIFICATION_SERVICE_PRINCIPAL_CLIENT_SECRET_VAR_NAME}=\"${CERTIFICATION_SPAUTH_SP_CLIENT_SECRET}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${CERTIFICATION_SERVICE_PRINCIPAL_CLIENT_SECRET_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${CERTIFICATION_SPAUTH_SP_CLIENT_SECRET}"
echo export ${CERTIFICATION_TENANT_ID_VAR_NAME}=\"${CERTIFICATION_TENANT_ID}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${CERTIFICATION_TENANT_ID_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${CERTIFICATION_TENANT_ID}"
echo export ${CERTIFICATION_SUBSCRIPTION_ID_VAR_NAME}=\"${SUB_ID}\" >> "${ENV_CONFIG_FILENAME}"
az keyvault secret set --name "${CERTIFICATION_SUBSCRIPTION_ID_VAR_NAME}" --vault-name "${KEYVAULT_NAME}" --value "${SUB_ID}"

# ---------------------------
# Display completion message
# ---------------------------
echo "INFO: setup-azure-conf-test completed."
echo "INFO: Remember to \`source ${ENV_CONFIG_FILENAME}\` before running local conformance tests."
if [[ -z ${CREDENTIALS_PATH} ]]; then
    echo "INFO: ${AZURE_CREDENTIALS_FILENAME} contains the repository secret to set to run the GitHub conformance test workflow."
fi
echo "INFO: To teardown the conformance test resources, run ${TEARDOWN_SCRIPT_NAME}."
