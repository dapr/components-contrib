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

# This script delete the Azure Eventhub created in the scoped Azure EventHub namespace as per the test suite.
# Step 1 : Login to Azure with provided SPN which has `Azure Event Hubs Data Owner` role on scoped eventhub namespace.
# Step 2 : Delete the eventhub/topic which is provided as an argument
# Step 3 : Delete the checkpoint container used by the consumer
# how to run : ./delete-eventhub.sh topic1


if [ "$#" -ne 1 ] 
then
    echo "invalid argument, eventhubname is required."
    exit 1;
fi

EVENTHUBNAME=$1

echo "begin: delete process for eventhub: ${EVENTHUBNAME}"

# login to azure
az login --service-principal -u $AzureCertificationServicePrincipalClientId -p $AzureCertificationServicePrincipalClientSecret --tenant $AzureCertificationTenantId

# delete eventhub
az eventhubs eventhub delete --resource-group $AzureResourceGroupName --namespace-name $AzureEventHubsPubsubNamespace --name $EVENTHUBNAME

# delete checkpoint container used by the consumer
CONTAINERNAME="certificationentitymgmttest"
az storage container delete --account-key $AzureBlobStorageAccessKey --account-name $AzureBlobStorageAccount --name $CONTAINERNAME

echo "end: delete process for eventhub: ${EVENTHUBNAME} and container ${CONTAINERNAME}"
