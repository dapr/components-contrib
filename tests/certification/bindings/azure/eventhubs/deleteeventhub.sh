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

CONTAINER_NAME=${1}

if [ -z "$CONTAINER_NAME" ]; then
    echo "Usage: ./deleteeventhub.sh [container-name]"
    exit 1
fi

echo "Deleting container $CONTAINER_NAME"

# login to azure
az login --service-principal -u $AzureCertificationServicePrincipalClientId -p $AzureCertificationServicePrincipalClientSecret --tenant $AzureCertificationTenantId

# delete container used by the consumer
az storage container delete --account-key $AzureBlobStorageAccessKey --account-name $AzureBlobStorageAccount --name "$CONTAINER_NAME"
