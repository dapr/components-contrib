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

if [[ -z "${AzureIotHubName}" ]]; then
    echo "ERROR: AzureIotHubName environment variable not defined."
    exit 1
fi

# Log in to Azure using provided Service Principal (SPN) credentials
# Required roles for SPN on IoT hub
# `IoT Hub Data Contributor` scoped at this resource
# `Owner` scoped at this resource

# Install azure-iot extension without prompt 
# https://docs.microsoft.com/en-us/cli/azure/azure-cli-extensions-overview
# https://github.com/Azure/azure-iot-cli-extension
az config set extension.use_dynamic_install=yes_without_prompt

# login to azure
az login --service-principal -u $AzureCertificationServicePrincipalClientId -p $AzureCertificationServicePrincipalClientSecret --tenant $AzureCertificationTenantId

# Create test device ID if not already present
IOT_HUB_TEST_DEVICE_NAME="certification-test-device"
if [[ -z "$(az iot hub device-identity show -n ${AzureIotHubName} -d ${IOT_HUB_TEST_DEVICE_NAME})" ]]; then
    az iot hub device-identity create -n ${AzureIotHubName} -d ${IOT_HUB_TEST_DEVICE_NAME}
    sleep 5
fi

# Send the test IoT device messages to the IoT Hub.`testmessageForEventHubCertificationTest` is being asserted in the certification test
# TODO : read messageCount and data as an argument
az iot device simulate -n ${AzureIotHubName} -d ${IOT_HUB_TEST_DEVICE_NAME} --data 'testmessageForEventHubCertificationTest' --msg-count 10 --msg-interval 1 --protocol http --properties "iothub-userid=dapr-user-id;iothub-messageid=dapr-message-id;applicationProperty=custom-value"
