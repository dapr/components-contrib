#!/usr/bin/env bash
# ------------------------------------------------------------
# Copyright (c) Microsoft Corporation and Dapr Contributors.
# Licensed under the MIT License.
# ------------------------------------------------------------

set -e

if [[ -z "${IOT_HUB_NAME}" ]]; then
    echo "ERROR: IOT_HUB_NAME environment variable not defined."
    exit 1
fi
if [[ -z "${AZURE_CREDENTIALS}" ]]; then
    echo "ERROR: AZURE_CREDENTIALS environment variable not defined."
    exit 1
fi

# Log in to Azure using provided Service Principal (SP) credentials
# The provided SP must have Contributor role access to the IoT Hub specified by IOT_HUB_NAME
SDK_AUTH_SP_APPID="$(echo "${AZURE_CREDENTIALS}" | grep 'clientId' | sed -E 's/(.*clientId\"\: \")|\",//g')"
SDK_AUTH_SP_CLIENT_SECRET="$(echo "${AZURE_CREDENTIALS}" | grep 'clientSecret' | sed -E 's/(.*clientSecret\"\: \")|\",//g')"
SDK_AUTH_SP_TENANT="$(echo "${AZURE_CREDENTIALS}" | grep 'tenantId' | sed -E 's/(.*tenantId\"\: \")|\",//g')"
az login --service-principal -u ${SDK_AUTH_SP_APPID} -p ${SDK_AUTH_SP_CLIENT_SECRET} --tenant ${SDK_AUTH_SP_TENANT}

# Create test device ID if not already present
IOT_HUB_TEST_DEVICE_NAME="test-device"
if [[ -z "$(az iot hub device-identity show -n ${IOT_HUB_NAME} -d ${IOT_HUB_TEST_DEVICE_NAME})" ]]; then
    az iot hub device-identity create -n ${IOT_HUB_NAME} -d ${IOT_HUB_TEST_DEVICE_NAME}
    sleep 5
fi

# Send the test IoT device messages to the IoT Hub
az iot device send-d2c-message -n ${IOT_HUB_NAME} -d ${IOT_HUB_TEST_DEVICE_NAME} --data '{ "data": "Integration test message" }' --msg-count 2
