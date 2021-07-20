// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

param storageName string
param rgLocation string = resourceGroup().location
param confTestTags object = {}

resource storageAccount 'Microsoft.Storage/storageAccounts@2021-02-01' = {
  name: storageName
  sku: {
    name: 'Standard_RAGRS'
  }
  kind: 'StorageV2'
  location: rgLocation
  tags: confTestTags
}
