// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

param serviceBusName string
param rgLocation string = resourceGroup().location
param confTestTags object = {}

resource serviceBus 'Microsoft.ServiceBus/namespaces@2017-04-01' = {
  name: serviceBusName
  location: rgLocation
  tags: confTestTags
  sku: {
    name: 'Standard' // Need at least Standard SKU to create Topics in ServiceBus Namespace
  }
}
