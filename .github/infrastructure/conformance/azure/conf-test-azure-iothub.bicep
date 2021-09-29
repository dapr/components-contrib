// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

param iotHubName string
param rgLocation string = resourceGroup().location
param confTestTags object = {}

var iotHubBindingsConsumerGroupName = '${iotHubName}/events/bindings-cg'
var iotHubPubsubConsumerGroupName = '${iotHubName}/events/pubsub-cg'

resource iotHub 'Microsoft.Devices/IotHubs@2021-03-31' = {
  name: iotHubName
  location: rgLocation
  tags: confTestTags
  sku: {
      capacity: 1
      name: 'F1'
  }
  properties: {
    eventHubEndpoints: {
      events: {
        retentionTimeInDays: 1
        partitionCount: 2
      }
    }
  }
}

resource iotHubBindingsConsumerGroup 'Microsoft.Devices/IotHubs/eventHubEndpoints/ConsumerGroups@2021-03-31' = {
  name: iotHubBindingsConsumerGroupName
  properties: {
    name: 'bindings-cg'
  }
  dependsOn: [
    iotHub
  ]
}

resource iotHubPubsubConsumerGroup 'Microsoft.Devices/IotHubs/eventHubEndpoints/ConsumerGroups@2021-03-31' = {
  name: iotHubPubsubConsumerGroupName
  properties: {
    name: 'pubsub-cg'
  }
  dependsOn: [
    iotHub
  ]
}

output iotHubBindingsConsumerGroupName string = iotHubBindingsConsumerGroup.name
output iotHubPubsubConsumerGroupName string = iotHubPubsubConsumerGroup.name
