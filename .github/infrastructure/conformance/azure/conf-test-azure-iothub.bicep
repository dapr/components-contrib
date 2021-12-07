/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
      name: 'S1'
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
