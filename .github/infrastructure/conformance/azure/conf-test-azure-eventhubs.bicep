// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

param eventHubsNamespaceName string
param rgLocation string = resourceGroup().location
param confTestTags object = {}

var eventHubName = '${eventHubsNamespaceName}-topic'
var eventHubPolicyName = '${eventHubName}-policy'
var eventHubConsumerGroupName = '${eventHubName}-cg'

resource eventHubsNamespace 'Microsoft.EventHub/namespaces@2017-04-01' = {
  name: eventHubsNamespaceName
  location: rgLocation
  tags: confTestTags
  sku: {
    name: 'Standard' // For > 1 consumer group
  }
  resource eventHub 'eventhubs' = {
    name: eventHubName
    resource eventHubPolicy 'authorizationRules' = {
      name: eventHubPolicyName
      properties: {
        rights: [
          'Send'
          'Listen'
        ]
      }
    }
    resource consumerGroup 'consumergroups' = {
      name: eventHubConsumerGroupName
    }
  }
}

output eventHubName string = eventHubsNamespace::eventHub.name
output eventHubPolicyName string = eventHubsNamespace::eventHub::eventHubPolicy.name
output eventHubConsumerGroupName string = eventHubsNamespace::eventHub::consumerGroup.name
