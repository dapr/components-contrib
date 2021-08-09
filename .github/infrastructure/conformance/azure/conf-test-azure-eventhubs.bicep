// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

param eventHubsNamespaceName string
param rgLocation string = resourceGroup().location
param confTestTags object = {}

var eventHubBindingsName = '${eventHubsNamespaceName}-bindings-topic'
var eventHubBindingsPolicyName = '${eventHubBindingsName}-policy'
var eventHubBindingsConsumerGroupName = '${eventHubBindingsName}-cg'

resource eventHubsNamespace 'Microsoft.EventHub/namespaces@2017-04-01' = {
  name: eventHubsNamespaceName
  location: rgLocation
  tags: confTestTags
  sku: {
    name: 'Standard' // For > 1 consumer group
  }
  resource eventHubBindings 'eventhubs' = {
    name: eventHubBindingsName
    resource eventHubBindingsPolicy 'authorizationRules' = {
      name: eventHubBindingsPolicyName
      properties: {
        rights: [
          'Send'
          'Listen'
        ]
      }
    }
    resource eventHubBindingsConsumerGroup 'consumergroups' = {
      name: eventHubBindingsConsumerGroupName
    }
  }

output eventHubBindingsName string = eventHubsNamespace::eventHubBindings.name
output eventHubBindingsPolicyName string = eventHubsNamespace::eventHubBindings::eventHubBindingsPolicy.name
output eventHubBindingsConsumerGroupName string = eventHubsNamespace::eventHubBindings::eventHubBindingsConsumerGroup.name
