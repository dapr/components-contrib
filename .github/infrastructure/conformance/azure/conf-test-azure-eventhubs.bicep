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

var eventHubPubsubName = '${eventHubsNamespaceName}-pubsub-topic'
var eventHubPubsubPolicyName = '${eventHubPubsubName}-policy'
var eventHubPubsubConsumerGroupName = '${eventHubPubsubName}-cg'


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
  resource eventHubPubsub 'eventhubs' = {
    name: eventHubPubsubName
    resource eventHubPubsubPolicy 'authorizationRules' = {
      name: eventHubPubsubPolicyName
      properties: {
        rights: [
          'Send'
          'Listen'
        ]
      }
    }
    resource eventHubPubsubConsumerGroup 'consumergroups' = {
      name: eventHubPubsubConsumerGroupName
    }
  }
}

output eventHubBindingsName string = eventHubsNamespace::eventHubBindings.name
output eventHubBindingsPolicyName string = eventHubsNamespace::eventHubBindings::eventHubBindingsPolicy.name
output eventHubBindingsConsumerGroupName string = eventHubsNamespace::eventHubBindings::eventHubBindingsConsumerGroup.name

output eventHubPubsubName string = eventHubsNamespace::eventHubPubsub.name
output eventHubPubsubPolicyName string = eventHubsNamespace::eventHubPubsub::eventHubPubsubPolicy.name
output eventHubPubsubConsumerGroupName string = eventHubsNamespace::eventHubPubsub::eventHubPubsubConsumerGroup.name
