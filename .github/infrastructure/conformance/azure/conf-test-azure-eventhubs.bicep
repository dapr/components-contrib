// ------------------------------------------------------------------------
// Copyright 2021 The Dapr Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// ------------------------------------------------------------------------

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
