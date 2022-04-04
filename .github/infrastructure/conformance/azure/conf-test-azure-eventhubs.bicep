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

var certificationEventHubPubsub1Name = 'certification-pubsub-topic1'
var certificationEventHubPubsub1PolicyName = '${certificationEventHubPubsub1Name}-policy'

var certificationEventHubPubsub2Name = 'certification-pubsub-topic2'
var certificationEventHubPubsub2PolicyName = '${certificationEventHubPubsub2Name}-policy'

var certificationConsumerGroupName1 = 'ehcertification1'
var certificationConsumerGroupName2 = 'ehcertification2'

resource eventHubsNamespace 'Microsoft.EventHub/namespaces@2017-04-01' = {
  name: eventHubsNamespaceName
  location: rgLocation
  tags: confTestTags
  sku: {
    name: 'Standard' // For > 1 consumer group
  }
  resource eventHubBindings 'eventhubs' = {
    name: eventHubBindingsName
    properties: {
      messageRetentionInDays: 1
    }
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
    properties: {
      messageRetentionInDays: 1
    }
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
  resource certificationEventHubPubsub1 'eventhubs' = {
    name: certificationEventHubPubsub1Name
    properties: {
      messageRetentionInDays: 1
    }
    resource certificationEventHubPubsub1Policy 'authorizationRules' = {
      name: certificationEventHubPubsub1PolicyName
      properties: {
        rights: [
          'Send'
          'Listen'
        ]
      }
    }
    resource eventHubPubsubConsumerGroup1 'consumergroups' = {
      name: certificationConsumerGroupName1
    }
    resource eventHubPubsubConsumerGroup2 'consumergroups' = {
      name: certificationConsumerGroupName2
    }
  }
  resource certificationEventHubPubsub2 'eventhubs' = {
    name: certificationEventHubPubsub2Name
    properties: {
      messageRetentionInDays: 1
    }
    resource certificationEventHubPubsub2Policy 'authorizationRules' = {
      name: certificationEventHubPubsub2PolicyName
      properties: {
        rights: [
          'Send'
          'Listen'
        ]
      }
    }
    resource eventHubPubsubConsumerGroup1 'consumergroups' = {
      name: certificationConsumerGroupName1
    }
    resource eventHubPubsubConsumerGroup2 'consumergroups' = {
      name: certificationConsumerGroupName2
    }
  }
}

output eventHubBindingsName string = eventHubsNamespace::eventHubBindings.name
output eventHubBindingsPolicyName string = eventHubsNamespace::eventHubBindings::eventHubBindingsPolicy.name
output eventHubBindingsConsumerGroupName string = eventHubsNamespace::eventHubBindings::eventHubBindingsConsumerGroup.name

output eventHubPubsubName string = eventHubsNamespace::eventHubPubsub.name
output eventHubPubsubPolicyName string = eventHubsNamespace::eventHubPubsub::eventHubPubsubPolicy.name
output eventHubPubsubConsumerGroupName string = eventHubsNamespace::eventHubPubsub::eventHubPubsubConsumerGroup.name

output certificationEventHubPubsub1Name string = eventHubsNamespace::certificationEventHubPubsub1.name
output certificationEventHubPubsub1PolicyName string = eventHubsNamespace::certificationEventHubPubsub1::certificationEventHubPubsub1Policy.name

output certificationEventHubPubsub2Name string = eventHubsNamespace::certificationEventHubPubsub2.name
output certificationEventHubPubsub2PolicyName string = eventHubsNamespace::certificationEventHubPubsub2::certificationEventHubPubsub2Policy.name
