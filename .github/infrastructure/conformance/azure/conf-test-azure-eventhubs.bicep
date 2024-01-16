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

var eventHubsNamespacePolicy = '${eventHubsNamespaceName}-namespace-policy'
var eventHubBindingsName = 'eventhubs-bindings-topic'
var eventHubBindingsPolicyName = '${eventHubBindingsName}-policy'
var eventHubBindingsConsumerGroupName = '${eventHubBindingsName}-cg'

var eventHubPubsubName = 'eventhubs-pubsub-topic'
var eventHubPubsubPolicyName = '${eventHubPubsubName}-policy'
var eventHubPubsubConsumerGroupName = '${eventHubPubsubName}-cg'

var eventHubBulkPubsubName = 'eventhubs-pubsub-topic-bulk'
var eventHubBulkPubsubPolicyName = '${eventHubBulkPubsubName}-policy'

var certificationEventHubPubsubTopicActiveName = 'certification-pubsub-topic-active'
var certificationEventHubPubsubTopicActivePolicyName = '${certificationEventHubPubsubTopicActiveName}-policy'

var certificationEventHubPubsubTopicPassiveName = 'certification-pubsub-topic-passive'

var certificationEventHubPubsubTopicMulti1Name = 'certification-pubsub-multi-topic1'
var certificationEventHubPubsubTopicMulti2Name = 'certification-pubsub-multi-topic2'
var certificationEventHubPubsubTopicMulti1PolicyName = '${certificationEventHubPubsubTopicMulti1Name}-policy'
var certificationEventHubPubsubTopicMulti2PolicyName = '${certificationEventHubPubsubTopicMulti2Name}-policy'

var certificationConsumerGroupName1 = 'ehcertification1'
var certificationConsumerGroupName2 = 'ehcertification2'

resource eventHubsNamespace 'Microsoft.EventHub/namespaces@2017-04-01' = {
  name: eventHubsNamespaceName
  location: rgLocation
  tags: confTestTags
  sku: {
    name: 'Standard' // For > 1 consumer group
  }
  // For connectionstring and test operation at namespace level
  resource eventHubPubsubNamespacePolicy 'authorizationRules' = {
    name: eventHubsNamespacePolicy
    properties: {
      rights: [
        'Send'
        'Listen'
      ]
    }
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
    resource eventHubBindingsCertificationConsumerGroup1 'consumergroups' = {
      name: certificationConsumerGroupName1
    }
    resource eventHubBindingsCertificationConsumerGroup2 'consumergroups' = {
      name: certificationConsumerGroupName2
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
  resource eventHubBulkPubsub 'eventhubs' = {
    name: eventHubBulkPubsubName
    properties: {
      messageRetentionInDays: 1
    }
    resource eventHubBulkPubsubPolicy 'authorizationRules' = {
      name: eventHubBulkPubsubPolicyName
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
  resource certificationEventHubPubsubTopicActive 'eventhubs' = {
    name: certificationEventHubPubsubTopicActiveName
    properties: {
      messageRetentionInDays: 1
    }
    resource certificationEventHubPubsubTopicActivePolicy 'authorizationRules' = {
      name: certificationEventHubPubsubTopicActivePolicyName
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
  resource certificationEventHubPubsubTopicPassive 'eventhubs' = {
    name: certificationEventHubPubsubTopicPassiveName
    properties: {
      messageRetentionInDays: 1
    }
    resource eventHubPubsubConsumerGroup1 'consumergroups' = {
      name: certificationConsumerGroupName1
    }
    resource eventHubPubsubConsumerGroup2 'consumergroups' = {
      name: certificationConsumerGroupName2
    }
  }
  resource certificationEventHubPubsubTopicMulti1 'eventhubs' = {
    name: certificationEventHubPubsubTopicMulti1Name
    properties: {
      messageRetentionInDays: 1
    }
    resource certificationEventHubPubsubTopicMulti1Policy 'authorizationRules' = {
      name: certificationEventHubPubsubTopicMulti1PolicyName
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
  resource certificationEventHubPubsubTopicMulti2 'eventhubs' = {
    name: certificationEventHubPubsubTopicMulti2Name
    properties: {
      messageRetentionInDays: 1
    }
    resource certificationEventHubPubsubTopicMulti2Policy 'authorizationRules' = {
      name: certificationEventHubPubsubTopicMulti2PolicyName
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

output eventHubBulkPubsubName string = eventHubsNamespace::eventHubBulkPubsub.name
output eventHubBulkPubsubPolicyName string = eventHubsNamespace::eventHubBulkPubsub::eventHubBulkPubsubPolicy.name

output eventHubsNamespacePolicyName string = eventHubsNamespace::eventHubPubsubNamespacePolicy.name
output certificationEventHubPubsubTopicActiveName string = eventHubsNamespace::certificationEventHubPubsubTopicActive.name
output certificationEventHubPubsubTopicActivePolicyName string = eventHubsNamespace::certificationEventHubPubsubTopicActive::certificationEventHubPubsubTopicActivePolicy.name

output certificationEventHubPubsubTopicMulti1Name string = eventHubsNamespace::certificationEventHubPubsubTopicMulti1.name
output certificationEventHubPubsubTopicMulti2Name string = eventHubsNamespace::certificationEventHubPubsubTopicMulti2.name
output certificationEventHubPubsubTopicMulti1PolicyName string = eventHubsNamespace::certificationEventHubPubsubTopicMulti1::certificationEventHubPubsubTopicMulti1Policy.name 
output certificationEventHubPubsubTopicMulti2PolicyName string = eventHubsNamespace::certificationEventHubPubsubTopicMulti2::certificationEventHubPubsubTopicMulti2Policy.name 
