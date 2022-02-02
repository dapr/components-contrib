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

targetScope = 'subscription'

@minLength(3)
@maxLength(15) // storageName must be < 24 characters total
@description('Provide an string prefix between 3-15 characters for all resource names deployed by this template. It should only consist of lower case alphabetical characters.')
param namePrefix string

@description('Provide a target location for the resource group and azure resources. Defaults to West US 2.')
param rgLocation string = 'West US 2'

@description('Provide tags to associate with resources created by this deployment. Defaults to Role=dapr-conf-test.')
param confTestTags object = {
  Role: 'dapr-conf-test'
}

@minLength(36)
@maxLength(36)
@description('Provide the user objectId in the current tenant to set as admin for Azure Key Vault.')
param adminId string

@minLength(36)
@maxLength(36)
@description('Provide the objectId of the Service Principal using secret auth with get access to secrets in Azure Key Vault.')
param sdkAuthSpId string

@minLength(36)
@maxLength(36)
@description('Provide the objectId of the Service Principal using cert auth with get and list access to all assets in Azure Key Vault.')
param certAuthSpId string

@minLength(16)
@description('Provide the SQL server admin password of at least 16 characters.')
param sqlServerAdminPassword string

var confTestRgName = '${toLower(namePrefix)}-conf-test-rg'
var acrName = '${toLower(namePrefix)}conftestregistry'
var cosmosDbName = '${toLower(namePrefix)}-conf-test-db'
var eventGridTopicName = '${toLower(namePrefix)}-conf-test-eventgrid-topic'
var eventHubsNamespaceName = '${toLower(namePrefix)}-conf-test-eventhubs'
var iotHubName = '${toLower(namePrefix)}-conf-test-iothub'
var keyVaultName = '${toLower(namePrefix)}-conf-test-kv'
var serviceBusName = '${toLower(namePrefix)}-conf-test-servicebus'
var sqlServerName = '${toLower(namePrefix)}-conf-test-sql'
var storageName = '${toLower(namePrefix)}ctstorage'

resource confTestRg 'Microsoft.Resources/resourceGroups@2021-04-01' = {
  name: confTestRgName
  location: rgLocation
  tags: confTestTags
}

module acr 'conf-test-azure-container-registry.bicep' = {
  name: acrName
  scope: resourceGroup(confTestRg.name)
  params: {
    confTestTags: confTestTags
    acrName: acrName
  }
}

module cosmosDb 'conf-test-azure-cosmosdb.bicep' = {
  name: cosmosDbName
  scope: resourceGroup(confTestRg.name)
  params: {
    confTestTags: confTestTags
    cosmosDbName: cosmosDbName
  }
}

module eventGridTopic 'conf-test-azure-eventgrid.bicep' = {
  name: eventGridTopicName
  scope: resourceGroup(confTestRg.name)
  params: {
    confTestTags: confTestTags
    eventGridTopicName: eventGridTopicName
  }
}

module eventHubsNamespace 'conf-test-azure-eventhubs.bicep' = {
  name: eventHubsNamespaceName
  scope: resourceGroup(confTestRg.name)
  params: {
    confTestTags: confTestTags
    eventHubsNamespaceName: eventHubsNamespaceName
  }
}

module iotHub 'conf-test-azure-iothub.bicep' = {
  name: iotHubName
  scope: resourceGroup(confTestRg.name)
  params: {
    confTestTags: confTestTags
    iotHubName: iotHubName
  }
}

module keyVault 'conf-test-azure-keyvault.bicep' = {
  name: keyVaultName
  scope: resourceGroup(confTestRg.name)
  params: {
    adminId: adminId
    confTestTags: confTestTags
    certAuthSpId: certAuthSpId
    keyVaultName: keyVaultName
    sdkAuthSpId: sdkAuthSpId
  }
}

module serviceBus 'conf-test-azure-servicebus.bicep' = {
  name: serviceBusName
  scope: resourceGroup(confTestRg.name)
  params: {
    confTestTags: confTestTags
    serviceBusName: serviceBusName
  }
}

module sqlServer 'conf-test-azure-sqlserver.bicep' = {
  name: sqlServerName
  scope: resourceGroup(confTestRg.name)
  params: {
    confTestTags: confTestTags
    sqlServerName: sqlServerName
    sqlServerAdminPassword: sqlServerAdminPassword
  }
}

module storage 'conf-test-azure-storage.bicep' = {
  name: storageName
  scope: resourceGroup(confTestRg.name)
  params: {
    confTestTags: confTestTags
    storageName: storageName
  }
}

output confTestRgName string = confTestRg.name
output acrName string = acr.name
output cosmosDbName string = cosmosDb.name
output cosmosDbSqlName string = cosmosDb.outputs.cosmosDbSqlName
output cosmosDbSqlContainerName string = cosmosDb.outputs.cosmosDbSqlContainerName
output eventGridTopicName string = eventGridTopic.name
output eventHubsNamespace string = eventHubsNamespace.name
output eventHubBindingsName string = eventHubsNamespace.outputs.eventHubBindingsName
output eventHubBindingsPolicyName string = eventHubsNamespace.outputs.eventHubBindingsPolicyName
output eventHubBindingsConsumerGroupName string = eventHubsNamespace.outputs.eventHubBindingsConsumerGroupName
output eventHubPubsubName string = eventHubsNamespace.outputs.eventHubPubsubName
output eventHubPubsubPolicyName string = eventHubsNamespace.outputs.eventHubPubsubPolicyName
output eventHubPubsubConsumerGroupName string = eventHubsNamespace.outputs.eventHubPubsubConsumerGroupName
output iotHubName string = iotHub.name
output iotHubBindingsConsumerGroupName string = iotHub.outputs.iotHubBindingsConsumerGroupName
output iotHubPubsubConsumerGroupName string = iotHub.outputs.iotHubPubsubConsumerGroupName
output keyVaultName string = keyVault.name
output serviceBusName string = serviceBus.name
output sqlServerName string = sqlServer.name
output sqlServerAdminName string = sqlServer.outputs.sqlServerAdminName
output storageName string = storage.name
