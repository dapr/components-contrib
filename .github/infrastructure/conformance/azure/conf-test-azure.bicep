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
@description('Provide the objectId of the Service Principal using secret auth with get access to secrets in Azure Key Vault and access Azure PostgreSQL')
param sdkAuthSpId string

@description('Provide the name of the Service Principal using secret auth with get access to secrets in Azure Key Vault and access Azure PostgreSQL')
param sdkAuthSpName string

@minLength(36)
@maxLength(36)
@description('Provide the objectId of the Service Principal using cert auth with get and list access to all assets in Azure Key Vault.')
param certAuthSpId string

@minLength(16)
@description('Provide the SQL server admin password of at least 16 characters.')
@secure()
param sqlServerAdminPassword string

var confTestRgName = '${toLower(namePrefix)}-conf-test-rg'
var cosmosDbName = '${toLower(namePrefix)}-conf-test-db'
var cosmosDbTableAPIName = '${toLower(namePrefix)}-conf-test-table'
var eventGridTopicName = '${toLower(namePrefix)}-conf-test-eventgrid-topic'
var eventHubsNamespaceName = '${toLower(namePrefix)}-conf-test-eventhubs'
var iotHubName = '${toLower(namePrefix)}-conf-test-iothub'
var keyVaultName = '${toLower(namePrefix)}-conf-test-kv'
var serviceBusName = '${toLower(namePrefix)}-conf-test-servicebus'
var sqlServerName = '${toLower(namePrefix)}-conf-test-sql'
var postgresServerName = '${toLower(namePrefix)}-conf-test-pg'
var storageName = '${toLower(namePrefix)}ctstorage'
var appconfigStoreName = '${toLower(namePrefix)}-conf-test-cfg'

resource confTestRg 'Microsoft.Resources/resourceGroups@2021-04-01' = {
  name: confTestRgName
  location: rgLocation
  tags: confTestTags
}

// Azure Container Registry is not currently used, but may be required again in the future.
// If so, look at the latest commit where it was present:
// https://github.com/dapr/components-contrib/tree/a8133088467fc29e1929a5dab396b11cf123a38b/.github/infrastructure

module cosmosDb 'conf-test-azure-cosmosdb.bicep' = {
  name: cosmosDbName
  scope: resourceGroup(confTestRg.name)
  params: {
    confTestTags: confTestTags
    cosmosDbName: cosmosDbName
    rgLocation: rgLocation
  }
}

module cosmosDbTable 'conf-test-azure-cosmosdb-table.bicep' = {
  name: cosmosDbTableAPIName
  scope: resourceGroup(confTestRg.name)
  params: {
    confTestTags: confTestTags
    cosmosDbTableAPIName: cosmosDbTableAPIName
    rgLocation: rgLocation
  }
}

module eventGridTopic 'conf-test-azure-eventgrid.bicep' = {
  name: eventGridTopicName
  scope: resourceGroup(confTestRg.name)
  params: {
    confTestTags: confTestTags
    eventGridTopicName: eventGridTopicName
    rgLocation: rgLocation
  }
}

module eventHubsNamespace 'conf-test-azure-eventhubs.bicep' = {
  name: eventHubsNamespaceName
  scope: resourceGroup(confTestRg.name)
  params: {
    confTestTags: confTestTags
    eventHubsNamespaceName: eventHubsNamespaceName
    rgLocation: rgLocation
  }
}

module iotHub 'conf-test-azure-iothub.bicep' = {
  name: iotHubName
  scope: resourceGroup(confTestRg.name)
  params: {
    confTestTags: confTestTags
    iotHubName: iotHubName
    rgLocation: rgLocation
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
    rgLocation: rgLocation
  }
}

module serviceBus 'conf-test-azure-servicebus.bicep' = {
  name: serviceBusName
  scope: resourceGroup(confTestRg.name)
  params: {
    confTestTags: confTestTags
    serviceBusName: serviceBusName
    rgLocation: rgLocation
  }
}

module sqlServer 'conf-test-azure-sqlserver.bicep' = {
  name: sqlServerName
  scope: resourceGroup(confTestRg.name)
  params: {
    confTestTags: confTestTags
    sqlServerName: sqlServerName
    sqlServerAdminPassword: sqlServerAdminPassword
    rgLocation: rgLocation
  }
}

module storage 'conf-test-azure-storage.bicep' = {
  name: storageName
  scope: resourceGroup(confTestRg.name)
  params: {
    confTestTags: confTestTags
    storageName: storageName
    rgLocation: rgLocation
  }
}

module postgres 'conf-test-azure-postgres.bicep' = {
  name: postgresServerName
  scope: resourceGroup(confTestRg.name)
  params: {
    confTestTags: confTestTags
    postgresServerName: postgresServerName
    sdkAuthSpId: sdkAuthSpId
    sdkAuthSpName: sdkAuthSpName
    rgLocation: rgLocation
  }
}

module appconfig 'conf-test-azure-appconfig.bicep' = {
  name: appconfigStoreName
  scope: resourceGroup(confTestRg.name)
  params: {
    configStoreName: appconfigStoreName
    location: rgLocation
  }
}

output confTestRgName string = confTestRg.name
output cosmosDbName string = cosmosDb.name
output cosmosDbSqlName string = cosmosDb.outputs.cosmosDbSqlName
output cosmosDbTableAPIName string = cosmosDbTable.outputs.cosmosDbTableAPIName
output cosmosDbSqlContainerName string = cosmosDb.outputs.cosmosDbSqlContainerName
output eventGridTopicName string = eventGridTopic.name
output eventHubsNamespace string = eventHubsNamespace.name
output eventHubBindingsName string = eventHubsNamespace.outputs.eventHubBindingsName
output eventHubBindingsPolicyName string = eventHubsNamespace.outputs.eventHubBindingsPolicyName
output eventHubBindingsConsumerGroupName string = eventHubsNamespace.outputs.eventHubBindingsConsumerGroupName
output eventHubPubsubName string = eventHubsNamespace.outputs.eventHubPubsubName
output eventHubPubsubPolicyName string = eventHubsNamespace.outputs.eventHubPubsubPolicyName
output eventHubPubsubConsumerGroupName string = eventHubsNamespace.outputs.eventHubPubsubConsumerGroupName
output eventHubsNamespacePolicyName string = eventHubsNamespace.outputs.eventHubsNamespacePolicyName
output certificationEventHubPubsubTopicActiveName string = eventHubsNamespace.outputs.certificationEventHubPubsubTopicActiveName
output certificationEventHubPubsubTopicActivePolicyName string = eventHubsNamespace.outputs.certificationEventHubPubsubTopicActivePolicyName
output certificationEventHubPubsubTopicMulti1Name string = eventHubsNamespace.outputs.certificationEventHubPubsubTopicMulti1Name
output certificationEventHubPubsubTopicMulti2Name string = eventHubsNamespace.outputs.certificationEventHubPubsubTopicMulti2Name
output iotHubName string = iotHub.name
output iotHubBindingsConsumerGroupName string = iotHub.outputs.iotHubBindingsConsumerGroupName
output iotHubPubsubConsumerGroupName string = iotHub.outputs.iotHubPubsubConsumerGroupName
output keyVaultName string = keyVault.name
output serviceBusName string = serviceBus.name
output sqlServerName string = sqlServer.name
output sqlServerAdminName string = sqlServer.outputs.sqlServerAdminName
output postgresServerName string = postgres.name
output storageName string = storage.name
output appconfigName string = appconfig.name
