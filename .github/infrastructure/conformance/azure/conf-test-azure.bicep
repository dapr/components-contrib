// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

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

var confTestRgName = '${toLower(namePrefix)}-conf-test-rg'
var cosmosDbName = '${toLower(namePrefix)}-conf-test-db'
var eventGridTopicName = '${toLower(namePrefix)}-conf-test-eventgrid-topic'
var keyVaultName = '${toLower(namePrefix)}-conf-test-kv'
var serviceBusName = '${toLower(namePrefix)}-conf-test-servicebus'
var storageName = '${toLower(namePrefix)}ctstorage'

resource confTestRg 'Microsoft.Resources/resourceGroups@2021-04-01' = {
  name: confTestRgName
  location: rgLocation
  tags: confTestTags
}

module cosmosDb 'conf-test-azure-cosmosdb.bicep' = {
  name: cosmosDbName
  scope: resourceGroup(confTestRg.name)
  params: {
    confTestTags: confTestTags
    cosmosDbName: cosmosDbName
  }
}

module eventGridTopic 'conf-test-azure-eventGrid.bicep' = {
  name: eventGridTopicName
  scope: resourceGroup(confTestRg.name)
  params: {
    confTestTags: confTestTags
    eventGridTopicName: eventGridTopicName
  }
}

module keyVault 'conf-test-azure-keyVault.bicep' = {
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

module storage 'conf-test-azure-storage.bicep' = {
  name: storageName
  scope: resourceGroup(confTestRg.name)
  params: {
    confTestTags: confTestTags
    storageName: storageName
  }
}

output confTestRgName string = confTestRg.name
output cosmosDbName string = cosmosDb.name
output cosmosDbSqlName string = cosmosDb.outputs.cosmosDbSqlName
output cosmosDbSqlContainerName string = cosmosDb.outputs.cosmosDbSqlContainerName
output eventGridTopicName string = eventGridTopic.name
output keyVaultName string = keyVault.name
output serviceBusName string = serviceBus.name
output storageName string = storage.name
