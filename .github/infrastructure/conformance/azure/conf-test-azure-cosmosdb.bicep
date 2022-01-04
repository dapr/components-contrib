// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

param cosmosDbName string
param rgLocation string = resourceGroup().location
param confTestTags object = {}

var cosmosDbSqlName = '${cosmosDbName}-sqldb'
var cosmosDbSqlContainerName = '${cosmosDbSqlName}-container'

resource cosmosDb 'Microsoft.DocumentDB/databaseAccounts@2021-04-15' = {
  name: cosmosDbName
  location: rgLocation
  tags: confTestTags
  properties: {
    consistencyPolicy: {
      defaultConsistencyLevel: 'Strong' // Needed by conformance test state.go
    }
    locations: [
      {
        locationName: rgLocation
      }
    ]
    databaseAccountOfferType: 'Standard'
  }

  resource cosmosDbSql 'sqlDatabases' = {
    name: cosmosDbSqlName
    properties: {
      resource: {
        id: cosmosDbSqlName
      }
    }
    resource cosmosDbSqlContainer 'containers' = {
      name: cosmosDbSqlContainerName
      properties: {
        resource: {
          id: cosmosDbSqlContainerName
          partitionKey: {
            paths: [
              '/partitionKey' // Defined by conformance test state.go
            ]
          }
        }
      }
    }
  }
}

output cosmosDbSqlName string = cosmosDb::cosmosDbSql.name
output cosmosDbSqlContainerName string = cosmosDb::cosmosDbSql::cosmosDbSqlContainer.name
