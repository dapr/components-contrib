/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
