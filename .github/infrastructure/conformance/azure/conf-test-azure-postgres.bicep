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

param postgresServerName string
param sdkAuthSpId string
param sdkAuthSpName string
param rgLocation string = resourceGroup().location
param confTestTags object = {}
param postgresqlVersion string = '14'
param tenantId string = subscription().tenantId

resource postgresServer 'Microsoft.DBforPostgreSQL/flexibleServers@2023-03-01-preview' = {
  name: postgresServerName
  location: rgLocation
  tags: confTestTags
  sku: {
    name: 'Standard_B1ms'
    tier: 'Burstable'
  }
  properties: {
    storage: {
      storageSizeGB: 32
      autoGrow: 'Disabled'
    }
    authConfig: {
      activeDirectoryAuth: 'Enabled'
      passwordAuth: 'Disabled'
      tenantId: tenantId
    }
    network: {}
    version: postgresqlVersion
  }

  resource daprTestDB 'databases@2023-03-01-preview' = {
    name: 'dapr_test'
    properties: {
      charset: 'UTF8'
      collation: 'en_US.utf8'
    }
  }

  resource fwRules 'firewallRules@2023-03-01-preview' = {
    name: 'allowall'
    properties: {
      startIpAddress: '0.0.0.0'
      endIpAddress: '255.255.255.255'
    }
  }

  resource azureAdAdmin 'administrators@2023-03-01-preview' = {
    name: sdkAuthSpId
    properties: {
      principalType: 'ServicePrincipal'
      principalName: sdkAuthSpName
      tenantId: tenantId
    }
  }
}
