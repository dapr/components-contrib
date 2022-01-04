// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

param sqlServerName string
param rgLocation string = resourceGroup().location
param confTestTags object = {}
param sqlServerAdminPassword string

var sqlServerAdminName = '${sqlServerName}-admin'

resource sqlServer 'Microsoft.Sql/servers@2021-02-01-preview' = {
  name: sqlServerName
  location: rgLocation
  tags: confTestTags
  properties: {
    administratorLogin: sqlServerAdminName
    administratorLoginPassword: sqlServerAdminPassword
    minimalTlsVersion: '1.2'
    publicNetworkAccess: 'Enabled'
  }
}

output sqlServerAdminName string = sqlServer.properties.administratorLogin
