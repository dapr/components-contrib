// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

param sqlServerName string
param rgLocation string = resourceGroup().location
param ipRanges array

resource sqlServer 'Microsoft.Sql/servers@2021-02-01-preview' = {
  name: sqlServerName
  location: rgLocation
}

resource sqlServerFirewallRule 'Microsoft.Sql/servers/firewallRules@2021-02-01-preview' = [for (ipRange, i) in ipRanges: {
  name: 'sqlGitHubRule${i}'
  parent: sqlServer
  properties: {
    endIpAddress: '${ipRange[1]}'
    startIpAddress: '${ipRange[0]}'
  }
}]
