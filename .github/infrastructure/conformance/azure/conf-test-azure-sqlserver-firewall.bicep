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
