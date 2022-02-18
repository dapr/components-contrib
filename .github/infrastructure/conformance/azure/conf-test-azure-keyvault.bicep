// ------------------------------------------------------------------------
// Copyright 2022 The Dapr Authors
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

param adminId string
param certAuthSpId string
param sdkAuthSpId string
param keyVaultName string
param rgLocation string = resourceGroup().location
param confTestTags object = {}
param tenantId string = subscription().tenantId

resource keyVault 'Microsoft.KeyVault/vaults@2019-09-01' = {
  name: keyVaultName
  location: rgLocation
  tags: confTestTags
  properties: {
    tenantId: tenantId
    sku: {
      family: 'A'
      name: 'standard'
    }
    accessPolicies: [
      // This access policy is to allow the user to manage the KeyVault instance.
      {
        tenantId: tenantId
        objectId: adminId
        permissions: {
          keys: [
            'all'
          ]
          secrets: [
            'all'
          ]
          certificates: [
            'all'
          ]
          storage: [
            'all'
          ]
        }
      }
      // This access policy is used by the AKV conformance test to Get and BulkGet secrets.
      {
        tenantId: tenantId
        objectId: certAuthSpId
        permissions: {
          keys: [
            'get'
            'list'
          ]
          secrets: [
            'get'
            'list'
          ]
          certificates: [
            'get'
            'list'
          ]
          storage: [
            'get'
            'list'
          ]
        }
      }
      // This access policy is used by GitHub workflow to retrieve the required-secrets
      // and required-certs for the conformance tests.
      {
        tenantId: tenantId
        objectId: sdkAuthSpId
        permissions: {
          secrets: [
            'get'
          ]
        }
      }
    ]
  }

  // These test secrets are defined by the conformance tests secretstores.go
  resource testsecret1 'secrets' = {
    name: 'conftestsecret'
    properties: {
      value: 'abcd'
      contentType: 'plaintext-value'
    }
  }

  resource testsecret2 'secrets' = {
    name: 'secondsecret'
    properties: {
      value: 'efgh'
      contentType: 'plaintext-value'
    }
  }
}
