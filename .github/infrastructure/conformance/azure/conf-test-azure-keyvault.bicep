// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

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
