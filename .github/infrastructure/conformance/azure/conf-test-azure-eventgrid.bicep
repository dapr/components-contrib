// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

param eventGridTopicName string
param rgLocation string = resourceGroup().location
param confTestTags object = {}

resource eventGridTopic 'Microsoft.EventGrid/topics@2020-06-01' = {
  name: eventGridTopicName
  location: rgLocation
  tags: confTestTags
}
