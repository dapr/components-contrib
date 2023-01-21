/*
Copyright 2023 The Dapr Authors
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

package eventhubs

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

type azureEventHubsMetadata struct {
	ConnectionString        string `json:"connectionString" mapstructure:"connectionString"`
	EventHubNamespace       string `json:"eventHubNamespace" mapstructure:"eventHubNamespace"`
	ConsumerGroup           string `json:"consumerID" mapstructure:"consumerID"`
	StorageConnectionString string `json:"storageConnectionString" mapstructure:"storageConnectionString"`
	StorageAccountName      string `json:"storageAccountName" mapstructure:"storageAccountName"`
	StorageAccountKey       string `json:"storageAccountKey" mapstructure:"storageAccountKey"`
	StorageContainerName    string `json:"storageContainerName" mapstructure:"storageContainerName"`
	EnableEntityManagement  bool   `json:"enableEntityManagement,string" mapstructure:"enableEntityManagement"`
	MessageRetentionInDays  int32  `json:"messageRetentionInDays,string" mapstructure:"messageRetentionInDays"`
	PartitionCount          int32  `json:"partitionCount,string" mapstructure:"partitionCount"`
	SubscriptionID          string `json:"subscriptionID" mapstructure:"subscriptionID"`
	ResourceGroupName       string `json:"resourceGroupName" mapstructure:"resourceGroupName"`

	// Internal properties
	namespaceName    string
	hubName          string
	aadTokenProvider azcore.TokenCredential
	properties       map[string]string
}

func parseEventHubsMetadata(meta map[string]string, log logger.Logger) (*azureEventHubsMetadata, error) {
	var m azureEventHubsMetadata
	err := metadata.DecodeMetadata(meta, &m)
	if err != nil {
		return nil, fmt.Errorf("failed to decode metada: %w", err)
	}

	// Store the raw properties in the object
	m.properties = meta

	// One and only one of connectionString and eventHubNamespace is required
	if m.ConnectionString == "" && m.EventHubNamespace == "" {
		return nil, errors.New("one of connectionString or eventHubNamespace is required")
	}
	if m.ConnectionString != "" && m.EventHubNamespace != "" {
		return nil, errors.New("only one of connectionString or eventHubNamespace should be passed")
	}

	// If both storageConnectionString and storageAccountKey are specified, show a warning because the connection string will take priority
	if m.StorageConnectionString != "" && m.StorageAccountName != "" {
		log.Warn("Property storageAccountKey is ignored when storageConnectionString is present")
	}

	// Entity management is only possible when using Azure AD
	if m.EnableEntityManagement && m.ConnectionString != "" {
		m.EnableEntityManagement = false
		log.Warn("Entity management support is not available when connecting with a connection string")
	}

	if m.EventHubNamespace != "" {
		// Older versions of Dapr required the namespace name to be just the name and not a FQDN
		// Automatically append ".servicebus.windows.net" to make them a FQDN if not present, but show a log
		if !strings.ContainsRune(m.EventHubNamespace, '.') {
			m.EventHubNamespace += ".servicebus.windows.net"
			log.Info("Property eventHubNamespace is not a FQDN; the suffix '.servicebus.windows.net' will be added automatically")
		}

		// The namespace name is the first part of the FQDN, until the first dot
		m.namespaceName = m.EventHubNamespace[0:strings.IndexRune(m.EventHubNamespace, '.')]
	}

	return &m, nil
}

var hubNameMatch = regexp.MustCompile(`(?i)(^|;)EntityPath=([^;]+)(;|$)`)

// Returns the hub name (topic) from the connection string.
// TODO: Temporary until https://github.com/Azure/azure-sdk-for-go/issues/19840 is fixed - then use `conn.ParsedConnectionFromStr(aeh.metadata.ConnectionString)` and look at the `HubName` property.
func hubNameFromConnString(connString string) string {
	match := hubNameMatch.FindStringSubmatch(connString)
	if len(match) < 3 {
		return ""
	}
	return match[2]
}
