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
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"

	azauth "github.com/dapr/components-contrib/internal/authentication/azure"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

type AzureEventHubsMetadata struct {
	ConnectionString        string `json:"connectionString" mapstructure:"connectionString"`
	EventHubNamespace       string `json:"eventHubNamespace" mapstructure:"eventHubNamespace"`
	ConsumerID              string `json:"consumerID" mapstructure:"consumerID"`
	StorageConnectionString string `json:"storageConnectionString" mapstructure:"storageConnectionString"`
	StorageAccountName      string `json:"storageAccountName" mapstructure:"storageAccountName"`
	StorageAccountKey       string `json:"storageAccountKey" mapstructure:"storageAccountKey"`
	StorageContainerName    string `json:"storageContainerName" mapstructure:"storageContainerName"`
	EnableEntityManagement  bool   `json:"enableEntityManagement,string" mapstructure:"enableEntityManagement"`
	MessageRetentionInDays  int32  `json:"messageRetentionInDays,string" mapstructure:"messageRetentionInDays"`
	PartitionCount          int32  `json:"partitionCount,string" mapstructure:"partitionCount"`
	SubscriptionID          string `json:"subscriptionID" mapstructure:"subscriptionID"`
	ResourceGroupName       string `json:"resourceGroupName" mapstructure:"resourceGroupName"`

	// Binding only
	EventHub      string `json:"eventHub" mapstructure:"eventHub" mdonly:"bindings"`
	ConsumerGroup string `json:"consumerGroup" mapstructure:"consumerGroup" mdonly:"bindings"` // Alias for ConsumerID

	// Internal properties
	namespaceName string
	hubName       string
	azEnvSettings azauth.EnvironmentSettings
	properties    map[string]string
}

func parseEventHubsMetadata(meta map[string]string, isBinding bool, log logger.Logger) (*AzureEventHubsMetadata, error) {
	var m AzureEventHubsMetadata
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

	// ConsumerGroup is an alias for ConsumerID
	if m.ConsumerID != "" && m.ConsumerGroup == "" {
		m.ConsumerGroup = m.ConsumerID
	}

	// For the binding, we need to have a property "eventHub" which is the topic name unless it's included in the connection string
	if isBinding {
		if m.ConnectionString == "" {
			if m.EventHub == "" {
				return nil, errors.New("property 'eventHub' is required when connecting with Azure AD")
			}
			m.hubName = m.EventHub
		} else {
			hubName := hubNameFromConnString(m.ConnectionString)
			if hubName != "" {
				m.hubName = hubName
			} else if m.EventHub != "" {
				m.hubName = m.EventHub
			} else {
				return nil, errors.New("the provided connection string does not contain a value for 'EntityPath' and no 'eventHub' property was passed")
			}
		}
	} else {
		// Ignored when not a binding
		m.EventHub = ""

		// If connecting using a connection string, parse hubName
		if m.ConnectionString != "" {
			hubName := hubNameFromConnString(m.ConnectionString)
			if hubName != "" {
				log.Infof(`The provided connection string is specific to the Event Hub ("entity path") '%s'; publishing or subscribing to a topic that does not match this Event Hub will fail when attempted`, hubName)
			} else {
				log.Info(`The provided connection string does not contain an Event Hub name ("entity path"); the connection will be established on first publish/subscribe and req.Topic field in incoming requests will be honored`)
			}

			m.hubName = hubName
		}
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

// Returns the hub name (topic) from the connection string.
func hubNameFromConnString(connString string) string {
	props, err := azeventhubs.ParseConnectionString(connString)
	if err != nil || props.EntityPath == nil {
		return ""
	}
	return *props.EntityPath
}
