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

package appconfig

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azappconfig"
	"github.com/google/uuid"

	"github.com/dapr/components-contrib/configuration"
	azauth "github.com/dapr/components-contrib/internal/authentication/azure"

	"github.com/dapr/kit/logger"
)

const (
	host                 = "appConfigHost"
	connectionString     = "appConfigConnectionString"
	maxRetries           = "maxRetries"
	retryDelay           = "retryDelay"
	maxRetryDelay        = "maxRetryDelay"
	defaultMaxRetries    = 3
	defaultRetryDelay    = time.Second * 4
	defaultMaxRetryDelay = time.Second * 120
)

// ConfigurationStore is a Azure App Configuration store.
type ConfigurationStore struct {
	client   *azappconfig.Client
	metadata metadata

	logger logger.Logger
}

// NewAzureAppConfigurationStore returns a new Azure App Configuration store.
func NewAzureAppConfigurationStore(logger logger.Logger) configuration.Store {
	s := &ConfigurationStore{
		logger: logger,
	}

	return s
}

// Init does metadata and connection parsing.
func (r *ConfigurationStore) Init(metadata configuration.Metadata) error {
	m, err := parseMetadata(metadata)
	if err != nil {
		return err
	}
	r.metadata = m

	coreClientOpts := azcore.ClientOptions{
		Telemetry: policy.TelemetryOptions{
			ApplicationID: "dapr-" + logger.DaprVersion,
		},
		Retry: policy.RetryOptions{
			MaxRetries:    int32(m.maxRetries),
			RetryDelay:    m.maxRetryDelay,
			MaxRetryDelay: m.maxRetryDelay,
		},
	}

	options := azappconfig.ClientOptions{
		ClientOptions: coreClientOpts,
	}

	if r.metadata.connectionString != "" {
		r.client, err = azappconfig.NewClientFromConnectionString(r.metadata.connectionString, &options)
		if err != nil {
			return err
		}
	} else {
		var settings azauth.EnvironmentSettings
		settings, err = azauth.NewEnvironmentSettings("appconfig", metadata.Properties)
		if err != nil {
			return err
		}

		var cred azcore.TokenCredential
		cred, err = settings.GetTokenCredential()
		if err != nil {
			return err
		}

		r.client, err = azappconfig.NewClient(r.metadata.host, cred, &options)
		if err != nil {
			return err
		}
	}

	return nil
}

func parseMetadata(meta configuration.Metadata) (metadata, error) {
	m := metadata{}

	if val, ok := meta.Properties[host]; ok && val != "" {
		m.host = val
	}

	if val, ok := meta.Properties[connectionString]; ok && val != "" {
		m.connectionString = val
	}

	if m.connectionString != "" && m.host != "" {
		return m, fmt.Errorf("azure store error: can't set both %s and %s fields in metadata", host, connectionString)
	}

	if m.connectionString == "" && m.host == "" {
		return m, fmt.Errorf("azure store error: specify %s or %s field in metadata", host, connectionString)
	}

	m.maxRetries = defaultMaxRetries
	if val, ok := meta.Properties[maxRetries]; ok && val != "" {
		parsedVal, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("azure store error: can't parse maxRetries field: %s", err)
		}
		m.maxRetries = parsedVal
	}

	m.maxRetryDelay = defaultMaxRetryDelay
	if val, ok := meta.Properties[maxRetryDelay]; ok && val != "" {
		parsedVal, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("azure store error: can't parse maxRetryDelay field: %s", err)
		}
		m.maxRetryDelay = time.Duration(parsedVal)
	}

	m.retryDelay = defaultRetryDelay
	if val, ok := meta.Properties[retryDelay]; ok && val != "" {
		parsedVal, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("azure store error: can't parse retryDelay field: %s", err)
		}
		m.retryDelay = time.Duration(parsedVal)
	}

	return m, nil
}

func (r *ConfigurationStore) Get(ctx context.Context, req *configuration.GetRequest) (*configuration.GetResponse, error) {
	keys := req.Keys
	items := make([]*configuration.Item, 0, len(keys))
	for _, key := range keys {
		resp, err := r.client.GetSetting(
			ctx,
			key,
			&azappconfig.GetSettingOptions{
				Label: to.Ptr("label"),
			})
		if err != nil {
			return nil, err
		}

		item := &configuration.Item{
			Metadata: map[string]string{},
		}
		item.Key = key
		item.Value = *resp.Value
		item.Metadata["label"] = *resp.Label

		items = append(items, item)
	}

	return &configuration.GetResponse{
		Items: items,
	}, nil
}

func (r *ConfigurationStore) Subscribe(ctx context.Context, req *configuration.SubscribeRequest, handler configuration.UpdateHandler) (string, error) {
	subscribeID := uuid.New().String()
	return subscribeID, nil
}

func (r *ConfigurationStore) Unsubscribe(ctx context.Context, req *configuration.UnsubscribeRequest) error {
	return nil
}
