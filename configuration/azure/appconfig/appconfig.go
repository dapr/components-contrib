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
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azappconfig"
	"github.com/google/uuid"

	"github.com/dapr/components-contrib/configuration"
	azauth "github.com/dapr/components-contrib/internal/authentication/azure"

	"github.com/dapr/kit/logger"
)

const (
	host                         = "host"
	connectionString             = "connectionString"
	maxRetries                   = "maxRetries"
	retryDelay                   = "retryDelay"
	maxRetryDelay                = "maxRetryDelay"
	subscribePollInterval        = "subscribePollInterval"
	requestTimeout               = "requestTimeout"
	defaultMaxRetries            = 3
	defaultRetryDelay            = time.Second * 4
	defaultMaxRetryDelay         = time.Second * 120
	defaultSubscribePollInterval = time.Hour * 24
	defaultRequestTimeout        = time.Second * 15
)

type azAppConfigClient interface {
	GetSetting(ctx context.Context, key string, options *azappconfig.GetSettingOptions) (azappconfig.GetSettingResponse, error)
	NewListSettingsPager(selector azappconfig.SettingSelector, options *azappconfig.ListSettingsOptions) *runtime.Pager[azappconfig.ListSettingsPage]
}

// ConfigurationStore is a Azure App Configuration store.
type ConfigurationStore struct {
	client                azAppConfigClient
	metadata              metadata
	subscribeCancelCtxMap sync.Map

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
		return m, fmt.Errorf("azure appconfig error: can't set both %s and %s fields in metadata", host, connectionString)
	}

	if m.connectionString == "" && m.host == "" {
		return m, fmt.Errorf("azure appconfig error: specify %s or %s field in metadata", host, connectionString)
	}

	m.maxRetries = defaultMaxRetries
	if val, ok := meta.Properties[maxRetries]; ok && val != "" {
		parsedVal, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("azure appconfig error: can't parse maxRetries field: %w", err)
		}
		m.maxRetries = parsedVal
	}

	m.maxRetryDelay = defaultMaxRetryDelay
	if val, ok := meta.Properties[maxRetryDelay]; ok && val != "" {
		parsedVal, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("azure appconfig error: can't parse maxRetryDelay field: %w", err)
		}
		m.maxRetryDelay = time.Duration(parsedVal)
	}

	m.retryDelay = defaultRetryDelay
	if val, ok := meta.Properties[retryDelay]; ok && val != "" {
		parsedVal, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("azure appconfig error: can't parse retryDelay field: %w", err)
		}
		m.retryDelay = time.Duration(parsedVal)
	}

	m.subscribePollInterval = defaultSubscribePollInterval
	if val, ok := meta.Properties[subscribePollInterval]; ok && val != "" {
		parsedVal, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("azure appconfig error: can't parse subscribePollInterval field: %w", err)
		}
		m.subscribePollInterval = time.Duration(parsedVal)
	}

	m.requestTimeout = defaultRequestTimeout
	if val, ok := meta.Properties[requestTimeout]; ok && val != "" {
		parsedVal, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("azure appconfig error: can't parse requestTimeout field: %w", err)
		}
		m.requestTimeout = time.Duration(parsedVal)
	}

	return m, nil
}

func (r *ConfigurationStore) Get(ctx context.Context, req *configuration.GetRequest) (*configuration.GetResponse, error) {
	timeoutContext, cancel := context.WithTimeout(ctx, r.metadata.requestTimeout)
	defer cancel()

	keys := req.Keys
	var items map[string]*configuration.Item

	if len(keys) == 0 {
		var err error
		if items, err = r.getAll(timeoutContext, req); err != nil {
			return &configuration.GetResponse{}, err
		}
	} else {
		items = make(map[string]*configuration.Item, len(keys))
		for _, key := range keys {
			// TODO: here contxt.TODO() is used because the SDK panics when a cancelled context is passed in GetSetting
			// Issue - https://github.com/Azure/azure-sdk-for-go/issues/19223 . Needs to be modified to use timeoutContext once the SDK is fixed
			resp, err := r.client.GetSetting(
				context.TODO(),
				key,
				&azappconfig.GetSettingOptions{
					Label: r.getLabelFromMetadata(req.Metadata),
				},
			)
			if err != nil {
				return &configuration.GetResponse{}, err
			}

			item := &configuration.Item{
				Metadata: map[string]string{},
			}
			item.Value = *resp.Value
			if resp.Label != nil {
				item.Metadata["label"] = *resp.Label
			}

			items[key] = item
		}
	}
	return &configuration.GetResponse{
		Items: items,
	}, nil
}

func (r *ConfigurationStore) getAll(ctx context.Context, req *configuration.GetRequest) (map[string]*configuration.Item, error) {
	items := make(map[string]*configuration.Item, 0)

	labelFilter := r.getLabelFromMetadata(req.Metadata)
	if labelFilter == nil {
		labelFilter = to.Ptr("*")
	}

	allSettingsPgr := r.client.NewListSettingsPager(
		azappconfig.SettingSelector{
			KeyFilter:   to.Ptr("*"),
			LabelFilter: labelFilter,
			Fields:      azappconfig.AllSettingFields(),
		},
		nil)

	// TODO: here contxt.TODO() is used because the SDK panics when a cancelled context is passed in NextPage
	// Issue - https://github.com/Azure/azure-sdk-for-go/issues/19223 . It needs to be modified to use ctx once the SDK is fixed
	for allSettingsPgr.More() {
		if revResp, err := allSettingsPgr.NextPage(context.TODO()); err == nil {
			for _, setting := range revResp.Settings {
				item := &configuration.Item{
					Metadata: map[string]string{},
				}
				item.Value = *setting.Value
				if setting.Label != nil {
					item.Metadata["label"] = *setting.Label
				}

				items[*setting.Key] = item
			}
		} else {
			return nil, fmt.Errorf("failed to load all keys, error is %w", err)
		}
	}
	return items, nil
}

func (r *ConfigurationStore) getLabelFromMetadata(metadata map[string]string) *string {
	if s, ok := metadata["label"]; ok && s != "" {
		return to.Ptr(s)
	}

	return nil
}

func (r *ConfigurationStore) Subscribe(ctx context.Context, req *configuration.SubscribeRequest, handler configuration.UpdateHandler) (string, error) {
	sentinelKey := r.getSentinelKeyFromMetadata(req.Metadata)
	if sentinelKey == "" {
		return "", fmt.Errorf("azure appconfig error: sentinel key is not provided in metadata")
	}
	uuid, err := uuid.NewRandom()
	if err != nil {
		return "", fmt.Errorf("azure appconfig error: failed to generate uuid, error is %w", err)
	}
	subscribeID := uuid.String()
	childContext, cancel := context.WithCancel(ctx)
	r.subscribeCancelCtxMap.Store(subscribeID, cancel)
	go r.doSubscribe(childContext, req, handler, sentinelKey, subscribeID)
	return subscribeID, nil
}

func (r *ConfigurationStore) doSubscribe(ctx context.Context, req *configuration.SubscribeRequest, handler configuration.UpdateHandler, sentinelKey string, id string) {
	for {
		// get sentinel key changes
		_, err := r.Get(ctx, &configuration.GetRequest{
			Keys:     []string{sentinelKey},
			Metadata: req.Metadata,
		})
		if err != nil {
			r.logger.Debugf("azure appconfig error: fail to get sentinel key changes or sentinel key's value is unchanged: %s", err)
		} else {
			items, err := r.Get(ctx, &configuration.GetRequest{
				Keys:     req.Keys,
				Metadata: req.Metadata,
			})
			if err != nil {
				r.logger.Errorf("azure appconfig error: fail to get configuration key changes: %s", err)
			} else {
				r.handleSubscribedChange(ctx, handler, items, id)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(r.metadata.subscribePollInterval):
		}
	}
}

func (r *ConfigurationStore) handleSubscribedChange(ctx context.Context, handler configuration.UpdateHandler, items *configuration.GetResponse, id string) {
	e := &configuration.UpdateEvent{
		Items: items.Items,
		ID:    id,
	}
	err := handler(ctx, e)
	if err != nil {
		r.logger.Errorf("azure appconfig error: fail to call handler to notify event for configuration update subscribe: %s", err)
	}
}

func (r *ConfigurationStore) getSentinelKeyFromMetadata(metadata map[string]string) string {
	if s, ok := metadata["sentinelKey"]; ok && s != "" {
		return s
	}
	return ""
}

func (r *ConfigurationStore) Unsubscribe(ctx context.Context, req *configuration.UnsubscribeRequest) error {
	if cancelContext, ok := r.subscribeCancelCtxMap.Load(req.ID); ok {
		// already exist subscription
		r.subscribeCancelCtxMap.Delete(req.ID)
		cancelContext.(context.CancelFunc)()
		return nil
	}
	return fmt.Errorf("azure appconfig error: subscription with id %s does not exist", req.ID)
}
