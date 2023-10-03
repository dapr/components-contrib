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
	"errors"
	"fmt"
	"reflect"
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
	contribMetadata "github.com/dapr/components-contrib/metadata"

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
	NewListSettingsPager(selector azappconfig.SettingSelector, options *azappconfig.ListSettingsOptions) *runtime.Pager[azappconfig.ListSettingsPageResponse]
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
func (r *ConfigurationStore) Init(_ context.Context, md configuration.Metadata) error {
	r.metadata = metadata{}
	err := r.metadata.Parse(r.logger, md)
	if err != nil {
		return err
	}

	coreClientOpts := azcore.ClientOptions{
		Telemetry: policy.TelemetryOptions{
			ApplicationID: "dapr-" + logger.DaprVersion,
		},
		Retry: policy.RetryOptions{
			MaxRetries:    int32(r.metadata.MaxRetries),
			RetryDelay:    r.metadata.MaxRetryDelay,
			MaxRetryDelay: r.metadata.MaxRetryDelay,
		},
	}

	options := azappconfig.ClientOptions{
		ClientOptions: coreClientOpts,
	}

	if r.metadata.ConnectionString != "" {
		r.client, err = azappconfig.NewClientFromConnectionString(r.metadata.ConnectionString, &options)
		if err != nil {
			return err
		}
	} else {
		var settings azauth.EnvironmentSettings
		settings, err = azauth.NewEnvironmentSettings(md.Properties)
		if err != nil {
			return err
		}

		var cred azcore.TokenCredential
		cred, err = settings.GetTokenCredential()
		if err != nil {
			return err
		}

		r.client, err = azappconfig.NewClient(r.metadata.Host, cred, &options)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *ConfigurationStore) Get(ctx context.Context, req *configuration.GetRequest) (*configuration.GetResponse, error) {
	keys := req.Keys
	var items map[string]*configuration.Item

	if len(keys) == 0 {
		var err error
		if items, err = r.getAll(ctx, req); err != nil {
			return &configuration.GetResponse{}, err
		}
	} else {
		items = make(map[string]*configuration.Item, len(keys))
		for _, key := range keys {
			resp, err := r.getSettings(
				ctx,
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

	for allSettingsPgr.More() {
		timeoutContext, cancel := context.WithTimeout(ctx, r.metadata.RequestTimeout)
		defer cancel()
		if revResp, err := allSettingsPgr.NextPage(timeoutContext); err == nil {
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
		return "", fmt.Errorf("sentinel key is not provided in metadata")
	}
	uuid, err := uuid.NewRandom()
	if err != nil {
		return "", fmt.Errorf("failed to generate uuid, error is %w", err)
	}
	subscribeID := uuid.String()
	childContext, cancel := context.WithCancel(ctx)
	r.subscribeCancelCtxMap.Store(subscribeID, cancel)
	go r.doSubscribe(childContext, req, handler, sentinelKey, subscribeID)
	return subscribeID, nil
}

func (r *ConfigurationStore) doSubscribe(ctx context.Context, req *configuration.SubscribeRequest, handler configuration.UpdateHandler, sentinelKey string, id string) {
	var etagVal *azcore.ETag
	for {
		// get sentinel key changes.
		resp, err := r.getSettings(
			ctx,
			sentinelKey,
			&azappconfig.GetSettingOptions{
				Label:         r.getLabelFromMetadata(req.Metadata),
				OnlyIfChanged: etagVal,
			},
		)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			r.logger.Debugf("Failed to get sentinel key or sentinel's key %s value is unchanged: %s", sentinelKey, err)
		} else {
			// if sentinel key has changed then update the Etag value.
			etagVal = resp.ETag
			items, err := r.Get(ctx, &configuration.GetRequest{
				Keys:     req.Keys,
				Metadata: req.Metadata,
			})
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				r.logger.Errorf("Failed to get configuration key changes: %s", err)
			} else {
				r.handleSubscribedChange(ctx, handler, items, id)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(r.metadata.SubscribePollInterval):
		}
	}
}

func (r *ConfigurationStore) getSettings(ctx context.Context, key string, getSettingsOptions *azappconfig.GetSettingOptions) (azappconfig.GetSettingResponse, error) {
	timeoutContext, cancel := context.WithTimeout(ctx, r.metadata.RequestTimeout)
	defer cancel()
	resp, err := r.client.GetSetting(timeoutContext, key, getSettingsOptions)
	return resp, err
}

func (r *ConfigurationStore) handleSubscribedChange(ctx context.Context, handler configuration.UpdateHandler, items *configuration.GetResponse, id string) {
	e := &configuration.UpdateEvent{
		Items: items.Items,
		ID:    id,
	}
	err := handler(ctx, e)
	if err != nil {
		r.logger.Errorf("Failed to call handler to notify event for configuration update subscribe: %s", err)
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
	return fmt.Errorf("subscription with id %s does not exist", req.ID)
}

// GetComponentMetadata returns the metadata of the component.
func (r *ConfigurationStore) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := metadata{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.ConfigurationStoreType)
	return
}
