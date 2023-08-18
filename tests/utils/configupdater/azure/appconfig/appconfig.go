package redis

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azappconfig"

	"github.com/dapr/components-contrib/configuration"
	"github.com/dapr/components-contrib/configuration/azure/appconfig"
	azauth "github.com/dapr/components-contrib/internal/authentication/azure"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/tests/utils/configupdater"
	"github.com/dapr/kit/logger"
)

type ConfigUpdater struct {
	client   *azappconfig.Client
	metadata appconfig.AppConfigMetadata
	logger   logger.Logger
}

func NewAzureAppconfigConfigUpdater(logger logger.Logger) configupdater.Updater {
	return &ConfigUpdater{
		logger: logger,
	}
}

func NewRedisConfigUpdater(logger logger.Logger) configupdater.Updater {
	return &ConfigUpdater{
		logger: logger,
	}
}

func (r *ConfigUpdater) Init(props map[string]string) error {
	err := r.metadata.Parse(r.logger, configuration.Metadata{Base: contribMetadata.Base{Properties: props}})
	if err != nil {
		return err
	}

	if r.metadata.ConnectionString != "" {
		r.client, err = azappconfig.NewClientFromConnectionString(r.metadata.ConnectionString, nil)
		if err != nil {
			return err
		}
	} else {
		var settings azauth.EnvironmentSettings
		settings, err = azauth.NewEnvironmentSettings(props)
		if err != nil {
			return err
		}

		var cred azcore.TokenCredential
		cred, err = settings.GetTokenCredential()
		if err != nil {
			return err
		}

		r.client, err = azappconfig.NewClient(r.metadata.Host, cred, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *ConfigUpdater) AddKey(items map[string]*configuration.Item) error {
	for key, item := range items {
		_, err := r.client.AddSetting(
			context.TODO(), key, to.Ptr(item.Value),
			&azappconfig.AddSettingOptions{
				Label: to.Ptr(item.Metadata["label"]),
			})
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *ConfigUpdater) UpdateKey(items map[string]*configuration.Item) error {
	for key, item := range items {
		_, err := r.client.SetSetting(
			context.TODO(), key, to.Ptr(item.Value),
			&azappconfig.SetSettingOptions{
				Label: to.Ptr(item.Metadata["label"]),
			})
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *ConfigUpdater) DeleteKey(keys []string) error {
	for _, key := range keys {
		_, err := r.client.DeleteSetting(context.TODO(), key, nil)
		if err != nil {
			return err
		}
	}
	return nil
}
