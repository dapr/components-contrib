package aws

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/dapr/components-contrib/common/aws/auth"
	"github.com/dapr/components-contrib/common/aws/options"
	"github.com/dapr/kit/logger"
	"sync"
)

type Provider struct {
	opts options.Options

	mutex  sync.RWMutex
	logger logger.Logger

	cancelConfigCtx context.CancelFunc

	awsConfig    *aws.Config
	authProvider auth.AuthProvider
}

func NewAWSConfigProvider(ctx context.Context, opts options.Options) (*Provider, error) {
	provider := &Provider{
		opts:   opts,
		logger: opts.Logger,
	}

	authProvider, err := auth.NewAuthProvider(ctx, opts)
	if err != nil {
		return nil, err
	}

	provider.authProvider = authProvider

	if err := provider.initOrRefreshConfig(ctx); err != nil {
		return nil, err
	}

	return provider, nil
}

func (p *Provider) initOrRefreshConfig(ctx context.Context) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	awsConfig := AWSConfigFromOptions(ctx, p.opts, config.WithCredentialsProvider(p.authProvider.GetAWSCredentialsProvider()))

	p.awsConfig = awsConfig
	return nil
}

func (p *Provider) GetConfig(ctx context.Context) (*aws.Config, error) {
	if p.awsConfig == nil {
		if err := p.initOrRefreshConfig(ctx); err != nil {
			return nil, err
		}
	}
	return p.awsConfig, nil
}

func (p *Provider) Close() error {
	p.cancelConfigCtx()
	// TODO: @mikeee - cleanup
	return nil
}
