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

package aws

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dapr/kit/logger"
)

type EnvironmentSettings struct {
	Metadata map[string]string
}

type Options struct {
	Logger     logger.Logger
	Properties map[string]string
	Region     string `json:"region" mapstructure:"region"`
	AccessKey  string `json:"accessKey" mapstructure:"accessKey"`
	SecretKey  string `json:"secretKey" mapstructure:"secretKey"`

	Endpoint     string
	SessionToken string
}

// TODO: Delete in Dapr 1.17 so we can move all IAM fields to use the defaults of:
// accessKey and secretKey and region as noted in the docs, and Options struct above.
type DeprecatedPostgresIAM struct {
	// Access key to use for accessing PostgreSQL.
	AccessKey string `json:"awsAccessKey" mapstructure:"awsAccessKey"`
	// Secret key to use for accessing PostgreSQL.
	SecretKey string `json:"awsSecretKey" mapstructure:"awsSecretKey"`
}

func GetConfig(opts Options) *aws.Config {
	cfg := aws.NewConfig()

	switch {
	case opts.Region != "":
		cfg.WithRegion(opts.Region)
	case opts.Endpoint != "":
		cfg.WithEndpoint(opts.Endpoint)
	}

	return cfg
}

//nolint:interfacebloat
type Provider interface {
	S3() *S3Clients
	DynamoDB() *DynamoDBClients
	Sqs() *SqsClients
	Sns() *SnsClients
	SnsSqs() *SnsSqsClients
	SecretManager() *SecretManagerClients
	ParameterStore() *ParameterStoreClients
	Kinesis() *KinesisClients
	Ses() *SesClients

	UpdatePostgres(context.Context, *pgxpool.Config)

	Close() error
}

func isX509Auth(m map[string]string) bool {
	tp, _ := m["trustProfileArn"]
	ta, _ := m["trustAnchorArn"]
	ar, _ := m["assumeRoleArn"]
	return tp != "" && ta != "" && ar != ""
}

func NewProvider(ctx context.Context, opts Options, cfg *aws.Config) (Provider, error) {
	if isX509Auth(opts.Properties) {
		return newX509(ctx, opts, cfg)
	}
	return newStaticIAM(ctx, opts, cfg)
}

// NewEnvironmentSettings returns a new EnvironmentSettings configured for a given AWS resource.
func NewEnvironmentSettings(md map[string]string) (EnvironmentSettings, error) {
	es := EnvironmentSettings{
		Metadata: md,
	}

	return es, nil
}
