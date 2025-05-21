package options

import "github.com/dapr/kit/logger"

type Options struct {
	Logger     logger.Logger
	Properties map[string]string

	// TODO: in Dapr 1.17 rm the alias on regions as we rm the aws prefixed one.
	// Docs have it just as region, but most metadata fields show the aws prefix...
	Region        string `json:"region" mapstructure:"region" mapstructurealiases:"awsRegion"`
	AccessKey     string `json:"accessKey" mapstructure:"accessKey"`
	SecretKey     string `json:"secretKey" mapstructure:"secretKey"`
	SessionName   string `json:"sessionName" mapstructure:"sessionName"`
	AssumeRoleARN string `json:"assumeRoleArn" mapstructure:"assumeRoleArn"`
	SessionToken  string `json:"sessionToken" mapstructure:"sessionToken"`

	Endpoint string
}
