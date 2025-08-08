package auth

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"

	"github.com/dapr/kit/logger"
)

type ProviderType int

const (
	StaticProviderTypeStatic ProviderType = iota
	StaticProviderTypeAssumeRole
	X509ProviderType
	ProviderTypeUnknown // Or default
)

type Options struct {
	Logger     logger.Logger
	Properties map[string]string

	Region                string `json:"region" mapstructure:"region" mapstructurealiases:"awsRegion"`
	AccessKey             string `json:"accessKey" mapstructure:"accessKey"`
	SecretKey             string `json:"secretKey" mapstructure:"secretKey"`
	SessionToken          string `json:"sessionToken" mapstructure:"sessionToken"`
	AssumeRoleArn         string `json:"assumeRoleArn" mapstructure:"assumeRoleArn"`
	AssumeRoleSessionName string `json:"assumeRoleSessionName" mapstructure:"assumeRoleSessionName"`
	TrustAnchorArn        string `json:"trustAnchorArn" mapstructure:"trustAnchorArn"`
	TrustProfileArn       string `json:"trustProfileArn" mapstructure:"trustProfileArn"`

	Endpoint string `json:"endpoint" mapstructure:"endpoint"`
}

// CredentialProvider provides an interface for retrieving AWS credentials.
type CredentialProvider interface {
	Retrieve(ctx context.Context) (aws.Credentials, error)
	Type() ProviderType
}

func NewCredentialProvider(ctx context.Context, opts Options, configOpts []func(*config.LoadOptions) error) (CredentialProvider,
	error,
) {
	// TODO: Refactor this to search the opts structure for the right fields rather than the metadata map
	if isX509Auth(opts.Properties) {
		return newAuthX509(ctx, opts)
	}
	return newAuthStatic(ctx, opts, configOpts)
}

// Coalesce is a helper function to return the first non-empty string from the inputs
// This helps us to migrate away from the deprecated duplicate aws auth profile metadata fields in Dapr 1.17.
func Coalesce(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
