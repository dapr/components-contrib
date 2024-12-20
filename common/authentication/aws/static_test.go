package aws

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetConfigV2(t *testing.T) {
	tests := []struct {
		name         string
		accessKey    string
		secretKey    string
		sessionToken string
		region       string
		endpoint     string
	}{
		{
			name:         "valid config",
			accessKey:    "testAccessKey",
			secretKey:    "testSecretKey",
			sessionToken: "testSessionToken",
			region:       "us-west-2",
			endpoint:     "https://test.endpoint.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			awsCfg, err := GetConfigV2(tt.accessKey, tt.secretKey, tt.sessionToken, tt.region, tt.endpoint)
			require.NoError(t, err)
			assert.NotNil(t, awsCfg)
			assert.Equal(t, tt.region, awsCfg.Region)
			assert.Equal(t, tt.endpoint, *awsCfg.BaseEndpoint)
		})
	}
}

func TestGetTokenClient(t *testing.T) {
	tests := []struct {
		name        string
		awsInstance *StaticAuth
	}{
		{
			name: "valid token client",
			awsInstance: &StaticAuth{
				accessKey:    aws.String("testAccessKey"),
				secretKey:    aws.String("testSecretKey"),
				sessionToken: "testSessionToken",
				region:       aws.String("us-west-2"),
				endpoint:     aws.String("https://test.endpoint.com"),
			},
		},
		{
			name: "creds from environment",
			awsInstance: &StaticAuth{
				region: aws.String("us-west-2"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			session, err := tt.awsInstance.createSession()
			require.NotNil(t, session)
			require.NoError(t, err)
			assert.Equal(t, tt.awsInstance.region, session.Config.Region)
		})
	}
}
