package aws

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
)

func TestNewKinesisWorkerConfig(t *testing.T) {
	cfg := aws.Config{}
	// missing region should yield an error
	_, err := NewKinesisWorkerConfig(cfg, "s", "c", "shared")
	assert.Error(t, err)

	cfg.Region = "us-east-1"
	// credentials nil should still produce a configuration
	kcl, err := NewKinesisWorkerConfig(cfg, "stream", "consumer", "shared")
	assert.NoError(t, err)
	assert.NotNil(t, kcl)
	assert.Equal(t, "stream", kcl.StreamName)
	assert.Equal(t, "consumer", kcl.ApplicationName)

	cfg.Credentials = aws.NewCredentialsCache(&fakeCreds{})
	kcl, err = NewKinesisWorkerConfig(cfg, "stream", "consumer", "shared")
	assert.NoError(t, err)
	assert.NotNil(t, kcl)
	assert.Equal(t, cfg.Credentials, kcl.KinesisCredentials)

	// unsupported mode should return an error
	_, err = NewKinesisWorkerConfig(cfg, "s", "c", "extended")
	assert.Error(t, err)
}
