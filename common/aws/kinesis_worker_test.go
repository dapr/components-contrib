package aws

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
)

func TestNewKinesisWorkerConfig(t *testing.T) {
	cfg := aws.Config{}
	assert.Nil(t, NewKinesisWorkerConfig(cfg, "s", "c", "shared"))

	cfg.Region = "us-east-1"
	cfg.Credentials = aws.NewCredentialsCache(&fakeCreds{})
	kcl := NewKinesisWorkerConfig(cfg, "stream", "consumer", "shared")
	assert.NotNil(t, kcl)
	assert.Equal(t, "stream", kcl.StreamName)
	assert.Equal(t, "consumer", kcl.ApplicationName)
	assert.Equal(t, cfg.Credentials, kcl.KinesisCredentials)

	assert.Nil(t, NewKinesisWorkerConfig(cfg, "s", "c", "extended"))
}
