package snssqs

import (
	"os"
	"testing"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
)

// TestIntegrationGetSecret requires AWS specific environments for authentication AWS_DEFAULT_REGION AWS_ACCESS_KEY_ID,
// AWS_SECRET_ACCESS_KkEY and AWS_SESSION_TOKEN
func TestIntegrationCreateAllSnsSqs(t *testing.T) {
	snsq := NewSnsSqs(logger.NewLogger("test"))
	err := snsq.Init(snssqs.snsSqsMetadata{
		Properties: map[string]string{
			"Region":       os.Getenv("AWS_DEFAULT_REGION"),
			"AccessKey":    os.Getenv("AWS_ACCESS_KEY_ID"),
			"SecretKey":    os.Getenv("AWS_SECRET_ACCESS_KEY"),
			"SessionToken": os.Getenv("AWS_SESSION_TOKEN"),
		},
	})
	assert.Nil(t, err)
	// snsq.Subscribe()

	assert.Nil(t, err)
	assert.NotNil(t, response)
}
