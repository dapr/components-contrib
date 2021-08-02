package rocketmq

import (
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"testing"
)

func getTestMetadata() map[string]string {
	return map[string]string{
		"nameServer":         "http://**.mq-internet-access.mq-internet.aliyuncs.com:80",
		"consumerGroup":      "GID_DAPR-MQ-TCP",
		"accessKey":          "**",
		"secretKey":          "**",
		"consumerBatchSize":  "1",
		"consumerThreadNums": "2",
	}
}

func TestNewRocketMQ(t *testing.T) {
	m := pubsub.Metadata{Properties: getTestMetadata()}
	r := NewRocketMQ(logger.NewLogger("test"))
	r.Init(m)
}
