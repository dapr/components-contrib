package internal

import (
	"fmt"
	"strings"

	"github.com/dapr/components-contrib/configuration"
)

func BuildRedisValue(content string, tags map[string]string) (map[string]string, error) {
	redisValue := map[string]string{
		contentKey: content,
	}

	for tagName, tagValue := range tags {
		tagKey := fmt.Sprintf("%s%s", tagKeyPrefix, tagName)
		redisValue[tagKey] = tagValue
	}

	return redisValue, nil
}

func ParseRedisValue(redisValueMap map[string]string, item *configuration.Item) {
	for k, v := range redisValueMap {
		if k == contentKey {
			item.Content = v
		} else if strings.Index(k, tagKeyPrefix) == 0 {
			tagName := k[len(tagKeyPrefix):]
			if item.Tags == nil {
				item.Tags = map[string]string{}
			}
			if tagName != "" {
				item.Tags[tagName] = v
			}
		}
	}
}
