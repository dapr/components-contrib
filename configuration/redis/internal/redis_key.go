package internal

import (
	"errors"
	"fmt"
	"strings"

	"github.com/dapr/components-contrib/configuration"
)

const separator = "/"
const prefix = "dapr/configuration"
const defaultGroup = "default"
const defaultLabel = ""
const contentKey = "content"
const tagKeyPrefix = "tag-"

func BuildRedisKey(appID, group, label, key string) (string, error) {
	if appID == "" {
		return "", errors.New("appID should not be empty")
	}

	if strings.Contains(appID, separator) {
		return "", fmt.Errorf("appID should not contain separator(%s)", separator)
	}

	if group == "" {
		group = defaultGroup
	} else {
		if strings.Contains(group, separator) {
			return "", fmt.Errorf("group should not contain separator(%s)", separator)
		}
	}

	if label == "" {
		label = defaultLabel
	} else {
		if strings.Contains(label, separator) {
			return "", fmt.Errorf("label should not contain separator(%s)", separator)
		}
	}

	return fmt.Sprintf("%s%s%s%s%s%s%s%s%s", prefix, separator, appID, separator, group, separator, label, separator, key), nil
}

func ParseRedisKey(redisKey string, item *configuration.Item) error {
	if len(redisKey) == 0 {
		return errors.New("appID should not be empty")
	}
	
	if strings.Index(redisKey, prefix) != 0 {
		return fmt.Errorf("format error for redis configuration key: it should start with %s, but is %s", prefix, redisKey)
	}

	split := strings.Split(redisKey, separator)
	if len(split) != 6 {
		return fmt.Errorf("format error for redis configuration key: %s", redisKey)
	}

	item.Group = split[3]
	item.Label = split[4]
	item.Key = split[5]

	return nil
}
