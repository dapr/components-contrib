package internal

import (
	"errors"
	"fmt"
	"strings"

	"github.com/dapr/components-contrib/configuration"
)

const separator = "/"
const prefix = "dapr/configuration"
const channelPrefix = "__keyspace@0__:"
const contentKey = "content"
const revisionKey = "_revision"
const tagKeyPrefix = "tag-"

func BuildRedisKeyPattern(appID string) (string, error) {
	if appID == "" {
		return "", errors.New("appID should not be empty")
	}

	if strings.Contains(appID, separator) {
		return "", fmt.Errorf("appID should not contain separator(%s)", separator)
	}

	return fmt.Sprintf("%s%s%s%s%s", prefix, separator, appID, separator, "*"), nil
}

func BuildRedisKey(appID, key string) (string, error) {
	if appID == "" {
		return "", errors.New("appID should not be empty")
	}

	if strings.Contains(appID, separator) {
		return "", fmt.Errorf("appID should not contain separator(%s)", separator)
	}

	return fmt.Sprintf("%s%s%s%s%s", prefix, separator, appID, separator, key), nil
}

func BuildRedisKey4Revision(appID string) (string, error) {
	if appID == "" {
		return "", errors.New("appID should not be empty")
	}

	if strings.Contains(appID, separator) {
		return "", fmt.Errorf("appID should not contain separator(%s)", separator)
	}

	return fmt.Sprintf("%s%s%s%s%s", prefix, separator, appID, separator, revisionKey), nil
}

func IsRevisionKey(key string) bool {
	return key == revisionKey
}

func ParseRedisKey(redisKey string, item *configuration.Item) error {
	if len(redisKey) == 0 {
		return errors.New("redisKey should not be empty")
	}
	
	if strings.Index(redisKey, prefix) != 0 {
		return fmt.Errorf("format error for redis configuration key: it should start with %s, but is %s", prefix, redisKey)
	}

	split := strings.Split(redisKey, separator)
	if len(split) != 4 {
		return fmt.Errorf("format error for redis configuration key: %s", redisKey)
	}

	item.Key = split[3]

	return nil
}

func ParseRedisKeyFromEvent(eventChannel string) (string, error) {
	index := strings.Index(eventChannel, channelPrefix)
	if index == -1 {
		return "", errors.New(fmt.Sprintf("wrong format of event channel, it should start with '%s': eventChannel=%s", channelPrefix, eventChannel))
	}

	return eventChannel[len(channelPrefix):], nil
}