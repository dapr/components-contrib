package internal

import (
	"errors"
	"fmt"
	"strings"
)

const separator = "/"
const prefix = "dapr/configuration"
const channelPrefix = "__keyspace@0__:"
const contentKey = "content"
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

func BuildRedisKey(appID string) (string, error) {
	if appID == "" {
		return "", errors.New("appID should not be empty")
	}

	if strings.Contains(appID, separator) {
		return "", fmt.Errorf("appID should not contain separator(%s)", separator)
	}

	return fmt.Sprintf("%s%s%s", prefix, separator, appID), nil
}

func ParseRedisKey(redisKey string) (string, error) {
	if len(redisKey) == 0 {
		return "", errors.New("redisKey should not be empty")
	}
	
	if strings.Index(redisKey, prefix) != 0 {
		return "", fmt.Errorf("format error for redis configuration key: it should start with %s, but is %s", prefix, redisKey)
	}

	split := strings.Split(redisKey, separator)
	if len(split) != 3 {
		return "", fmt.Errorf("format error for redis configuration key: %s", redisKey)
	}

	return split[2], nil
}

func ParseRedisKeyFromEvent(eventChannel string) (string, error) {
	index := strings.Index(eventChannel, channelPrefix)
	if index == -1 {
		return "", errors.New(fmt.Sprintf("wrong format of event channel, it should start with '%s': eventChannel=%s", channelPrefix, eventChannel))
	}

	return eventChannel[len(channelPrefix):], nil
}