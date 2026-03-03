/*
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
	http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kafka

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/IBM/sarama"
)

const (
	seekOnStartNever    = "never"
	seekOnStartEarliest = "earliest"
	seekOnStartLatest   = "latest"
	seekOnStartOffset   = "offset"
	seekOnStartTS       = "timestamp"

	seekApplyWhenAlways         = "always"
	seekApplyWhenIfNoCheckpoint = "ifNoCheckpoint"
)

type seekMode int

const (
	seekModeNever seekMode = iota
	seekModeEarliest
	seekModeLatest
	seekModeOffset
	seekModeTimestamp
)

func (s seekMode) String() string {
	switch s {
	case seekModeEarliest:
		return seekOnStartEarliest
	case seekModeLatest:
		return seekOnStartLatest
	case seekModeOffset:
		return seekOnStartOffset
	case seekModeTimestamp:
		return seekOnStartTS
	default:
		return seekOnStartNever
	}
}

type startupSeekConfig struct {
	mode      seekMode
	value     int64
	applyWhen string
	seekOnce  bool
	partition *int32
	enabled   bool
}

type startupSeekKey struct {
	group     string
	topic     string
	partition int32
}

func parseStartupSeekConfig(meta map[string]string, seekOnStart, seekValue, seekApplyWhen string, seekOnce bool) (startupSeekConfig, error) {
	conf := startupSeekConfig{
		mode:      seekModeNever,
		applyWhen: seekApplyWhenIfNoCheckpoint,
		seekOnce:  true,
		enabled:   false,
	}

	if seekApplyWhen != "" {
		switch strings.ToLower(seekApplyWhen) {
		case strings.ToLower(seekApplyWhenAlways):
			conf.applyWhen = seekApplyWhenAlways
		case strings.ToLower(seekApplyWhenIfNoCheckpoint):
			conf.applyWhen = seekApplyWhenIfNoCheckpoint
		default:
			return conf, fmt.Errorf("kafka error: invalid seekApplyWhen: %s", seekApplyWhen)
		}
	}

	conf.seekOnce = seekOnce

	if seekOnStart == "" {
		return conf, nil
	}

	switch strings.ToLower(seekOnStart) {
	case seekOnStartNever:
		return conf, nil
	case seekOnStartEarliest:
		conf.mode = seekModeEarliest
	case seekOnStartLatest:
		conf.mode = seekModeLatest
	case seekOnStartOffset:
		if seekValue == "" {
			return conf, fmt.Errorf("kafka error: missing seekValue for seekOnStart=%s", seekOnStartOffset)
		}
		offset, err := strconv.ParseInt(seekValue, 10, 64)
		if err != nil || offset < 0 {
			return conf, fmt.Errorf("kafka error: invalid seekValue for seekOnStart=%s", seekOnStartOffset)
		}
		conf.mode = seekModeOffset
		conf.value = offset
	case seekOnStartTS:
		if seekValue == "" {
			return conf, fmt.Errorf("kafka error: missing seekValue for seekOnStart=%s", seekOnStartTS)
		}
		ts, err := strconv.ParseInt(seekValue, 10, 64)
		if err != nil {
			return conf, fmt.Errorf("kafka error: invalid seekValue for seekOnStart=%s", seekOnStartTS)
		}
		conf.mode = seekModeTimestamp
		conf.value = ts
	default:
		return conf, fmt.Errorf("kafka error: invalid seekOnStart: %s", seekOnStart)
	}

	if seekPartitionRaw, ok := meta["seekPartition"]; ok && seekPartitionRaw != "" {
		partition, err := strconv.ParseInt(seekPartitionRaw, 10, 32)
		if err != nil || partition < 0 {
			return conf, fmt.Errorf("kafka error: invalid seekPartition: %s", seekPartitionRaw)
		}
		partition32 := int32(partition)
		conf.partition = &partition32
	}

	conf.enabled = true

	return conf, nil
}

func (k *Kafka) wasStartupSeekApplied(key startupSeekKey) bool {
	k.startupSeekLock.Lock()
	defer k.startupSeekLock.Unlock()
	_, ok := k.startupSeekApplied[key]
	return ok
}

func (k *Kafka) markStartupSeekApplied(key startupSeekKey) {
	k.startupSeekLock.Lock()
	defer k.startupSeekLock.Unlock()
	k.startupSeekApplied[key] = struct{}{}
}

func (k *Kafka) ensureSeekClient() (sarama.Client, error) {
	k.seekClientLock.Lock()
	defer k.seekClientLock.Unlock()

	if k.seekClient != nil {
		return k.seekClient, nil
	}

	client, err := sarama.NewClient(k.brokers, k.config)
	if err != nil {
		return nil, err
	}

	k.seekClient = client
	return k.seekClient, nil
}

func (k *Kafka) getOffsetForTimestamp(topic string, partition int32, timestampMillis int64) (int64, error) {
	if k.offsetLookupFn != nil {
		return k.offsetLookupFn(topic, partition, timestampMillis)
	}

	client, err := k.ensureSeekClient()
	if err != nil {
		return 0, err
	}

	return client.GetOffset(topic, partition, timestampMillis)
}

func (k *Kafka) getCommittedOffset(topic string, partition int32) (int64, error) {
	if k.committedOffsetFn != nil {
		return k.committedOffsetFn(topic, partition)
	}

	client, err := k.ensureSeekClient()
	if err != nil {
		return 0, err
	}

	offsetManager, err := sarama.NewOffsetManagerFromClient(k.consumerGroup, client)
	if err != nil {
		return 0, err
	}
	defer offsetManager.Close()

	partitionManager, err := offsetManager.ManagePartition(topic, partition)
	if err != nil {
		return 0, err
	}
	defer partitionManager.Close()

	nextOffset, _ := partitionManager.NextOffset()
	return nextOffset, nil
}

func (k *Kafka) closeSeekClient() error {
	k.seekClientLock.Lock()
	defer k.seekClientLock.Unlock()

	if k.seekClient == nil {
		return nil
	}

	err := k.seekClient.Close()
	k.seekClient = nil
	return err
}

func makeStartupSeekKey(group, topic string, partition int32) startupSeekKey {
	return startupSeekKey{
		group:     group,
		topic:     topic,
		partition: partition,
	}
}

func shouldApplyStartupSeek(conf startupSeekConfig, committedOffset int64) bool {
	if conf.applyWhen == seekApplyWhenAlways {
		return true
	}

	return committedOffset < 0
}
