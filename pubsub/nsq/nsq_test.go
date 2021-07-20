package nsq

import (
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseNSQMetadata(t *testing.T) {
	t.Run("metadata is correct", func(t *testing.T) {
		fakeProperties := map[string]string{
			"nsq_lookups":           "10.42.0.10:4160,10.42.0.20:4160",
			"nsq_address":           "10.42.0.10:4150,10.42.0.20:4150",
			"dial_timeout":          "10s",
			"read_timeout":          "30s",
			"write_timeout":         "30s",
			"msg_timeout":           "30s",
			"max_requeue_delay":     "10s",
			"lookupd_poll_interval": "10s",
			"default_requeue_delay": "10s",
			"heartbeat_interval":    "30s",
			"client_id":             "hello",
			"user_agent":            "google",
			"auth_secret":           "password",
			"lookupd_poll_jitter":   "3.1415",
			"max_attempts":          "3",
			"max_in_flight":         "200",
			"sample_rate":           "1",
		}
		// paser metadata
		settings := Settings{}
		err := settings.Decode(fakeProperties)
		config := settings.ToNSQConfig()
		// assert
		assert.NoError(t, err)
		assert.NotEmpty(t, settings.NsqLookups)
		lookups := strings.Join(settings.lookups, ",")
		assert.Equal(t, fakeProperties["nsq_lookups"], lookups)

		assert.NotEmpty(t, settings.NsqAddress)
		nsqds := strings.Join(settings.nsqds, ",")
		assert.Equal(t, fakeProperties["nsq_address"], nsqds)

		// nsqconfig value
		dialTimeOut, _ := time.ParseDuration(fakeProperties["dial_timeout"])
		assert.Equal(t, dialTimeOut, config.DialTimeout)
		readtTimeOut, _ := time.ParseDuration(fakeProperties["read_timeout"])
		assert.Equal(t, readtTimeOut, config.ReadTimeout)
		writeTimeOut, _ := time.ParseDuration(fakeProperties["write_timeout"])
		assert.Equal(t, writeTimeOut, config.WriteTimeout)
		msgTimeOut, _ := time.ParseDuration(fakeProperties["msg_timeout"])
		assert.Equal(t, msgTimeOut, config.MsgTimeout)
		maxRequeueDelay, _ := time.ParseDuration(fakeProperties["max_requeue_delay"])
		assert.Equal(t, maxRequeueDelay, config.MaxRequeueDelay)
		lookupdPollInterval, _ := time.ParseDuration(fakeProperties["lookupd_poll_interval"])
		assert.Equal(t, lookupdPollInterval, config.LookupdPollInterval)
		defaultRequeueDelay, _ := time.ParseDuration(fakeProperties["default_requeue_delay"])
		assert.Equal(t, defaultRequeueDelay, config.DefaultRequeueDelay)
		heartbeatInterval, _ := time.ParseDuration(fakeProperties["heartbeat_interval"])
		assert.Equal(t, heartbeatInterval, config.HeartbeatInterval)

		assert.Equal(t, config.ClientID, fakeProperties["client_id"])
		assert.Equal(t, config.UserAgent, fakeProperties["user_agent"])
		assert.Equal(t, config.AuthSecret, fakeProperties["auth_secret"])

		lookupdPollJitter, _ := strconv.ParseFloat(fakeProperties["lookupd_poll_jitter"], 32)
		equal := math.Abs(lookupdPollJitter-config.LookupdPollJitter) < 0.0001
		assert.Equal(t, equal, true)

		maxAttempts, _ := strconv.ParseInt(fakeProperties["max_attempts"], 10, 32)
		assert.Equal(t, int(maxAttempts), int(config.MaxAttempts))
		maxInFlight, _ := strconv.ParseInt(fakeProperties["max_in_flight"], 10, 32)
		assert.Equal(t, int(maxInFlight), config.MaxInFlight)
		sampleRate, _ := strconv.ParseInt(fakeProperties["sample_rate"], 10, 32)
		assert.Equal(t, int(sampleRate), int(config.SampleRate))
	})

	t.Run("subscribe metadata is correct", func(t *testing.T) {
		fakeProperties := map[string]string{
			"concurrency": "4",
			"channel":     "nsqchannel",
		}

		settings := Settings{}
		// paser metadata
		err := settings.Decode(fakeProperties)
		assert.NoError(t, err)

		// assert
		concurrency, _ := strconv.ParseInt(fakeProperties["concurrency"], 10, 32)
		assert.Equal(t, settings.Concurrency, int(concurrency))
		assert.Equal(t, settings.Channel, fakeProperties["channel"])
	})
}
