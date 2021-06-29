package nsq

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseNSQMetadata(t *testing.T) {
	t.Run("metadata is correct", func(t *testing.T) {
		fakeProperties := map[string]string{
			"nsq_lookups":   "10.42.0.10:4160,10.42.0.20:4160",
			"nsq_address":   "10.42.0.10:4150,10.42.0.20:4150",
			"read_timeout":  "30s",
			"write_timeout": "30s",
			"max_in_flight": "200",
		}
		// paser metadata
		settings := Settings{}
		err := settings.Decode(fakeProperties)

		// assert
		assert.NoError(t, err)
		assert.NotEmpty(t, settings.NsqLookups)
		lookups := strings.Join(settings.lookups, ",")
		assert.Equal(t, fakeProperties["nsq_lookups"], lookups)

		assert.NotEmpty(t, settings.NsqAddress)
		nsqds := strings.Join(settings.nsqds, ",")
		assert.Equal(t, fakeProperties["nsq_address"], nsqds)

		// nsqconfig value
		readtTimeOut, _ := time.ParseDuration(fakeProperties["read_timeout"])
		assert.Equal(t, readtTimeOut, settings.ReadTimeout)
		writeTimeOut, _ := time.ParseDuration(fakeProperties["write_timeout"])
		assert.Equal(t, writeTimeOut, settings.WriteTimeout)
		maxInFlight, _ := strconv.ParseInt(fakeProperties["max_in_flight"], 10, 32)
		assert.Equal(t, int(maxInFlight), settings.MaxInFlight)
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
