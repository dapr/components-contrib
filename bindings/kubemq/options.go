package kubemq

import (
	"errors"
	"strconv"
	"strings"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/metadata"
)

type options struct {
	Address            string `mapstructure:"address"`
	Channel            string `mapstructure:"channel"`
	AuthToken          string `mapstructure:"authToken"`
	AutoAcknowledged   bool   `mapstructure:"autoAcknowledged"`
	PollMaxItems       int    `mapstructure:"pollMaxItems"`
	PollTimeoutSeconds int    `mapstructure:"pollTimeoutSeconds"`

	internalHost string `mapstructure:"-"`
	internalPort int    `mapstructure:"-"`
}

func parseAddress(address string) (string, int, error) {
	var host string
	var port int
	var err error
	hostPort := strings.Split(address, ":")
	if len(hostPort) != 2 {
		return "", 0, errors.New("invalid kubemq address, address format is invalid")
	}
	host = hostPort[0]
	if len(host) == 0 {
		return "", 0, errors.New("invalid kubemq address, host is empty")
	}
	port, err = strconv.Atoi(hostPort[1])
	if err != nil {
		return "", 0, errors.New("invalid kubemq address, port is invalid")
	}
	return host, port, nil
}

// createOptions creates a new instance from the kubemq options
func createOptions(md bindings.Metadata) (*options, error) {
	result := &options{
		internalHost:       "",
		internalPort:       0,
		Channel:            "",
		AuthToken:          "",
		AutoAcknowledged:   false,
		PollMaxItems:       1,
		PollTimeoutSeconds: 3600,
	}

	err := metadata.DecodeMetadata(md.Properties, result)
	if err != nil {
		return nil, err
	}

	if result.Address != "" {
		var err error
		result.internalHost, result.internalPort, err = parseAddress(result.Address)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errors.New("invalid kubemq address, address is empty")
	}

	if result.Channel == "" {
		return nil, errors.New("invalid kubemq channel, channel is empty")
	}

	if result.PollMaxItems < 1 {
		return nil, errors.New("invalid kubemq pollMaxItems value, value must be greater than 0")
	}

	if result.PollTimeoutSeconds < 1 {
		return nil, errors.New("invalid kubemq pollTimeoutSeconds value, value must be greater than 0")
	}

	return result, nil
}

func parsePolicyDelaySeconds(md map[string]string) int {
	if md == nil {
		return 0
	}
	if val, found := md["delaySeconds"]; found && val != "" {
		delaySeconds, err := strconv.Atoi(val)
		if err != nil {
			return 0
		}
		if delaySeconds < 0 {
			return 0
		}
		return delaySeconds
	}
	return 0
}

func parsePolicyExpirationSeconds(md map[string]string) int {
	if md == nil {
		return 0
	}
	if val, found := md["expirationSeconds"]; found && val != "" {
		expirationSeconds, err := strconv.Atoi(val)
		if err != nil {
			return 0
		}
		if expirationSeconds < 0 {
			return 0
		}
		return expirationSeconds
	}
	return 0
}

func parseSetPolicyMaxReceiveCount(md map[string]string) int {
	if md == nil {
		return 0
	}
	if val, found := md["maxReceiveCount"]; found && val != "" {
		maxReceiveCount, err := strconv.Atoi(val)
		if err != nil {
			return 0
		}
		if maxReceiveCount < 0 {
			return 0
		}
		return maxReceiveCount
	}
	return 0
}

func parsePolicyMaxReceiveQueue(md map[string]string) string {
	if md == nil {
		return ""
	}
	if val, found := md["maxReceiveQueue"]; found && val != "" {
		return val
	}
	return ""
}
