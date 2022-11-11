package kubemq

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dapr/components-contrib/bindings"
)

type options struct {
	host               string
	port               int
	channel            string
	authToken          string
	autoAcknowledged   bool
	pollMaxItems       int
	pollTimeoutSeconds int
}

func parseAddress(address string) (string, int, error) {
	var host string
	var port int
	var err error
	hostPort := strings.Split(address, ":")
	if len(hostPort) != 2 {
		return "", 0, fmt.Errorf("invalid kubemq address, address format is invalid")
	}
	host = hostPort[0]
	if len(host) == 0 {
		return "", 0, fmt.Errorf("invalid kubemq address, host is empty")
	}
	port, err = strconv.Atoi(hostPort[1])
	if err != nil {
		return "", 0, fmt.Errorf("invalid kubemq address, port is invalid")
	}
	return host, port, nil
}

// createOptions creates a new instance from the kubemq options
func createOptions(md bindings.Metadata) (*options, error) {
	result := &options{
		host:               "",
		port:               0,
		channel:            "",
		authToken:          "",
		autoAcknowledged:   false,
		pollMaxItems:       1,
		pollTimeoutSeconds: 3600,
	}
	if val, found := md.Properties["address"]; found && val != "" {
		var err error
		result.host, result.port, err = parseAddress(val)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("invalid kubemq address, address is empty")
	}
	if val, ok := md.Properties["channel"]; ok && val != "" {
		result.channel = val
	} else {
		return nil, fmt.Errorf("invalid kubemq channel, channel is empty")
	}

	if val, found := md.Properties["authToken"]; found && val != "" {
		if found && val != "" {
			result.authToken = val
		}
	}

	if val, found := md.Properties["autoAcknowledged"]; found && val != "" {
		autoAcknowledged, err := strconv.ParseBool(val)
		if err != nil {
			return nil, fmt.Errorf("invalid kubemq autoAcknowledged value, %s", err.Error())
		}
		result.autoAcknowledged = autoAcknowledged
	}
	if val, found := md.Properties["pollMaxItems"]; found && val != "" {
		pollMaxItems, err := strconv.Atoi(val)
		if err != nil {
			return nil, fmt.Errorf("invalid kubemq pollMaxItems value, %s", err.Error())
		}
		if pollMaxItems < 1 {
			return nil, fmt.Errorf("invalid kubemq pollMaxItems value, value must be greater than 0")
		}
		result.pollMaxItems = pollMaxItems
	}
	if val, found := md.Properties["pollTimeoutSeconds"]; found && val != "" {
		timeoutSecond, err := strconv.Atoi(val)
		if err != nil {
			return nil, fmt.Errorf("invalid kubemq pollTimeoutSeconds value, %s", err.Error())
		} else {
			if timeoutSecond < 1 {
				return nil, fmt.Errorf("invalid kubemq pollTimeoutSeconds value, value must be greater than 0")
			}
			result.pollTimeoutSeconds = timeoutSecond
		}
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
