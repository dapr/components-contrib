package kubemq

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dapr/components-contrib/pubsub"
)

type metadata struct {
	host              string
	port              int
	clientID          string
	authToken         string
	group             string
	isStore           bool
	disableReDelivery bool
}

func parseAddress(address string) (string, int, error) {
	var host string
	var port int
	var err error
	hostPort := strings.Split(address, ":")
	if len(hostPort) != 2 {
		return "", 0, fmt.Errorf("invalid kubeMQ address, address format is invalid")
	}
	host = hostPort[0]
	if len(host) == 0 {
		return "", 0, fmt.Errorf("invalid kubeMQ address, host is empty")
	}
	port, err = strconv.Atoi(hostPort[1])
	if err != nil {
		return "", 0, fmt.Errorf("invalid kubeMQ address, port is invalid")
	}
	return host, port, nil
}

// createMetadata creates a new instance from the pubsub metadata
func createMetadata(pubSubMetadata pubsub.Metadata) (*metadata, error) {
	result := &metadata{}
	if val, found := pubSubMetadata.Properties["address"]; found && val != "" {
		var err error
		result.host, result.port, err = parseAddress(val)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("invalid kubeMQ address, address is empty")
	}
	if val, found := pubSubMetadata.Properties["clientID"]; found && val != "" {
		result.clientID = val
	}

	if val, found := pubSubMetadata.Properties["authToken"]; found && val != "" {
		result.authToken = val
	}

	if val, found := pubSubMetadata.Properties["group"]; found && val != "" {
		result.group = val
	}
	result.isStore = true
	if val, found := pubSubMetadata.Properties["store"]; found && val != "" {
		switch val {
		case "false":
			result.isStore = false
		case "true":
			result.isStore = true
		default:
			return nil, fmt.Errorf("invalid kubeMQ store value, store can be true or false")
		}
	}
	if val, found := pubSubMetadata.Properties["disableReDelivery"]; found && val != "" {
		if val == "true" {
			result.disableReDelivery = true
		}
	}
	return result, nil
}
