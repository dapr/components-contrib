package kubemq

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
)

type kubemqMetadata struct {
	Address           string `mapstructure:"address"`
	internalHost      string `mapstructure:"-"`
	internalPort      int    `mapstructure:"-"`
	ClientID          string `mapstructure:"clientID"`
	AuthToken         string `mapstructure:"authToken"`
	Group             string `mapstructure:"group"`
	IsStore           bool   `mapstructure:"store"`
	DisableReDelivery bool   `mapstructure:"disableReDelivery"`
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
func createMetadata(pubSubMetadata pubsub.Metadata) (*kubemqMetadata, error) {
	result := &kubemqMetadata{
		IsStore: true,
	}

	err := metadata.DecodeMetadata(pubSubMetadata.Properties, result)
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
		return nil, fmt.Errorf("invalid kubeMQ address, address is empty")
	}
	return result, nil
}
