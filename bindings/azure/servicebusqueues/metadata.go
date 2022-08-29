package servicebusqueues

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/internal/utils"
	contribMetadata "github.com/dapr/components-contrib/metadata"
)

type serviceBusQueuesMetadata struct {
	ConnectionString           string `json:"connectionString"`
	NamespaceName              string `json:"namespaceName,omitempty"`
	QueueName                  string `json:"queueName"`
	TimeoutInSec               int    `json:"timeoutInSec"`
	MaxConnectionRecoveryInSec int    `json:"maxConnectionRecoveryInSec"`
	MinConnectionRecoveryInSec int    `json:"minConnectionRecoveryInSec"`
	MaxRetriableErrorsPerSec   *int   `json:"maxRetriableErrorsPerSec"`
	MaxActiveMessages          int    `json:"maxActiveMessages"`
	LockRenewalInSec           int    `json:"lockRenewalInSec"`
	MaxConcurrentHandlers      int    `json:"maxConcurrentHandlers"`
	ttl                        time.Duration
	DisableEntityManagement    bool `json:"disableEntityManagement"`
}

const (
	// Keys
	connectionString           = "connectionString"
	namespaceName              = "namespaceName"
	queueName                  = "queueName"
	timeoutInSec               = "timeoutInSec"
	maxConnectionRecoveryInSec = "maxConnectionRecoveryInSec"
	minConnectionRecoveryInSec = "minConnectionRecoveryInSec"
	maxRetriableErrorsPerSec   = "maxRetriableErrorsPerSec"
	maxActiveMessages          = "maxActiveMessages"
	lockRenewalInSec           = "lockRenewalInSec"
	maxConcurrentHandlers      = "maxConcurrentHandlers"
	disableEntityManagement    = "disableEntityManagement"

	// Default time to live for queues, which is 14 days. The same way Azure Portal does.
	defaultMessageTimeToLive = time.Hour * 24 * 14

	// Default timeout in seconds
	defaultTimeoutInSec = 60

	// Default minimum and maximum recovery time while trying to reconnect
	defaultMinConnectionRecoveryInSec = 2
	defaultMaxConnectionRecoveryInSec = 300

	// Default lock renewal interval, in seconds
	defaultLockRenewalInSec = 20

	// Default number of max active messages
	// Max active messages should be >= max concurrent handlers
	defaultMaxActiveMessages = 1

	// Default number of max concurrent handlers
	// For backwards-compatibility reasons, this only handles one message at a time
	defaultMaxConcurrentHandlers = 1

	// Default rate of retriable errors per second
	defaultMaxRetriableErrorsPerSec = 10

	// By default entity management is enabled
	defaultDisableEntityManagement = false

	errorMessagePrefix = "azure service bus error:"
)

func (a *AzureServiceBusQueues) parseMetadata(metadata bindings.Metadata) (*serviceBusQueuesMetadata, error) {
	m := serviceBusQueuesMetadata{}

	if val, ok := metadata.Properties[connectionString]; ok && val != "" {
		m.ConnectionString = val
	}

	if val, ok := metadata.Properties[namespaceName]; ok && val != "" {
		m.NamespaceName = val
	}

	if m.ConnectionString != "" && m.NamespaceName != "" {
		return nil, errors.New("connectionString and namespaceName are mutually exclusive")
	}

	ttl, ok, err := contribMetadata.TryGetTTL(metadata.Properties)
	if err != nil {
		return nil, err
	}
	if !ok {
		// set the same default message time to live as suggested in Azure Portal to 14 days (otherwise it will be 10675199 days)
		ttl = defaultMessageTimeToLive
	}
	m.ttl = ttl

	if val, ok := metadata.Properties[queueName]; ok && val != "" {
		// Queue names are case-insensitive and are forced to lowercase. This mimics the Azure portal's behavior.
		m.QueueName = strings.ToLower(val)
	}

	/* Optional configuration settings - defaults will be set by the client. */
	m.TimeoutInSec = defaultTimeoutInSec
	if val, ok := metadata.Properties[timeoutInSec]; ok && val != "" {
		m.TimeoutInSec, err = strconv.Atoi(val)
		if err != nil {
			return &m, fmt.Errorf("%s invalid timeoutInSec %s, %s", errorMessagePrefix, val, err)
		}
	}

	m.MinConnectionRecoveryInSec = defaultMinConnectionRecoveryInSec
	if val, ok := metadata.Properties[minConnectionRecoveryInSec]; ok && val != "" {
		m.MinConnectionRecoveryInSec, err = strconv.Atoi(val)
		if err != nil {
			return &m, fmt.Errorf("%s invalid minConnectionRecoveryInSec %s, %s", errorMessagePrefix, val, err)
		}
	}

	m.MaxConnectionRecoveryInSec = defaultMaxConnectionRecoveryInSec
	if val, ok := metadata.Properties[maxConnectionRecoveryInSec]; ok && val != "" {
		m.MaxConnectionRecoveryInSec, err = strconv.Atoi(val)
		if err != nil {
			return &m, fmt.Errorf("%s invalid maxConnectionRecoveryInSec %s, %s", errorMessagePrefix, val, err)
		}
	}

	m.MaxActiveMessages = defaultMaxActiveMessages
	if val, ok := metadata.Properties[maxActiveMessages]; ok && val != "" {
		m.MaxActiveMessages, err = strconv.Atoi(val)
		if err != nil {
			return &m, fmt.Errorf("%s invalid maxActiveMessages %s, %s", errorMessagePrefix, val, err)
		}
	}

	m.MaxConcurrentHandlers = defaultMaxConcurrentHandlers
	if val, ok := metadata.Properties[maxConcurrentHandlers]; ok && val != "" {
		m.MaxConcurrentHandlers, err = strconv.Atoi(val)
		if err != nil {
			return &m, fmt.Errorf("%s invalid maxConcurrentHandlers %s, %s", errorMessagePrefix, val, err)
		}
	}

	if m.MaxConcurrentHandlers > m.MaxActiveMessages {
		return nil, fmt.Errorf("%s maxConcurrentHandlers cannot be bigger than maxActiveMessages, %s", errorMessagePrefix, err)
	}

	m.LockRenewalInSec = defaultLockRenewalInSec
	if val, ok := metadata.Properties[lockRenewalInSec]; ok && val != "" {
		m.LockRenewalInSec, err = strconv.Atoi(val)
		if err != nil {
			return &m, fmt.Errorf("%s invalid lockRenewalInSec %s, %s", errorMessagePrefix, val, err)
		}
	}

	m.MaxRetriableErrorsPerSec = to.Ptr(defaultMaxRetriableErrorsPerSec)
	if val, ok := metadata.Properties[maxRetriableErrorsPerSec]; ok && val != "" {
		mRetriableErrorsPerSec, err := strconv.Atoi(val)
		if err != nil {
			return &m, fmt.Errorf("%s invalid lockRenewalInSec %s, %s", errorMessagePrefix, val, err)
		}
		if mRetriableErrorsPerSec < 0 {
			return nil, fmt.Errorf("%smaxRetriableErrorsPerSec must be non-negative, %s", errorMessagePrefix, err)
		}
		m.MaxRetriableErrorsPerSec = to.Ptr(mRetriableErrorsPerSec)
	}

	m.DisableEntityManagement = defaultDisableEntityManagement
	if val, ok := metadata.Properties[disableEntityManagement]; ok && val != "" {
		m.DisableEntityManagement = utils.IsTruthy(val)
	}

	return &m, nil
}
