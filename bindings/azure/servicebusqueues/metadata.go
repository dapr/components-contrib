package servicebusqueues

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"

	"github.com/dapr/components-contrib/bindings"
	contrib_metadata "github.com/dapr/components-contrib/metadata"
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
}

const (
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
)

func (a *AzureServiceBusQueues) parseMetadata(metadata bindings.Metadata) (*serviceBusQueuesMetadata, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	var m serviceBusQueuesMetadata
	err = json.Unmarshal(b, &m)
	if err != nil {
		return nil, err
	}

	if m.ConnectionString != "" && m.NamespaceName != "" {
		return nil, errors.New("connectionString and namespaceName are mutually exclusive")
	}

	ttl, ok, err := contrib_metadata.TryGetTTL(metadata.Properties)
	if err != nil {
		return nil, err
	}
	if !ok {
		// set the same default message time to live as suggested in Azure Portal to 14 days (otherwise it will be 10675199 days)
		ttl = defaultMessageTimeToLive
	}
	m.ttl = ttl

	// Queue names are case-insensitive and are forced to lowercase. This mimics the Azure portal's behavior.
	m.QueueName = strings.ToLower(m.QueueName)

	if m.TimeoutInSec < 1 {
		m.TimeoutInSec = defaultTimeoutInSec
	}

	if m.MinConnectionRecoveryInSec < 1 {
		m.MinConnectionRecoveryInSec = defaultMinConnectionRecoveryInSec
	}

	if m.MaxConnectionRecoveryInSec < 1 {
		m.MaxConnectionRecoveryInSec = defaultMaxConnectionRecoveryInSec
	}

	if m.MinConnectionRecoveryInSec > m.MaxConnectionRecoveryInSec {
		return nil, errors.New("maxConnectionRecoveryInSec must be greater than minConnectionRecoveryInSec")
	}

	if m.MaxActiveMessages < 1 {
		m.MaxActiveMessages = defaultMaxActiveMessages
	}

	if m.MaxConcurrentHandlers < 1 {
		m.MaxConcurrentHandlers = defaultMaxConcurrentHandlers
	}

	if m.MaxConcurrentHandlers > m.MaxActiveMessages {
		return nil, errors.New("maxConcurrentHandlers cannot be bigger than maxActiveMessages")
	}

	if m.LockRenewalInSec < 1 {
		m.LockRenewalInSec = defaultLockRenewalInSec
	}

	if m.MaxRetriableErrorsPerSec == nil {
		m.MaxRetriableErrorsPerSec = to.Ptr(defaultMaxRetriableErrorsPerSec)
	}
	if *m.MaxRetriableErrorsPerSec < 0 {
		return nil, errors.New("maxRetriableErrorsPerSec must be non-negative")
	}

	return &m, nil
}
