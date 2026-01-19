# Azure Service Bus Queues Certification Tests

This directory contains certification tests for the Azure Service Bus Queues pubsub component, including tests for session-based FIFO message ordering.

## Prerequisites

Before running the tests, ensure you have:

1. An Azure subscription with an Azure Service Bus namespace
2. A connection string with `Manage` permissions (RootManageSharedAccessKey or equivalent)
3. Go 1.21 or later installed
4. The Dapr CLI installed (for embedded sidecar tests)

## Environment Setup

Set the following environment variable with your Azure Service Bus connection string:

```bash
export AzureServicebusConnectionString="Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<key>"
```

You can retrieve the connection string using Azure CLI:

```bash
az servicebus namespace authorization-rule keys list \
  --resource-group <your-resource-group> \
  --namespace-name <your-namespace> \
  --name RootManageSharedAccessKey \
  --query primaryConnectionString -o tsv
```

## Running the Tests

### Run All Queue Tests

```bash
cd /path/to/components-contrib
AzureServicebusConnectionString="<connection-string>" \
  go test -v -timeout 10m ./tests/certification/pubsub/azure/servicebus/queues/...
```

### Run Session-Specific Tests Only

```bash
# FIFO ordering test with dynamic sessions
AzureServicebusConnectionString="<connection-string>" \
  go test -v -timeout 5m \
  -run "TestServicebusQueuesWithSessionsFIFO" \
  ./tests/certification/pubsub/azure/servicebus/queues/...

# Partitioned sessions test (explicit session IDs)
AzureServicebusConnectionString="<connection-string>" \
  go test -v -timeout 5m \
  -run "TestServicebusQueuesPartitionedSessions" \
  ./tests/certification/pubsub/azure/servicebus/queues/...

# Default behavior test (without sessions)
AzureServicebusConnectionString="<connection-string>" \
  go test -v -timeout 5m \
  -run "TestServicebusQueuesDefaultBehavior" \
  ./tests/certification/pubsub/azure/servicebus/queues/...

# Optional session parameters test
AzureServicebusConnectionString="<connection-string>" \
  go test -v -timeout 5m \
  -run "TestServicebusQueuesOptionalSessionParameters" \
  ./tests/certification/pubsub/azure/servicebus/queues/...
```

### Run with Verbose Debug Output

```bash
AzureServicebusConnectionString="<connection-string>" \
  go test -v -timeout 10m \
  ./tests/certification/pubsub/azure/servicebus/queues/... 2>&1 | tee test-output.log
```

## Test Descriptions

### TestServicebusQueuesWithSessionsFIFO

Verifies that messages published with session IDs are delivered in FIFO (First-In-First-Out) order within each session. The test:

1. Configures a subscription with `requireSessions=true` and `maxConcurrentSessions=1`
2. Publishes 10 messages to session "session2"
3. Publishes 10 messages to session "session1"
4. Verifies that messages within each session are received in the exact order they were published
5. Confirms FIFO ordering is maintained independently for each session

### TestServicebusQueuesDefaultBehavior

Verifies that the component works correctly without session configuration (backward compatibility). The test:

1. Uses a component configuration without any session-related metadata
2. Publishes messages without session IDs
3. Verifies all messages are received (order not guaranteed without sessions)

### TestServicebusQueuesOptionalSessionParameters

Verifies that session parameters are optional and the component initializes correctly with various configurations:

1. Tests with `requireSessions=true` but without explicit timeout or concurrent session values
2. Verifies default values are applied correctly

### TestServicebusQueuesPartitionedSessions

Verifies the partitioned session mode with explicit session IDs. The test:

1. Configures a component with `sessionIds: "partition-A, partition-B, partition-C"`
2. Publishes 5 messages to each partition (15 total)
3. Verifies FIFO ordering is maintained within each partition
4. Validates that messages are distributed across partitions as expected

This is the recommended mode for production workloads that need both ordering and scalability.

## Component Configurations

The tests use three component configurations located in the `components/` directory:

### components/default/service_bus.yaml

Standard configuration without session support:

```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: messagebus
spec:
  type: pubsub.azure.servicebus.queues
  version: v1
  metadata:
    - name: connectionString
      secretKeyRef:
        name: AzureServiceBusConnectionString
        key: AzureServiceBusConnectionString
```

### components/sessions/service_bus.yaml

Configuration with session support enabled:

```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: messagebus
spec:
  type: pubsub.azure.servicebus.queues
  version: v1
  metadata:
    - name: connectionString
      secretKeyRef:
        name: AzureServiceBusConnectionString
        key: AzureServiceBusConnectionString
    - name: requireSessions
      value: "true"
    - name: sessionIdleTimeoutInSec
      value: "60"
    - name: maxConcurrentSessions
      value: "8"
```

### components/partitioned/service_bus.yaml

Configuration with partitioned sessions (explicit session IDs):

```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: messagebus
spec:
  type: pubsub.azure.servicebus.queues
  version: v1
  metadata:
    - name: connectionString
      secretKeyRef:
        name: AzureServiceBusConnectionString
        key: AzureServiceBusConnectionString
    # Partitioned session mode: explicit session IDs for controlled scaling
    - name: sessionIds
      value: "partition-A, partition-B, partition-C"
    - name: sessionIdleTimeoutInSec
      value: "30"
```

## Troubleshooting

### Queue Session Mismatch Error

If you see an error like:

```
queue already exists but session requirement doesn't match
```

This means the queue was previously created with a different session configuration. Azure Service Bus does not allow changing the session requirement of an existing queue. Delete the queue and run the test again:

```bash
az servicebus queue delete \
  --resource-group <your-resource-group> \
  --namespace-name <your-namespace> \
  --name <queue-name>
```

### Connection Timeout

If tests timeout waiting for messages, ensure:

1. The connection string is correct and has Manage permissions
2. Your network allows outbound connections to Azure Service Bus (port 5671/5672 for AMQP)
3. There are no firewall rules blocking access

### Cleaning Up Test Queues

To list and delete queues created during testing:

```bash
# List all queues
az servicebus queue list \
  --resource-group <your-resource-group> \
  --namespace-name <your-namespace> \
  --query "[].name" -o tsv

# Delete a specific queue
az servicebus queue delete \
  --resource-group <your-resource-group> \
  --namespace-name <your-namespace> \
  --name <queue-name>
```

## Session Metadata Reference

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `sessionIds` | string | - | **(Recommended)** Comma-separated list of explicit session IDs for partitioned mode |
| `requireSessions` | bool | `false` | Enable dynamic session mode (auto-enabled when `sessionIds` is set) |
| `sessionIdleTimeoutInSec` | int | `60` | Seconds to wait for messages before closing session |
| `maxConcurrentSessions` | int | `8` | Max concurrent sessions in dynamic mode (ignored when `sessionIds` is set) |

### Partitioned Sessions (Recommended)

When using `sessionIds`, publishers MUST include a valid SessionId from the list:

```go
// Partitions: "partition-A, partition-B, partition-C"
client.PublishEvent(ctx, "pubsub", "queue-name", data, 
    dapr.PublishEventWithMetadata(map[string]string{
        "SessionId": "partition-A",  // Must be in sessionIds list
    }))
```

### Dynamic Sessions

When using `requireSessions=true` without `sessionIds`, any SessionId is valid:

```go
client.PublishEvent(ctx, "pubsub", "queue-name", data, 
    dapr.PublishEventWithMetadata(map[string]string{
        "SessionId": "order-123",  // Any value
    }))
```
