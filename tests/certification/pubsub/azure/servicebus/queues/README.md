# Azure Service Bus Queues Pubsub Certification

The purpose of this module is to provide tests that certify the Azure Service Bus Queues Pubsub as a stable component.

## Important: Queue vs Topic Semantics

Unlike topics (publish-subscribe), queues use **competing consumer** semantics:
- Each message is delivered to **only ONE** consumer
- Multiple subscribers compete for messages (load balancing)
- Messages are NOT broadcast to all subscribers

## Test Plan

### Certification Tests

1. **TestServicebusQueues** - Basic pub/sub functionality
   - Run dapr application with 1 publisher and 2 subscribers
   - Publisher publishes to 1 queue
   - Both subscribers compete for messages (each message goes to only ONE subscriber)
   - Verify that all expected messages were received (distributed among consumers)

2. **TestServicebusQueuesMultipleSubsSameApp** - Multiple subscriptions in same app
   - Run dapr application with 1 publisher and 1 subscriber app with multiple handlers
   - Publisher publishes to 2 queues
   - Verify messages are received on correct queues

3. **TestServicebusQueuesNonexistingQueue** - Auto-creation of queues
   - Run dapr application with 1 publisher and 1 subscriber
   - Verify the creation of queue on first publish
   - Send messages to the queue created
   - Verify that subscriber received all the messages

4. **TestServicebusQueuesNetworkInterruption** - Network resilience
   - Run dapr application with 1 publisher and 1 subscriber
   - Publisher publishes to 1 queue
   - Simulate network interruptions using tc (traffic control)
   - Verify that the component recovers and all messages are received
   - **Note**: Requires root/sudo privileges

5. **TestServicebusQueuesEntityManagement** - Disabled entity management
   - Run dapr application with 1 publisher
   - Publisher tries to publish to a queue that does not exist
   - Verify that the queue does NOT get auto-created
   - Verify that an error is returned

6. **TestServicebusQueuesDefaultTtl** - Message TTL (Time-To-Live)
   - Run dapr application with 1 publisher and 1 subscriber
   - Publisher publishes to 1 queue, wait for TTL to expire
   - Verify the message is deleted/expired before subscriber starts

7. **TestServicebusQueuesAuthentication** - Azure AD authentication
   - Run dapr application with 1 publisher and 1 subscriber
   - Uses Service Principal authentication instead of connection string
   - Publisher publishes to 1 queue
   - Verify that all expected messages were received

8. **TestServicebusQueuesMessageMetadata** - Message metadata handling
   - Verify that custom metadata (partition key) is correctly passed

9. **TestServicebusQueuesMultipleQueues** - Multiple queues
   - Verify publishing to multiple queues simultaneously

10. **TestServicebusQueuesLargeMessages** - Large message payloads
    - Verify handling of larger message payloads (1KB+)

11. **TestServicebusQueuesSequentialPublish** - Sequential batches
    - Verify multiple sequential batch publishes

12. **TestServicebusQueuesReconnection** - Sidecar restart recovery
    - Verify component reconnects after sidecar restart

13. **TestServicebusQueuesEmptyMessages** - Minimal messages
    - Verify handling of minimal/edge-case message payloads

14. **TestServicebusQueuesConcurrentPublishers** - Multiple publishers
    - Verify multiple sidecars publishing to the same queue

## Prerequisites

### Azure Resources
- Azure Service Bus namespace (Standard or Premium tier)
- Queues will be auto-created by tests (unless testing disabled entity management)

### Required Queues
The following queues should exist or will be auto-created:
- `certification-pubsub-queue-active`
- `certification-pubsub-queue-passive`
- `certification-queue-per-test-run`

## Environment Variables

### Required for Basic Tests (Connection String Authentication)

```bash
# Azure Service Bus connection string (from Azure Portal > Service Bus > Shared access policies)
export AzureServiceBusConnectionString="Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<key>"
```

### Required for Authentication Test (Service Principal / Azure AD)

```bash
# Service Bus namespace (full FQDN)
export AzureServiceBusNamespace="<namespace>.servicebus.windows.net"

# Azure AD Tenant ID
export AzureCertificationTenantId="<tenant-id>"

# Service Principal Client ID (App ID)
export AzureCertificationServicePrincipalClientId="<client-id>"

# Service Principal Client Secret
export AzureCertificationServicePrincipalClientSecret="<client-secret>"
```

### Creating a Service Principal for Authentication Test

```bash
# Create Service Principal with Contributor role on the Service Bus namespace
az ad sp create-for-rbac --name "dapr-cert-sp" \
  --role Contributor \
  --scopes /subscriptions/<subscription-id>/resourceGroups/<resource-group>/providers/Microsoft.ServiceBus/namespaces/<namespace> \
  -o json

# IMPORTANT: Also assign the Data Owner role for sending/receiving messages
az role assignment create \
  --assignee "<service-principal-client-id>" \
  --role "Azure Service Bus Data Owner" \
  --scope "/subscriptions/<subscription-id>/resourceGroups/<resource-group>/providers/Microsoft.ServiceBus/namespaces/<namespace>"
```

## Running Tests

### Run All Tests
```bash
# Set environment variables first
export AzureServiceBusConnectionString="..."

cd tests/certification/pubsub/azure/servicebus/queues
go test -v -timeout 30m
```

### Run Specific Test
```bash
go test -v -timeout 5m -run "TestServicebusQueues$"
go test -v -timeout 5m -run "TestServicebusQueuesMultipleSubsSameApp$"
go test -v -timeout 5m -run "TestServicebusQueuesAuthentication$"
```

### Run Network Interruption Test (requires sudo)
```bash
# Clean any residual tc rules first
sudo tc qdisc del dev eth0 root 2>/dev/null || true

# Run the test with sudo
sudo -E go test -v -timeout 5m -run "TestServicebusQueuesNetworkInterruption$"
```

### Run All Tests with All Variables
```bash
export AzureServiceBusConnectionString="Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<key>"
export AzureServiceBusNamespace="<namespace>.servicebus.windows.net"
export AzureCertificationTenantId="<tenant-id>"
export AzureCertificationServicePrincipalClientId="<client-id>"
export AzureCertificationServicePrincipalClientSecret="<client-secret>"

go test -v -timeout 30m
```

## Troubleshooting

### "pubsub messagebus is not found"
- Check that environment variables are set correctly
- Verify the connection string format is correct

### Network Interruption Test Fails with "packet rules already setup"
```bash
# Clean residual tc rules
sudo tc qdisc del dev eth0 root
```

### Authentication Test Times Out
- Ensure the Service Principal has the "Azure Service Bus Data Owner" role
- Wait a few minutes after role assignment for propagation
- The namespace name must be the full FQDN (e.g., `myns.servicebus.windows.net`)

### Tests Hang or Timeout
- Check Azure Service Bus connectivity
- Verify the namespace is accessible from your network
- Check for any firewall rules blocking ports 5671/5672 (AMQP)
