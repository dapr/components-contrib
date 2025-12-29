# Azure Service Bus Queues Certification

This document describes the certification tests for the Azure Service Bus Queues pub/sub component.

## Test Cases

### Basic Functionality
- **TestServicebusQueues**: Tests basic pub/sub functionality with single publisher and multiple subscribers
- **TestServicebusQueuesMultipleSubsSameApp**: Tests multiple subscribers from the same application

### Network Resilience
- **TestServicebusQueuesNetworkInterruption**: Tests recovery from network interruptions

### Entity Management
- **TestServicebusQueuesEntityManagement**: Tests behavior when entity management is disabled
- **TestServicebusQueuesNonexistingQueue**: Tests auto-creation of non-existing queues

### TTL (Time-To-Live)
- **TestServicebusQueuesDefaultTtl**: Tests message expiration with default TTL

## Prerequisites

- Azure Service Bus namespace
- Connection string stored in `AzureServiceBusConnectionString` environment variable

## Running Tests

```bash
go test -v -tags=certtests -count=1 ./tests/certification/pubsub/azure/servicebus/queues/
```
