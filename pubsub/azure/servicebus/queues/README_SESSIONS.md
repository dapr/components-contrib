# Azure Service Bus Queues - Session Support

This document describes the session support feature for the Azure Service Bus Queues pubsub component, which enables FIFO (First-In-First-Out) message ordering.

## Overview

Azure Service Bus sessions provide guaranteed FIFO message processing within a session. When sessions are enabled, messages with the same session ID are delivered to a single consumer in the exact order they were published.

## Session Modes

This component supports two session modes:

### Mode 1: Dynamic Sessions (Default)

Use `requireSessions: true` to let Azure Service Bus dynamically assign sessions to subscribers.

- Sessions are distributed automatically across subscribers
- Unpredictable which subscriber gets which session
- Good for workloads where you don't care about session assignment

### Mode 2: Partitioned Sessions (Recommended for Scaling)

Use `sessionIds: "partition-1, partition-2, partition-3"` for explicit, controlled partitioning.

- **You define exactly which session IDs exist**
- **Publishers MUST use one of these session IDs**
- **Each subscriber connects to ALL defined sessions**
- **Scale up to N replicas (where N = number of session IDs)**
- **Predictable, controlled load distribution**

## Partitioned Session Mode: Scalable FIFO

This is the recommended mode for production workloads that need both **ordering** and **scalability**.

### How It Works

```
                    YAML Configuration
                    sessionIds: "A, B, C"
                           │
           ┌───────────────┼───────────────┐
           ▼               ▼               ▼
      Session A       Session B       Session C
           │               │               │
           ▼               ▼               ▼
    ┌──────────┐    ┌──────────┐    ┌──────────┐
    │  Pod 1   │    │  Pod 2   │    │  Pod 3   │
    │ (owns A) │    │ (owns B) │    │ (owns C) │
    └──────────┘    └──────────┘    └──────────┘
```

### Configuration

```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: order-queue
spec:
  type: pubsub.azure.servicebus.queues
  version: v1
  metadata:
    - name: connectionString
      value: "Endpoint=sb://..."
    - name: sessionIds
      value: "order-region-us, order-region-eu, order-region-asia"
```

### Publishing

Publishers **MUST** include a valid `SessionId` from the configured list:

```go
// Go SDK - choose the appropriate partition
client.PublishEvent(ctx, "order-queue", "orders", orderData,
    dapr.PublishEventWithMetadata(map[string]string{
        "SessionId": "order-region-us",  // Must be in sessionIds list
    }))
```

```bash
# HTTP API
curl -X POST http://localhost:3500/v1.0/publish/order-queue/orders \
  -H "Content-Type: application/json" \
  -d '{"orderId": "123"}' \
  -H "metadata.SessionId: order-region-us"
```

**Validation**: If you publish with a `SessionId` not in the configured list, the publish will fail with an error.

### Scaling

| Session IDs Configured | Max Replicas | Behavior |
|------------------------|--------------|----------|
| 1                      | 1            | Global FIFO (no scaling) |
| 3                      | 3            | 3-way partition, each replica handles 1 session |
| 10                     | 10           | 10-way partition |

**Rule**: `replicas ≤ len(sessionIds)`

If you have more replicas than session IDs, the extra replicas will compete for the same sessions (which is fine, Azure handles the locking).

### Partitioning Strategy

Choose session IDs based on your domain:

| Use Case | Session IDs Example |
|----------|---------------------|
| By region | `us-east, us-west, eu-central, asia-pacific` |
| By customer tier | `premium, standard, free` |
| By order type | `express, standard, economy` |
| By hash bucket | `bucket-0, bucket-1, bucket-2, bucket-3` |

### Example: Hash-based Partitioning

```go
// Distribute orders across partitions using consistent hashing
sessionIDs := []string{"partition-0", "partition-1", "partition-2", "partition-3"}
partitionIndex := hash(order.CustomerID) % len(sessionIDs)
sessionID := sessionIDs[partitionIndex]

client.PublishEvent(ctx, "order-queue", "orders", orderData,
    dapr.PublishEventWithMetadata(map[string]string{
        "SessionId": sessionID,
    }))
```

## Critical: One-to-One Session Model

**Sessions operate on a strict 1:1 model between a session and its receiver.**

This means:

- Each session can only be processed by ONE receiver at a time
- If you have multiple application instances (horizontal scaling), each instance will receive DIFFERENT sessions
- You cannot have multiple subscribers processing the same session concurrently
- The session is locked to a single receiver until it releases the lock or times out

### Scaling Implications

| Scenario | Behavior |
|----------|----------|
| 1 app instance, 1 session | The instance processes all messages for that session in order |
| 1 app instance, N sessions | The instance processes up to `maxConcurrentSessions` sessions in parallel |
| M app instances, 1 session | Only ONE instance receives the session; others wait or get different sessions |
| M app instances, N sessions | Sessions are distributed across instances; each session goes to exactly one instance |

If you need multiple consumers to process the same logical stream of messages, sessions are NOT the right solution. Consider using:
- Standard queues without sessions (no ordering guarantee)
- Partitioning your workload by session ID across multiple queues
- A different architecture that does not require competing consumers on ordered messages

## What Session Support Does

1. **Guarantees FIFO ordering within a session**: Messages published with the same `SessionId` are delivered in the exact order they were sent. This is enforced by Azure Service Bus at the infrastructure level.

2. **Provides exclusive session locking**: Only one receiver can process messages from a given session at a time. The session is locked to that receiver until:
   - The receiver explicitly releases it
   - The session idle timeout expires
   - The receiver disconnects

3. **Enables concurrent processing across different sessions**: Multiple sessions can be processed in parallel (controlled by `maxConcurrentSessions`), but each individual session is always processed by exactly one receiver.

4. **Distributes sessions across scaled instances**: When running multiple application instances, Azure Service Bus automatically distributes different sessions to different instances. This provides horizontal scaling while maintaining per-session ordering.

5. **Automatically creates session-enabled queues**: When the component detects that a message has a `SessionId` in its metadata, it ensures the queue is created with session support.

## What Session Support Does NOT Do

1. **Does NOT allow multiple subscribers for the same session**: A session is exclusively locked to one receiver. If you have 3 app instances and 1 session, only 1 instance will process it. The other 2 will either wait for other sessions or remain idle.

2. **Does NOT guarantee ordering across different sessions**: Messages with different session IDs may be delivered in any order relative to each other. FIFO is only guaranteed within a single session.

3. **Does NOT provide global message ordering**: If you need ALL messages in a queue processed in strict order, you must use a single session ID for all messages AND run only one receiver.

4. **Does NOT allow changing session configuration on existing queues**: Azure Service Bus does not permit changing a queue from session-enabled to non-session-enabled (or vice versa) after creation. You must delete and recreate the queue.

5. **Does NOT support session state persistence**: This implementation does not use Azure Service Bus session state features. Session state management must be handled by the application if needed.

6. **Does NOT guarantee exactly-once delivery**: Azure Service Bus provides at-least-once delivery. Applications should implement idempotency if exactly-once processing is required.

7. **Does NOT work without session IDs**: Messages sent to a session-enabled queue MUST have a `SessionId`. Messages without a session ID will be rejected.

8. **Does NOT support competing consumers on a session**: Unlike standard queues where multiple consumers can compete for messages, session-enabled queues enforce exclusive access per session.

## When to Use Sessions

Sessions are appropriate when:

- You need strict ordering for a subset of messages (e.g., all events for a specific entity)
- Each "stream" of ordered messages can be processed independently
- You can partition your workload by a logical identifier (customer ID, order ID, aggregate ID)
- You accept that each partition (session) will be processed by exactly one consumer

Sessions are NOT appropriate when:

- You need multiple consumers to process the same stream of messages
- You need global ordering across all messages in the queue
- Your workload cannot be partitioned into independent streams
- You require high throughput on a single logical stream with multiple workers

## Configuration

### Component Metadata

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `connectionString` | string | Yes* | - | Azure Service Bus connection string |
| `namespaceName` | string | Yes* | - | Azure Service Bus namespace (alternative to connectionString) |
| `sessionIds` | string | No | - | **Recommended**: Comma-separated list of session IDs for partitioned mode |
| `requireSessions` | bool | No | `false` | Enable dynamic session mode (auto-enabled when `sessionIds` is set) |
| `sessionIdleTimeoutInSec` | int | No | `60` | Seconds to wait for messages before releasing session lock |
| `maxConcurrentSessions` | int | No | `8` | Max concurrent sessions in dynamic mode (ignored when `sessionIds` is set) |

*Either `connectionString` or `namespaceName` (with Azure AD auth) is required.

### Example: Partitioned Sessions (Recommended)

```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: order-queue
spec:
  type: pubsub.azure.servicebus.queues
  version: v1
  metadata:
    - name: connectionString
      value: "Endpoint=sb://mynamespace.servicebus.windows.net/;..."
    - name: sessionIds
      value: "region-us, region-eu, region-asia"
    - name: sessionIdleTimeoutInSec
      value: "120"
```

With this configuration:
- Publishers must include `SessionId: region-us`, `region-eu`, or `region-asia`
- You can scale up to 3 replicas
- Each session guarantees FIFO ordering

### Example: Dynamic Sessions

```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: order-queue
spec:
  type: pubsub.azure.servicebus.queues
  version: v1
  metadata:
    - name: connectionString
      value: "Endpoint=sb://mynamespace.servicebus.windows.net/;..."
    - name: requireSessions
      value: "true"
    - name: maxConcurrentSessions
      value: "4"
```

With this configuration:
- Publishers can use any SessionId
- Azure distributes sessions dynamically to subscribers
- Less predictable but more flexible

## Usage

### Publishing Messages with Session ID

Include the `SessionId` metadata when publishing:

```go
// Go SDK
client.PublishEvent(ctx, "order-queue", "orders", orderData,
    dapr.PublishEventWithMetadata(map[string]string{
        "SessionId": "customer-12345",
    }))
```

```bash
# HTTP API
curl -X POST http://localhost:3500/v1.0/publish/order-queue/orders \
  -H "Content-Type: application/json" \
  -d '{"orderId": "123"}' \
  -H "metadata.SessionId: customer-12345"
```

### Subscribing

No special handling required in the subscriber:

```go
func orderHandler(ctx context.Context, e *common.TopicEvent) (bool, error) {
    // Messages with the same SessionId arrive in FIFO order
    log.Printf("Received order: %s", e.Data)
    return false, nil
}
```

## Architecture Examples

### Example 1: Single Instance Processing Multiple Sessions

```
Publisher                    Queue                    Consumer (1 instance)
    |                          |                            |
    |--[SessionId=A, msg1]---->|                            |
    |--[SessionId=B, msg1]---->|                            |
    |--[SessionId=A, msg2]---->|                            |
    |--[SessionId=B, msg2]---->|       accepts A,B          |
    |                          |--------------------------->|
                                                            |
                               Processes A: msg1, msg2 (in order)
                               Processes B: msg1, msg2 (in order)
                               (A and B may interleave)
```

### Example 2: Multiple Instances with Session Distribution

```
Publisher                    Queue                    Consumers (2 instances)
    |                          |                            
    |--[SessionId=A, msg1]---->|                    Instance 1
    |--[SessionId=B, msg1]---->|      accepts A     |
    |--[SessionId=C, msg1]---->|------------------->| processes A
    |--[SessionId=A, msg2]---->|                    |
    |--[SessionId=B, msg2]---->|      accepts B,C   Instance 2
    |--[SessionId=C, msg2]---->|------------------->| processes B, C
    
    Session A: ALL messages go to Instance 1
    Session B: ALL messages go to Instance 2
    Session C: ALL messages go to Instance 2
```

### Example 3: What Does NOT Work

```
INVALID: Multiple consumers on same session

Publisher                    Queue                    Consumers
    |                          |                            
    |--[SessionId=A, msg1]---->|                    Instance 1
    |--[SessionId=A, msg2]---->|      accepts A     |
    |--[SessionId=A, msg3]---->|------------------->| gets ALL messages
    |                          |                    |
                                                    Instance 2
                                      BLOCKED       |
                                      (waiting)     | gets NOTHING for A
```

## Session Receiver Behavior

1. The component starts up to `maxConcurrentSessions` session receiver goroutines
2. Each receiver calls `AcceptNextSessionForQueue()` to get the next available session
3. Once a session is accepted, the receiver processes messages from that session ONLY
4. If no messages arrive within `sessionIdleTimeoutInSec`, the session is released
5. The receiver then accepts another available session
6. Session locks are automatically renewed while processing

## Limitations

1. **Immutable session configuration**: Once a queue is created, its session setting cannot be changed

2. **SessionId is mandatory**: Publishing without `SessionId` to a session-enabled queue fails

3. **No competing consumers per session**: Only one receiver processes a session at a time

4. **Connection overhead**: Each session receiver maintains a connection to Azure Service Bus

5. **Message size limits**: Standard tier: 256 KB; Premium tier: 100 MB

## Known Issues and Caveats

### Issue 1: Publisher/Subscriber Configuration Mismatch

The component uses two different mechanisms to determine session behavior:

- **Publisher side**: Detects `SessionId` in message metadata and creates a session-enabled queue
- **Subscriber side**: Uses explicit `requireSessions` configuration

**Risk**: If a publisher sends a message with `SessionId` (creating a session-enabled queue) but the subscriber has `requireSessions=false`, the subscriber will fail because it attempts to use a non-session receiver on a session-enabled queue.

**Recommendation**: Always explicitly configure `requireSessions=true` in both publisher and subscriber configurations when using sessions:

```yaml
# Component configuration - use the same for both publisher and subscriber
metadata:
  - name: requireSessions
    value: "true"
```

### Issue 2: Implicit Queue Creation Behavior

When `Publish` is called with a `SessionId` but the queue doesn't exist, the component creates a session-enabled queue. This is a convenience feature but can lead to unexpected behavior:

1. If the queue is later accessed without sessions, it will fail
2. There's no way to "undo" session configuration on an existing queue
3. The queue may be created with sessions unintentionally

**Recommendation**: Pre-create queues with the desired configuration in production environments using Azure Portal, ARM templates, or Bicep.

### Issue 3: Missing SessionId Validation

When publishing to a session-enabled queue, a `SessionId` is **required** by Azure Service Bus. The component does not validate this before sending, so errors will only appear at Azure Service Bus level.

**Recommendation**: Always include `SessionId` in metadata when publishing to session-enabled queues. The component will not prevent you from omitting it, but Azure will reject the message.

### Issue 4: Race Condition on Queue Creation

If multiple publishers start simultaneously and some include `SessionId` while others don't, there's a race condition on queue creation:

- If non-session publish wins: Queue created without sessions, session publishes fail
- If session publish wins: Queue created with sessions, non-session publishes fail

**Recommendation**: Ensure all publishers agree on session configuration before starting.

## Testing

- Unit tests: `pubsub/azure/servicebus/queues/servicebus_test.go`
- Certification tests: `tests/certification/pubsub/azure/servicebus/queues/`

## References

- [Azure Service Bus Sessions](https://docs.microsoft.com/en-us/azure/service-bus-messaging/message-sessions)
- [Dapr Pub/Sub](https://docs.dapr.io/developing-applications/building-blocks/pubsub/)
- [Azure Service Bus Queues Component](https://docs.dapr.io/reference/components-reference/supported-pubsub/setup-azure-servicebus-queues/)
