# Azure Service Bus Queue Input/Output Binding Certification
The purpose of this module is to provide tests that certify the Azure Service Bus Queue Input/Output Binding as a stable component.

## Test Plan
### Certification Tests
- Verify Queue level TTL is respected
   - Create a component spec with the field `ttlInSeconds`
   - Run dapr application with component
   - Ensure a queue was created in Azure Service Bus Namespace with TTL set
   - Send a message, wait TTL seconds, and verify the message is deleted
- Verify Message level TTL is respected
   - Create a component spec without the field `ttlInSeconds`
   - Run dapr application with component
   - Ensure a queue was created in Azure Service Bus Namespace with TTL set to default (very large)
   - Send a message with `ttlInSeconds` metadata set, wait TTL seconds, and verify the message is deleted
- Verify Queue/Message TTL interaction
   - Create a component spec with the field `ttlInSeconds`
   - Run dapr application with component
   - Ensure a queue was created in Azure Service Bus Namespace with TTL set
   - Send a message with `ttlInSeconds` and ensure message field is respected over queue field
- Verify create and receive accuracy
   - Create multiple input/output bindings with different queues
   - Run dapr application with components
   - Send/receive messages across queues, ensure target/receiver is always correct
   - Additionally, ensure that the messages are in order
- Verify Network Resiliency
   - During the create and receive accuracy test, simulate a network error before sending any messages
   - After recovery send messages to the queue
   - Ensure that all messages are received
- Verify App Failure Resiliency
   - Start an application that is guaranteed to fail
   - Ensure the binding continues to read incoming messages
   - Ensure the messages that are failed are retried

### Future Tests
1. Provide iterations around the different auth mechanisms supported by Azure Service Bus.
   1. Utilize a connection string (covered).
   1. Utilize a service principal with the appropriate roles granted.
   1. Utilize Managed Identity.