# Azure Storage Queue Input/Output Binding Certification
The purpose of this module is to provide tests that certify the Azure Storage Queue Input/Output Binding as a stable component.

## Test Plan
### Certification Tests
- Verify the queue is created/present inside the storage account.
   - Create component spec.
   - Run dapr application with component.
   - Ensure the queue is created/present.
- Verify the storage access key is present and the connection is established to Azure Storage Queue.
   - Create component spec.
   - Run dapr application with component.
   - Ensure that you have access to the queue by using the access key and connection to the queue is established.
   - Ensure that the access or connection fails when the access key is not valid.
- Verify data is getting stored in the storage queue.
   - Create component spec with the data to be stored.
   - Run dapr application with component to store data in the queue as output binding.
   - Read stored data from the queue as input binding.
   - Ensure that read data is same as the data that was stored.
- Verify Data level TTL is regarded.
   - Create component spec with with the field `ttlInSeconds`.
   - Run dapr application with component.
   - Send a message, wait TTL seconds, and verify the message is deleted/expired.
- Verify decodeBase64 attribute is regarded.
   - Create component spec with the field `decodeBase64` set true.
   - Run dapr application with component.
   - Send an encoded message to the queue.
   - Ensure that the decoded message is stored in the queue.
- Verify reconnection to the queue for output binding.
   - Simulate a network error before sending any messages.
   - Run dapr application with the component.
   - After the reconnection, send messages to the queue.
   - Ensure that the messages sent after the reconnection are stored in the queue.
- Verify reconnection to the queue for input binding.
   - Simulate a network error before reading any messages.
   - Run dapr application with the component.
   - After the reconnection, read messages from the queue.
   - Ensure that the messages after the reconnection are read.