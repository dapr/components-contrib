# Azure Event Hubs Bindings certification testing

This project aims to test the Azure Event Hubs bindings component under various conditions.

## Test Plan

- Test sending /receiving data between single partition
   - Start an app with 1 sender and 1 receiver
   - Provide multiple partitions but store data in one partition
   - Receiver should only receive message from one partition
   - Sends 100+ unique messages
   - Simulates periodic errors
   - Confirm that all expected messages were received
   - Confirm that receiver does not receive messages from other than one partition

- Test sending /receiving data multiple partitions/sender and receivers
   - Start an app with 1 sender and 1 receiver
   - Send data from 2 partitions 
   - Sends 100+ unique messages
   - Simulates periodic errors
   - Confirm that all expected messages were received
   - Confirm  messages were received from all partitions

- Test reconnection
   - Start an app with 1 senders and 1 receivers
   - Send 100+ unique messages from 1 sender
   - Interrupt the connection
   - Confirm that all expected messages were received by respective receiver

- IoT hub testing
   - Move existing IoT test from integration to conformance test 
  
- Auth testing
   - Test connection string based authentication mechanism
   - Test service principal based authentication mechanism

### Running the tests

This must be run in the GitHub Actions Workflow configured for test infrastructure setup.