# CockroachDB State Store Certification

The purpose of this module is to provide tests that certify the CockroachDB State Store as a stable component.

## Test plan

## Connection Test
* Verify the connection is established to CockroachDB.
    * Create component spec.
    * Run the component with docker compose
    * Run dapr application with component.
    * Ensure that you have access to the queue and connection to the queue is established.

## Basic Operations Test
* Verify that cockroachDB can save, retrieve, and delete states
    * Save basic state information with a specified key
    * Perform a Get Operation with that same key and ensure the the data previously written is retrieved
    * Delete the state item
    * Perform another Get Operation and verify that the data retreived is a nil byte array


## ETAG Test
* Verify cockroachDB Etag behavior
    * Save state without an etag and ensure default etag (value of 1) is created
    * Save state with a new, previously unsused etag and ensure that no item is updated
    * Get state with etag of 1 and verify that our first written data is retrieved
    * Overwrite the initial data through a Set operation with an etag value of 1
    * Run a Get Operation and verify that the previous Set operation was successful

## Transactions Test
* Verify that cockroachDB can handle multiple opperations in a single request
    * Save basic state information with a specified key
    * Perform a Get Operation with that same key and ensure the the data previously written is retrieved
    * Run a suite of set and delete requests
    * Run a Get Operation on the data that should've been deleted from the Delete operation in the multi command and verify that the data no longer exists
    * Retrieve data written from set requests and verify the data was correctly written

## Restart Test
* Verify that cockroachDB can have state data persist after a restart
    * Stop cockroachDB by calling a dockercompose stop command
    * Start cockroachDB by calling a dockercompose start command
    * Perform a Get Operation using a previously inserted key and verify that the data previously saved before the restart is returned

## TTLs and cleanups

1. Correctly parse the `cleanupIntervalInSeconds` metadata property:
   - No value uses the default value (3600 seconds)
   - A positive value sets the interval to the given number of seconds
   - A zero or negative value disables the cleanup
2. The cleanup method deletes expired records and updates the metadata table with the last time it ran
3. The cleanup method doesn't run if the last iteration was less than `cleanupIntervalInSeconds` or if another process is doing the cleanup
