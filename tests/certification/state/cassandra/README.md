
## Test for TTL 
1. TTL not expiring
2. TTL not a valid number
3. TTL Expires as expected
   - Provide a TTL of 5 second
   - Fetch this record just after saving 
   - Sleep for 5 seconds 
   - Try to fetch again after a gap of 5 seconds, record shouldn't be deleted

## Connection Recovery

1.  When Cassandra goes down and then comes back up the client is able to reconnect

## Test Metadata Fields
1. Verify `port` attribute is used
   - set port to non default value
   - run dapr application with component
   - component should successfully initialize
 
2. Verify `keyspace` attribute is used
   - set keyspace to non-default value
   - run dapr application with component
   - component should successfully initialize and create keyspace

3. Verify `table` attribute is used
   - set table to non-default value
   - run dapr application with component
   - component should successfully initialize and create table
   - successfully run query on table

4. Verify `protoVersion` attribute is used
   - set protoVersion to non-default value 0 
   - run dapr application with component
   - cassandra client itself should detect version from cluster if protoVersion == 0
   - component should successfully initialize
   - run queries to verify
   
5. Verify `protoVersion` attribute is used -negative test
   - set protoVersion to non-default value 1 
   - run dapr application with component
   - component should recieve errors on queries

6. Verify `replicationFactor` attribute is used
   - set replicationFactor to non-default value 2 
   - run dapr application with component using 2 nodes
   - component should successfully initialize
   - run queries to verify

7. Verify `replicationFactor` attribute is used - negative test
   - set replicationFactor to non-default value 2 
   - run dapr application with component using 1 node
   - component should recieve errors on queries

8. Verify `consistency` attribute is used - negative test
   - set consistency to non-default value "Three"
   - run dapr application with component 
   - component should successfully initialize
   - run queries and see failure due to less than 3 nodes available

9. Verify `consistency` attribute is used 
   - set consistency to non-default value "Two" 
   - run dapr application with component 
   - component should successfully initialize
   - run queries successfully