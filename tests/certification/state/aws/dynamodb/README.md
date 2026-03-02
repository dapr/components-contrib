# AWS DynamoDB certification testing

This project aims to test the AWS DynamoDB State Store component under various conditions.

## Test plan

### Basic Test
1. Able to create and test connection.
2. Able to do set, fetch and delete.

### TTL Test using master key authentication
1. Able to create and test connection.
2. Able to do set TTL, fetch (expired and non-expired) data and delete.

### Run tests locally

1. Run `docker run -p 8000:8000 amazon/dynamodb-local -jar DynamoDBLocal.jar -sharedDb -inMemory`
2. Create the table with
```
AWS_ACCESS_KEY_ID=fake AWS_SECRET_ACCESS_KEY=fake AWS_DEFAULT_REGION=us-east-1 aws dynamodb create-table \
  --table-name dapr-state-1 \
  --attribute-definitions AttributeName=key,AttributeType=S \
  --key-schema AttributeName=key,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --endpoint-url http://localhost:8000 \
  --region us-east-1   
```

and 

```
AWS_ACCESS_KEY_ID=fake AWS_SECRET_ACCESS_KEY=fake AWS_DEFAULT_REGION=us-east-1 aws dynamodb update-time-to-live \
  --table-name dapr-state-1 \
  --time-to-live-specification "Enabled=true,AttributeName=ttlExpireTime" \
  --endpoint-url http://localhost:8000
```
3. Replace the state store with:
```
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: statestore-basic
spec:
  type: state.aws.dynamodb
  version: v1
  metadata:
    - name: table
      value: "dapr-state-1"
    - name: region
      value: "us-east-1"
    - name: endpoint
      value: "http://localhost:8000"
    - name: accessKey
      value: "fakeMyKeyId"
    - name: secretKey
      value: "fakeSecretAccessKey"
    - name: partitionKey
      value: "key"
```
