# Conformance Tests

## Tests structure

1. `tests/` directory contains the configuration and the test definition for conformance tests.
2. All the conformance tests are within the `tests/conformance` directory.
3. All the configurations are in the `tests/config` directory.
4. Each of the component specific `component` definition are in their specific `component type` folder in the `tests/config` folder. E.g. `redis` statestore component definition within `state` directory. The component types are `bindings`, `state`, `secretstores`, `pubsub`. Cloud specific components will be within their own `cloud` directory within the `component type` folder, e.g. `pubsub/azure/servicebus`.
5. Similar to the component definitions, each component type has its own set of the conformance tests definitions.
6. Each `component type` contains a `tests.yml` definition that defines the component to be tested along with component specific test configuration. Nested folder names have their `/` in path replaced by `.` in the component name in `tests.yml`, e.g. `azure/servicebus` should be `azure.servicebus`
7. All the tests configurations are defined in `common.go` file.
8. Each `component type` has its own `_test` file to trigger the conformance tests. E.g. `bindings_test.go`.
9. Each test added will also need to be added to the `conformance.yml` workflow file.

## Conformance test workflow

1. All components/services tested in the automated tests are currently manually setup by the Dapr team and run in an environment maintained by the Dapr team. As a contributor, follow the next two steps for integrating a new component with conformance tests.
2. If the component is tested locally within a docker container requiring no secrets, then add the component to the `pr-components` step in conformance test workflow `.github/conformance.yml`. `pr-components` step generates the component matrix for which the conformance tests will be run on each PR.
3. If the component is tested against a service and requires secrets, then add the component to the `cron-components` step in conformance test workflow `.github/conformance.yml`. `cron-components` defines the components for which the conformance test will be run against the master code in a scheduled manner.

## Integrating a new component with conformance tests

1. Add the component specific YAML to `tests/config/<COMPONENT-TYPE>/<COMPONENT>/<FILE>.yaml`.
2. All passwords will be of the form `${{PASSWORD_KEY}}` so that it is injected via environment variables.
3. Register the component `New**` function in `common.go`. For example:

    ```go
    ...
        switch tc.Component {
        case "azure.servicebusqueues":
            binding = b_azure_servicebusqueues.NewAzureServiceBusQueues(testLogger)
        case "azure.storagequeues":
            binding = b_azure_storagequeues.NewAzureStorageQueues(testLogger)
        case "azure.eventgrid":
            binding = b_azure_eventgrid.NewAzureEventGrid(testLogger)
        case "kafka":
            binding = b_kafka.NewKafka(testLogger)
        case "new-component":
            binding = b_new_component.NewComponent(testLogger)
        default:
            return nil
        }
    ...
    ```

4. Add the config to `tests.yml` defined inside `tests/config/<COMPONENT-TYPE>/` folder. For example:

    ```yaml
    componentType: binding
    components:
    ## All other components
    - component: <COMPONENT>
        allOperations: <true/false>
        operations: <List of operations if needed>
    ```

5. Any UUID generation for keys can be specified using `$((uuid))`. E.g. see [/tests/config/bindings/tests.yml](../config/bindings/tests.yml)
6. Run the specific conformance test following the process below.
7. Follow steps 2 and 3 for changes to the [workflow](#conformance-test-workflow).

## Running conformance tests

1. Test setup is independent of the test run.
2. Run the service that needs to conformance tested locally or in your own cloud account.

   - For cloud-agnostic components such as Kafka, MQTT etc., there are `docker-compose` definitions under the [/.github/infrastructure](../../.github/infrastructure/) folder you can use to quickly create an instance of the service. For example, to setup Kafka for conformance tests:

        ```bash
        docker-compose -f ./.github/infrastructure/docker-compose-kafka.yml -p kafka up -d
        ```

   - For Azure components such as Blob Storage, Key Vault etc., there is an automation script that can help you create the resources under your subscription, and extract the environment variables needed to run the conformance tests. See [/.github/infrastructure/conformance/azure/README.md](../../.github/infrastructure/conformance/azure/README.md) for more details.
   - Some components require additional set up or teardown scripts, which are placed in [/.github/scripts/components-scripts/](../../.github/scripts/components-scripts/)

3. Some conformance tests require credentials in the form of environment variables. For examples Azure Cosmos DB conformance tests will need to have Azure Cosmos DB credentials. You will need to supply them to make these tests pass.
4. To run specific tests, run:

    ```bash
    # TEST_NAME can be TestPubsubConformance, TestStateConformance, TestSecretStoreConformance or TestBindingsConformance
    # COMPONENT_NAME is the component name from the tests.yml file, e.g. azure.servicebus, redis, mongodb etc.
    go test -v -tags=conftests -count=1 ./tests/conformance -run="${TEST_NAME}/${COMPONENT_NAME}"
    ```

### Debug conformance tests

To run all conformance tests

```bash
 dlv test --build-flags '-v -tags=conftests' ./tests/conformance
```

To run a specific conformance test

```bash
 dlv test --build-flags '-v -tags=conftests' ./tests/conformance -- -test.run "TestStateConformance/redis"
 ```
 
If you want to combine VS Code & dlv for debugging so you can set breakpoints in the IDE, create a debug launch configuration as follows:

```json
{
    "version": "0.2.0",
    "configurations": [
    {
        "name": "Launch test function",
        "type": "go",
        "request": "launch",
        "mode": "test",
        "program": "${workspaceFolder}/tests/conformance",
        "buildFlags": "-v -tags=conftests",
        "env": {
            "SOMETHING_REQUIRED_BY_THE_TEST": "<somevalue>"
        },
        "args": [
            "-test.run",
            "TestStateConformance/redis",
        ]
    },
    ]
}
```

## Using Terraform for conformance tests

If you are writing new conformance tests and they require cloud resources, you should use the Terraform framework we have in place. To enable your component test to use terraform there are a few changes in the normal steps you must do.

1. Create a setup and teardown script in [/.github/scripts/components-scripts/](../../.github/scripts/components-scripts/) for your component. You should also define new env variables. You will need a variable for each specific resource your tests will use. If you require 3 different topics and 2 different tables for your tests you should have 5 different env variables set. The only convention you must follow for the variables is the value must use `$UNIQUE_ID` to ensure there are no conflicts with the resource names.

    ```bash
    echo "PUBSUB_AWS_SNSSQS_QUEUE=testQueue-${UNIQUE_ID}" >> $GITHUB_ENV
    ```

    Take a look at the AWS DynamoDB [setup](../../.github/scripts/components-scripts/conformance-state.aws.dynamodb-setup.sh) and [teardown](../../.github/scripts/components-scripts/conformance-state.aws.dynamodb-destroy.sh) scripts as example.

2. When updating the `tests.yml` defined inside `tests/config/<COMPONENT-TYPE>/` folder you should overwrite the default names of any resources the conformance tests use. These values should reference env variables which should be defined in the conformance.yml.

    ```yaml
      - component: aws.snssqs.terraform
        operations: ["publish", "subscribe", "multiplehandlers"]
        config:
        pubsubName: aws-snssqs
        testTopicName: ${{PUBSUB_AWS_SNSSQS_TOPIC}}
        testMultiTopic1Name: ${{PUBSUB_AWS_SNSSQS_TOPIC_MULTI_1}}
        testMultiTopic2Name: ${{PUBSUB_AWS_SNSSQS_TOPIC_MULTI_2}}
    ```

3. When writing your `component.yml` you should reference your credentials using env variables and any resources specified in the yaml should use env variables as well just as you did in the `test.yml`. Also if your component has an option that controls resource creation such as `disableEntityManagement` you will need to set it so it prohibits new resource creation. We want to use only terraform to provision resources and not dapr itself for these tests.

    ```yaml
      metadata:
        - name: accessKey
          value: ${{AWS_ACCESS_KEY_ID}}
        - name: secretKey
          value: ${{AWS_SECRET_ACCESS_KEY}}
        - name: region
          value: "us-east-1"
        - name: consumerID
          value: ${{PUBSUB_AWS_SNSSQS_QUEUE}}
        - name: disableEntityManagement
          value: "true"
    ```


4. You will need to create a new terrafrorm file `component.tf` to provision your resources. The file should be placed in its own folder in the `.github/infrastructure/terraform/conformance` directory such as 
`.github/infrastructure/terraform/conformance/pubsub/aws/snsqsq`. The terraform file should use a `UNIQUE_ID` variable and use this variables when naming its resources so they matched the names defined earlier. Make sure any resources your tests will use are defined in terraform.

    ```
    variable "UNIQUE_ID" {
        type        = string
        description = "Unique Id of the github worklow run."
    }
    ```

5. Register your test in the file [/.github/scripts/test-info.mjs](../../.github/scripts/test-info.mjs) file, making sure to set `requiresTerraform: true`.

## Adding new AWS component in GitHub Actions

1. For tests involving aws components we use a service account to provision the resources needed. If you are contributing a brand new component you will need to make sure our account has sufficient permissions to provision resources and use handle component. A Dapr STC member will have to update the service account so contact them for assistance.

2. In your component yaml for your tests you should set the component metadata properties `accesskey` and `secretkey` to the values of `${{AWS_ACCESS_KEY_ID}}` and `${{AWS_SECRET_ACCESS_KEY}}`. These env values will contain the credentials for the testing service account.

    ```yaml
      metadata:
        - name: accessKey
          value: ${{AWS_ACCESS_KEY_ID}}
        - name: secretKey
          value: ${{AWS_SECRET_ACCESS_KEY}}
    ```
