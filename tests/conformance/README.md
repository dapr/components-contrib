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

   - For cloud-agnostic components such as Kafka, MQTT etc., there are `docker-compose` definitions under the [/.github/infrastructure](https://github.com/dapr/components-contrib/tree/master/.github/infrastructure) folder you can use to quickly create an instance of the service. For example, to setup Kafka for conformance tests:

        ```bash
        docker-compose -f ./.github/infrastructure/docker-compose-kafka.yml -p kafka up -d
        ```

   - For Azure components such as Blob Storage, Key Vault etc., there is an automation script that can help you create the resources under your subscription, and extract the environment variables needed to run the conformance tests. See [/.github/infrastructure/conformance/azure/README.md](../../.github/infrastructure/conformance/azure/README.md) for more details.

    > Given the variability in components and how they need to be set up for the conformance tests, you may need to refer to the [GitHub workflow for conformance tests](../../.github/workflows/conformance.yml) for any extra setup required by some components. E.g. Azure Event Grid bindings require setting up an Ngrok instance or similar endpoint for the test.

3. Some conformance tests require credentials in the form of environment variables. For examples Azure CosmosDB conformance tests will need to have Azure CosmosDB credentials. You will need to supply them to make these tests pass.
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