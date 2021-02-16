# Conformance Tests
## Tests structure
1. `tests/` directory contains the configuration and the test definition for conformance tests.
2. All the conformance tests are within the `tests/conformance` directory.
3. All the configurations are in the `tests/config` directory.
4. Each of the component specific `component` definition are in their specific `component type` folder in the `tests/config` folder. For eg: `redis` statestore component definition within `state` directory. The component types are `bindings`, `state`, `secretstores`, `pubsub`. Cloud specific components will be within their own `cloud` directory within the `component type` folder eg: `pubsub/azure/servicebus`.
5. Similar to the component definitions, each component type has its own set of the conformance tests definitions.
6. Each `component type` contains a `tests.yml` definition that defines the component to be tested along with component specific test configuration. Nested folder names have their `/` in path replaced by `.` in the component name in `tests.yml` eg: `azure/servicebus` should be `azure.servicebus`
7. All the tests configurations are defined in `common.go` file.
8. Each `component type` has its own `_test` file to trigger the conformance tests. Eg: `bindings_test.go`.
9. Each test added will also need to be added to the `conformance.yml` workflow file.

## Conformance test workflow
1. All components/services needed are currently manually setup. 
2. If the component is tested locally within a docker container requiring no secrets, then add the component to the `pr-components` step in conformance test workflow `.github/conformance.yml`. `pr-components` step generates the component matrix for which the conformance tests will be run on each PR. 
3. If the component is tested against a service and requires secrets, then add the component to the `cron-components` step in conformance test workflow `.github/conformance.yml`. `cron-components` defines the components for which the conformance test will be run against the master code in a scehduled manner.

## Integrating a new component with conformance tests 
1. Add the component specific YAML to `tests/config/<COMPONENT-TYPE>/<COMPONENT>/<FILE>.yaml`.
2. All passwords will be of the form `${{PASSWORD_KEY}}` so that it is injected via environment variables.
3. Register the component `New**` function in `common.go`. Eg: 
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
4. Add the config to `tests.yml` defined inside `tests/config/<COMPONENT-TYPE>/` folder. Eg: 
```yaml
componentType: binding
components:
   ## All other components
  - component: <COMPONENT>
    allOperations: <true/false>
    operations: <List of operations if needed>
```
5. Any UUID generation for keys can be specified using `$((uuid))`. Eg: See `../config/bindings/tests.yml`
6. Run the specific conformance test following the process below.

## Running conformance tests
1. Test setup is independent of the test run.
2. Run the service that needs to conformance tested locally or in your own cloud account.
3. Some conformance tests require credentials in the form of environment variables. For examples Azure CosmosDB conformance tests will need to have Azure CosmosDB credentials. You will need to supply them to make these tests pass.
5. To run specific tests, run:
```bash
# TEST_NAME can be TestPubsubConformance, TestStateConformance, TestSecretStoreConformance or TestBindingsConformance
# COMPONENT_NAME is the component name from the tests.yml file eg: azure.servicebus, redis, mongodb etc.
go test -v -tags=conftests -count=1 ./tests/conformance -run="${TEST_NAME}/${COMPONENT_NAME}"
