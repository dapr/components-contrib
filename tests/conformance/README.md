# Conformance Tests
// TODO update readme for conformance tests after workflow is created.

## Tests structure
1. `tests/` directory contains the configuration and the test definition for conformance tests.
2. All the conformance tests are within the `tests/conformance` directory.
3. All the configurations are in the `tests/config` directory.
4. Each of the component specific `component` definition are in their specific `component type` folder in the `tests/config` folder. For eg: `redis` statestore component definition within `state` directory. And the other component types are `state`, `secretstores`, `pubsub`. Cloud specific components will be within their own `cloud` directory within the `component type` folder eg: `pubsub/azure/servicebus`.
5. Similar to the component definitions, each component type has its own set of the conformance tests definitions.
6. Each `component type` contains a `tests.yml` definition that defines the component to be tested along with component specific test configuration. Nested folder names have their `/` in path replaced by `.` in the component name in `tests.yml` eg: `azure/servicebus` should be `azure.servicebus`
7. All the tests configurations are defined in `common.go` file.
8. Each `component type` has its own `_test` file to trigger the conformance tests.
9. Each test added will also need to be added to the `conformance.yml` workflow file. 

## Running conformance tests
1. Test test setup is independent of the test run.
2. Run Redis with 6379 exposed locally.
3. Run Mongodb locally.
4. Run all conformance tests with `make conf-tests`.
> Note Some conformance tests require credentials in the form of environment variables. For examples Azure CosmosDB conformance tests will need to have Azure CosmosDB credentials. You will need to supply them to make these tests pass.
5. To run specific tests, run:
```bash
# TEST_NAME can be TestPubsubConformance, TestStateConformance, TestSecretStoreConformance or TestOutputBindingConformance
# COMPONENT_TYPE is the type of the component can be pubsub, state, output-binding, secretstores
# COMPONENT_NAME is the component name from the tests.yml file eg: azure.servicebus, redis, mongodb etc.
go test -v -tags=conftests -count=1 ./tests/conformance -run="${TEST_NAME}/${COMPONENT_TYPE}/${COMPONENT_NAME}"
```