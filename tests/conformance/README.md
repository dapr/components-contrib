# Conformance Tests
// TODO update readme for conformance tests after workflow is created.

## Tests structure
1. `tests/` directory contains the configuration and the test definition for conformance tests.
2. All the conformance tests are within the `tests/conformance` directory.
3. All the configurations are in the `tests/config` directory.
4. Each of the component specific `component` definition are in their specific `component type` folder in the `tests/config` folder. For eg: `redis` statestore component definition within `state` directory. And the other component types are `state`, `secretstores`, `pubsub` and `bindings`.
5. Similar to the component definitions, each component type has its own set of the conformance tests definitions. 
6. Each component type contains a `tests.yml` definition that defines the component to be tested along with component specific test configuration. 
7. All the tests configurations are defined in `common.go` file. 
8. Each component type has its own `_test` file to trigger the conformance tests. 

## Running conformance tests
1. Test test setup is independent of the test run. 
2. Run Redis with 6379 exposed locally.
3. Run `make conf-tests` to run the conformance tests locally.
> Note: Any component that cannot run as dockerized containers are not tested locally. 