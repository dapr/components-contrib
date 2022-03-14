# Start the environment

In Visual Studio Code run Remote-Containers: **Open Folder in Container...** from the Command Palette (F1) and select the `tests/e2e/bindings/zeebe` folder.

# Run the tests on the command line

```bash
cd tests/e2e/bindings/zeebe/
docker-compose up -d
docker exec -w /go/src/github.com/dapr/components-contrib -it zeebe-dapr-1 make e2e-tests-zeebe
```

# Run the tests in VS Code

To run the tests you have to add the `e2etests` build flag to your VSCode buildTags settings: "go.buildTags": "e2etests".

# BPMN process files

The test suite uses two BPMN processes which are located in the `processes` folder. These processes can be edited with the [Camunda Modeler](https://camunda.com/de/products/camunda-platform/modeler/)

# Missing tests

## Incident related tests

Currently it's not possible to get an incident key which is needed to resolve an incident.
