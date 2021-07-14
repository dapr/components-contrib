# Start the environment

In Visual Studio Code run Remote-Containers: **Open Folder in Container...** from the Command Palette (F1) and select the `tests/e2e/bindings/zeebe` folder.

# Run the tests

```bash
cd tests/e2e/bindings/zeebe/
docker-compose up -d
docker exec -w /go/src/github.com/dapr/components-contrib -it zeebe_dapr_1 make e2e-tests-zeebe
```

# BPMN process files

The test suite uses two BPMN processes which are located in the `processes` folder. These processes can be edited with the [Camunda Modeler](https://camunda.com/de/products/camunda-platform/modeler/)

# Missing tests

## Job related tests

There is currently an issue which prevents the activate-jobs command to return the activated jobs: https://github.com/camunda-cloud/zeebe/issues/5925
This command is a prerequisite for the other commands because it returns the jobKey which the other commands need to operate an a job.

Without the working command it is not possible to write tests for he following commands:
- activate-jobs
- complete-job
- fail-job
- update-job-retries
- throw-error

If this issue is fixed, then the tests can be written.

## Incident related tests

Currently it's not possible to get an incident key which is needed to resolve an incident.
