/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package zeebe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"syscall"

	"github.com/camunda-cloud/zeebe/clients/go/pkg/pb"
	"github.com/google/uuid"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/bindings/zeebe/command"
	"github.com/dapr/components-contrib/bindings/zeebe/jobworker"
	"github.com/dapr/kit/logger"
)

const (
	// TestProcessFile contains the basic test process file name.
	TestProcessFile string = "test.bpmn"
	// CalcProcessFile contains the calculation process file name.
	CalcProcessFile string = "calc.bpmn"
)

type EnvVars struct {
	ZeebeVersion                 string
	ZeebeBrokerHost              string
	ZeebeBrokerGatewayPort       string
	ZeebeBrokerClusterSize       string
	ZeebeBrokerReplicationFactor string
	ZeebeBrokerPartitionsCount   string
}

type MetadataPair struct {
	Key, Value string
}

type CalcVariables struct {
	Operator      string  `json:"operator"`
	FirstOperand  float64 `json:"firstOperand"`
	SecondOperand float64 `json:"secondOperand"`
}

type CalcResult struct {
	Result float64 `json:"result"`
}

// GetEnvVars returns the Zeebe environment vars.
func GetEnvVars() EnvVars {
	return EnvVars{
		ZeebeVersion:                 os.Getenv("ZEEBE_VERSION"),
		ZeebeBrokerHost:              os.Getenv("ZEEBE_BROKER_NETWORK_HOST"),
		ZeebeBrokerGatewayPort:       os.Getenv("ZEEBE_BROKER_GATEWAY_NETWORK_PORT"),
		ZeebeBrokerClusterSize:       os.Getenv("ZEEBE_BROKER_CLUSTER_CLUSTERSIZE"),
		ZeebeBrokerReplicationFactor: os.Getenv("ZEEBE_BROKER_CLUSTER_REPLICATIONFACTOR"),
		ZeebeBrokerPartitionsCount:   os.Getenv("ZEEBE_BROKER_CLUSTER_PARTITIONSCOUNT"),
	}
}

// Command initializes the Zeebe command binding and returns it.
func Command() (*command.ZeebeCommand, error) {
	testLogger := logger.NewLogger("test")
	envVars := GetEnvVars()

	cmd := command.NewZeebeCommand(testLogger)
	err := cmd.Init(bindings.Metadata{
		Name: "test",
		Properties: map[string]string{
			"gatewayAddr":            envVars.ZeebeBrokerHost + ":" + envVars.ZeebeBrokerGatewayPort,
			"gatewayKeepAlive":       "45s",
			"usePlainTextConnection": "true",
		},
	})
	if err != nil {
		return nil, err
	}

	return cmd, nil
}

// JobWorker initializes the Zeebe job worker binding and returns it.
func JobWorker(jobType string, additionalMetadata ...MetadataPair) (*jobworker.ZeebeJobWorker, error) {
	testLogger := logger.NewLogger("test")
	envVars := GetEnvVars()
	metadata := bindings.Metadata{
		Name: "test",
		Properties: map[string]string{
			"gatewayAddr":            envVars.ZeebeBrokerHost + ":" + envVars.ZeebeBrokerGatewayPort,
			"gatewayKeepAlive":       "45s",
			"usePlainTextConnection": "true",
			"jobType":                jobType,
		},
	}

	for _, pair := range additionalMetadata {
		metadata.Properties[pair.Key] = pair.Value
	}

	cmd := jobworker.NewZeebeJobWorker(testLogger)
	if err := cmd.Init(metadata); err != nil {
		return nil, err
	}

	return cmd, nil
}

// TestID creates a valid test ID for the isolation of processes during test. This ID is optimized for the usage inside
// a BPMN file because some values inside a BPMN must always start with a character. This function will ensure that
// this is always the case.
func TestID() string {
	return fmt.Sprintf("zeebe-test-%s", uuid.New().String())
}

// ProcessIDModifier modifies the process ID in a BPMN file.
func ProcessIDModifier(id string) func(string) string {
	return func(content string) string {
		return strings.ReplaceAll(content, "id=\"zeebe-test\"", fmt.Sprintf("id=\"%s\"", id))
	}
}

// NameModifier modifies the name of a process.
func NameModifier(name string) func(string) string {
	return func(content string) string {
		return strings.ReplaceAll(content, "name=\"Test\"", fmt.Sprintf("name=\"%s\"", name))
	}
}

// JobTypeModifier modifies the job type of a process.
func JobTypeModifier(from string, to string) func(string) string {
	return func(content string) string {
		return strings.ReplaceAll(content, fmt.Sprintf("type=\"%s\"", from), fmt.Sprintf("type=\"%s\"", to))
	}
}

// RetryModifier modifies the job retries for a specific job type.
func RetryModifier(jobType string, retries int) func(string) string {
	return func(content string) string {
		return strings.ReplaceAll(
			content,
			fmt.Sprintf("type=\"%s\" retries=\"1\"", jobType),
			fmt.Sprintf("type=\"%s\" retries=\"%d\"", jobType, retries))
	}
}

// GetTestFile loads the content of a BPMN process file. The function also accepts a list of
// modifier functions which allows to manipulate the content of the returned BPMN file.
func GetTestFile(fileName string, modifiers ...func(string) string) ([]byte, error) {
	dataBytes, err := ioutil.ReadFile("../processes/" + fileName)
	if err != nil {
		return nil, err
	}

	dataStr := string(dataBytes)
	for _, modifier := range modifiers {
		dataStr = modifier(dataStr)
	}
	dataBytes = []byte(dataStr)

	return dataBytes, nil
}

// DeployProcess deploys a test BPMN file. The function also accepts a list of modifier functions which
// allows to manipulate the content of the returned BPMN file. On success the function returns
// a JSON with the deployment information.
func DeployProcess(
	cmd *command.ZeebeCommand,
	fileName string,
	modifiers ...func(string) string,
) (*pb.ProcessMetadata, error) {
	data, err := GetTestFile(fileName, modifiers...)
	if err != nil {
		return nil, err
	}

	metadata := map[string]string{"fileName": fileName}
	req := &bindings.InvokeRequest{Data: data, Metadata: metadata, Operation: command.DeployProcessOperation}
	res, err := cmd.Invoke(context.TODO(), req)
	if err != nil {
		return nil, err
	}

	deployment := &pb.DeployProcessResponse{}
	err = json.Unmarshal(res.Data, deployment)
	if err != nil {
		return nil, err
	}

	if res.Metadata != nil {
		return nil, fmt.Errorf("response metadata was expected to be nil, got: %v", res.Metadata)
	}

	if deployment.Key == 0 {
		return nil, errors.New("deployment key was expected not to be 0")
	}

	numDeployedProcesses := len(deployment.Processes)
	if numDeployedProcesses != 1 {
		return nil, fmt.Errorf("number of deployed processes was expected to be 1, got: %d", numDeployedProcesses)
	}

	return deployment.Processes[0], nil
}

// CreateProcessInstance creates a process instance and returns the process instance data.
func CreateProcessInstance(
	cmd *command.ZeebeCommand,
	payload map[string]interface{},
) (*pb.CreateProcessInstanceResponse, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req := &bindings.InvokeRequest{Data: data, Operation: command.CreateInstanceOperation}
	res, err := cmd.Invoke(context.TODO(), req)
	if err != nil {
		return nil, err
	}

	processInstance := &pb.CreateProcessInstanceResponse{}
	err = json.Unmarshal(res.Data, processInstance)
	if err != nil {
		return nil, err
	}

	return processInstance, nil
}

func ActicateJob(
	cmd *command.ZeebeCommand,
	payload map[string]interface{},
) (*[]pb.ActivatedJob, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req := &bindings.InvokeRequest{Data: data, Operation: command.ActivateJobsOperation}
	res, err := cmd.Invoke(context.TODO(), req)
	if err != nil {
		return nil, err
	}

	activatedJobs := &[]pb.ActivatedJob{}
	err = json.Unmarshal(res.Data, activatedJobs)
	if err != nil {
		return nil, err
	}

	return activatedJobs, nil
}

// CalcWorker is a simple calculation worker.
func CalcWorker(request *bindings.ReadResponse) ([]byte, error) {
	variables := &CalcVariables{}
	err := json.Unmarshal(request.Data, variables)
	if err != nil {
		return nil, err
	}

	result := CalcResult{}
	switch variables.Operator {
	case "+":
		result.Result = variables.FirstOperand + variables.SecondOperand
	case "-":
		result.Result = variables.FirstOperand - variables.SecondOperand
	case "/":
		result.Result = variables.FirstOperand / variables.SecondOperand
	case "*":
		result.Result = variables.FirstOperand * variables.SecondOperand
	default:
		return nil, fmt.Errorf("unexpected operator: %s", variables.Operator)
	}

	response, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}

	return response, nil
}

// InitTestProcess initializes a test process.
func InitTestProcess(
	cmd *command.ZeebeCommand,
	id string,
	testWorker func(context.Context, *bindings.ReadResponse) ([]byte, error),
	additionalMetadata ...MetadataPair,
) error {
	testJobType := id + "-test"

	_, err := DeployProcess(
		cmd,
		TestProcessFile,
		ProcessIDModifier(id),
		RetryModifier("test", 3),
		JobTypeModifier("test", testJobType))
	if err != nil {
		return err
	}

	ackJob, err := JobWorker(testJobType, additionalMetadata...)
	if err != nil {
		return err
	}

	go ackJob.Read(testWorker)

	return nil
}

// InterruptProcess interrupts a process.
func InterruptProcess() {
	pid := syscall.Getpid()
	proc, _ := os.FindProcess(pid)
	proc.Signal(os.Interrupt)
}
