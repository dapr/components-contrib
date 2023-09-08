/*
Copyright 2023 The Dapr Authors
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

package zeebe_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"runtime"
	"strings"
	"time"

	"github.com/camunda/zeebe/clients/go/v8/pkg/pb"
	"github.com/camunda/zeebe/clients/go/v8/pkg/zbc"
	"github.com/dapr/components-contrib/bindings"
	bindings_zeebe_command "github.com/dapr/components-contrib/bindings/zeebe/command"
	"github.com/dapr/components-contrib/tests/certification/flow"
	dapr_client "github.com/dapr/go-sdk/client"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

const (
	SidecarName           = "zeebeSidecar"
	CommandName           = "zeebe-command"
	JobworkerTestName     = "zeebe-jobworker-test"
	JobworkerCalcExecName = "zeebe-jobworker-calc-exec"
	JobworkerCalcAckName  = "zeebe-jobworker-calc-ack"
	DockerComposeYaml     = "../docker-compose.yml"
	DockerComposeTlsYaml  = "../docker-compose-tls.yml"
	TlsKeyFile            = "/tmp/dapr-cert-test-zeebe-key.pem"
	TlsCertFile           = "/tmp/dapr-cert-test-zeebe-cert.pem"
	// TestProcessFile contains the basic test process file name.
	TestProcessFile string = "test.bpmn"
	// CalcProcessFile contains the calculation process file name.
	CalcProcessFile string = "calc.bpmn"
	TestDmnFile     string = "test.dmn"
)

type EnvVars struct {
	ZeebeVersion                 string
	ZeebeBrokerHost              string
	ZeebeBrokerGatewayPort       string
	ZeebeBrokerClusterSize       string
	ZeebeBrokerReplicationFactor string
	ZeebeBrokerPartitionsCount   string
}

type DeployResourceResponseJson struct {
	Key         int64             `json:"key,omitempty"`
	Deployments []*DeploymentJson `json:"deployments,omitempty"`
}

type DeploymentJson struct {
	Metadata DeploymentMetadataJson `json:"metadata,omitempty"`
}

type DeploymentMetadataJson struct {
	Process              *pb.ProcessMetadata              `json:"process,omitempty"`
	Decision             *pb.DecisionMetadata             `json:"decision,omitempty"`
	DecisionRequirements *pb.DecisionRequirementsMetadata `json:"decisionRequirements,omitempty"`
}

// GetEnvVars returns the Zeebe environment vars.
func GetEnvVars(relativePath string) EnvVars {
	_, filename, _, ok := runtime.Caller(1)
	if !ok {
		log.Fatal("Cannot get path to current file")
	}

	filepath := path.Join(path.Dir(filename), relativePath)

	err := godotenv.Load(filepath)
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	return EnvVars{
		ZeebeVersion:                 os.Getenv("ZEEBE_VERSION"),
		ZeebeBrokerHost:              os.Getenv("ZEEBE_BROKER_NETWORK_HOST"),
		ZeebeBrokerGatewayPort:       os.Getenv("ZEEBE_BROKER_GATEWAY_NETWORK_PORT"),
		ZeebeBrokerClusterSize:       os.Getenv("ZEEBE_BROKER_CLUSTER_CLUSTERSIZE"),
		ZeebeBrokerReplicationFactor: os.Getenv("ZEEBE_BROKER_CLUSTER_REPLICATIONFACTOR"),
		ZeebeBrokerPartitionsCount:   os.Getenv("ZEEBE_BROKER_CLUSTER_PARTITIONSCOUNT"),
	}
}

// ProvideKeyAndCert copies the key and the cert to the temp directory so that it can be referenced in the config files
func ProvideKeyAndCert(relativeCertPath string) func(ctx flow.Context) error {
	_, filename, _, ok := runtime.Caller(1)
	if !ok {
		log.Fatal("Cannot get path to current file")
	}

	certPath := path.Join(path.Dir(filename), relativeCertPath, "cert.pem")
	keyPath := path.Join(path.Dir(filename), relativeCertPath, "key.pem")

	return func(ctx flow.Context) error {
		_, err := copyFile(certPath, TlsCertFile)
		if err != nil {
			return err
		}

		_, err = copyFile(keyPath, TlsKeyFile)
		if err != nil {
			return err
		}

		return nil
	}
}

// TestID creates a valid test ID for the isolation of resources during test. This ID is optimized for the usage inside
// a BPMN file because some values inside a BPMN must always start with a character. This function will ensure that
// this is always the case.
func TestID() string {
	return fmt.Sprintf("zeebe-test-%s", uuid.New().String())
}

// IDModifier modifies an ID of a resource.
func IDModifier(id string) func(string) string {
	return func(content string) string {
		return strings.ReplaceAll(content, "id=\"zeebe-test\"", fmt.Sprintf("id=\"%s\"", id))
	}
}

// NameModifier modifies the name of a resource.
func NameModifier(name string) func(string) string {
	return func(content string) string {
		return strings.ReplaceAll(content, "name=\"Test\"", fmt.Sprintf("name=\"%s\"", name))
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

// GetTestFile loads the content of a test file. The function also accepts a list of
// modifier functions which allows to manipulate the content of the returned file.
func GetTestFile(fileName string, modifiers ...func(string) string) ([]byte, error) {
	dataBytes, err := os.ReadFile("../resources/" + fileName)
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

func GetDaprClient(grpcPort int) dapr_client.Client {
	client, clientErr := dapr_client.NewClientWithPort(fmt.Sprint(grpcPort))
	if clientErr != nil {
		panic(clientErr)
	}

	return client
}

func CheckZeebeConnection(ctx flow.Context) error {
	client, err := zbc.NewClient(&zbc.ClientConfig{
		GatewayAddress:         "localhost:26500",
		UsePlaintextConnection: true,
		KeepAlive:              45 * time.Second,
	})
	if err != nil {
		return err
	}

	err = client.Close()
	if err != nil {
		return err
	}
	return nil
}

func CheckZeebeConnectionTls(relativeCertPath string) func(ctx flow.Context) error {
	_, filename, _, ok := runtime.Caller(1)
	if !ok {
		log.Fatal("Cannot get path to current file")
	}

	filepath := path.Join(path.Dir(filename), relativeCertPath)

	return func(ctx flow.Context) error {
		client, err := zbc.NewClient(&zbc.ClientConfig{
			GatewayAddress:         "localhost:26500",
			UsePlaintextConnection: false,
			KeepAlive:              45 * time.Second,
			CaCertificatePath:      filepath,
		})
		if err != nil {
			return err
		}

		err = client.Close()
		if err != nil {
			return err
		}
		return nil
	}
}

// ExecCommandOperation abstracts the command binding request for the different operations
func ExecCommandOperation(
	context context.Context,
	client dapr_client.Client,
	operation bindings.OperationKind,
	data []byte, metadata map[string]string,
) (out *dapr_client.BindingEvent, err error) {
	req := &dapr_client.InvokeBindingRequest{
		Name:      CommandName,
		Operation: string(operation),
		Data:      data,
		Metadata:  metadata,
	}

	out, err = client.InvokeBinding(context, req)
	if err != nil {
		return nil, fmt.Errorf("%w", err)
	}
	return out, nil
}

// DeployResource deploys a test resource. The function also accepts a list of modifier functions which
// allows to manipulate the content of the deployed resource. On success the function returns
// a JSON with the deployment information.
func DeployResource(
	client dapr_client.Client,
	context context.Context,
	fileName string,
	expectedDeployments int,
	modifiers ...func(string) string,
) (*DeployResourceResponseJson, error) {
	data, err := GetTestFile(fileName, modifiers...)
	if err != nil {
		return nil, err
	}

	metadata := map[string]string{"fileName": fileName}
	res, err := ExecCommandOperation(context, client, bindings_zeebe_command.DeployResourceOperation, data, metadata)
	if err != nil {
		return nil, err
	}

	deployment := &DeployResourceResponseJson{}
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

	numDeployedResources := len(deployment.Deployments)
	if numDeployedResources != expectedDeployments {
		return nil, fmt.Errorf("number of deployed resources was expected to be %d, got: %d", expectedDeployments, numDeployedResources)
	}

	return deployment, nil
}

// CreateProcessInstance creates a process instance and returns the process instance data.
func CreateProcessInstance(
	client dapr_client.Client,
	context context.Context,
	payload map[string]interface{},
) (*pb.CreateProcessInstanceWithResultResponse, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	res, err := ExecCommandOperation(context, client, bindings_zeebe_command.CreateInstanceOperation, data, nil)
	if err != nil {
		return nil, err
	}

	processInstance := &pb.CreateProcessInstanceWithResultResponse{}
	err = json.Unmarshal(res.Data, processInstance)
	if err != nil {
		return nil, err
	}

	return processInstance, nil
}

func ActicateJob(
	client dapr_client.Client,
	context context.Context,
	payload map[string]interface{},
) (*[]pb.ActivatedJob, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	res, err := ExecCommandOperation(context, client, bindings_zeebe_command.ActivateJobsOperation, data, nil)
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

func copyFile(in, out string) (int64, error) {
	i, e := os.Open(in)
	if e != nil {
		return 0, e
	}
	defer i.Close()
	o, e := os.Create(out)
	if e != nil {
		return 0, e
	}
	defer o.Close()
	return o.ReadFrom(i)
}
