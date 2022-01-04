// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package sns

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/service/sns"

	aws_auth "github.com/dapr/components-contrib/authentication/aws"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

// AWSSNS is an AWS SNS binding.
type AWSSNS struct {
	client   *sns.SNS
	topicARN string

	logger logger.Logger
}

type snsMetadata struct {
	TopicArn     string `json:"topicArn"`
	Region       string `json:"region"`
	Endpoint     string `json:"endpoint"`
	AccessKey    string `json:"accessKey"`
	SecretKey    string `json:"secretKey"`
	SessionToken string `json:"sessionToken"`
}

type dataPayload struct {
	Message interface{} `json:"message"`
	Subject interface{} `json:"subject"`
}

// NewAWSSNS creates a new AWSSNS binding instance.
func NewAWSSNS(logger logger.Logger) *AWSSNS {
	return &AWSSNS{logger: logger}
}

// Init does metadata parsing.
func (a *AWSSNS) Init(metadata bindings.Metadata) error {
	m, err := a.parseMetadata(metadata)
	if err != nil {
		return err
	}
	client, err := a.getClient(m)
	if err != nil {
		return err
	}
	a.client = client
	a.topicARN = m.TopicArn

	return nil
}

func (a *AWSSNS) parseMetadata(metadata bindings.Metadata) (*snsMetadata, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	var m snsMetadata
	err = json.Unmarshal(b, &m)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

func (a *AWSSNS) getClient(metadata *snsMetadata) (*sns.SNS, error) {
	sess, err := aws_auth.GetClient(metadata.AccessKey, metadata.SecretKey, metadata.SessionToken, metadata.Region, metadata.Endpoint)
	if err != nil {
		return nil, err
	}
	c := sns.New(sess)

	return c, nil
}

func (a *AWSSNS) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (a *AWSSNS) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var payload dataPayload
	err := json.Unmarshal(req.Data, &payload)
	if err != nil {
		return nil, err
	}

	msg := fmt.Sprintf("%v", payload.Message)
	subject := fmt.Sprintf("%v", payload.Subject)

	input := &sns.PublishInput{
		Message:  &msg,
		Subject:  &subject,
		TopicArn: &a.topicARN,
	}

	_, err = a.client.Publish(input)
	if err != nil {
		return nil, err
	}

	return nil, nil
}
