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

package ses

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ses"

	"github.com/dapr/components-contrib/bindings"
	awsAuth "github.com/dapr/components-contrib/common/authentication/aws"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

const (
	// The character encoding for the email.
	CharSet = "UTF-8"
)

// AWSSES is an AWS SNS binding.
type AWSSES struct {
	metadata *sesMetadata
	logger   logger.Logger
	svc      *ses.SES
}

type sesMetadata struct {
	Region       string `json:"region"`
	AccessKey    string `json:"accessKey"`
	SecretKey    string `json:"secretKey"`
	SessionToken string `json:"sessionToken"`
	EmailFrom    string `json:"emailFrom"`
	EmailTo      string `json:"emailTo"`
	Subject      string `json:"subject"`
	EmailCc      string `json:"emailCc"`
	EmailBcc     string `json:"emailBcc"`
}

// NewAWSSES creates a new AWSSES binding instance.
func NewAWSSES(logger logger.Logger) bindings.OutputBinding {
	return &AWSSES{logger: logger}
}

// Init does metadata parsing.
func (a *AWSSES) Init(_ context.Context, metadata bindings.Metadata) error {
	// Parse input metadata
	meta, err := a.parseMetadata(metadata)
	if err != nil {
		return err
	}

	svc, err := a.getClient(meta)
	if err != nil {
		return err
	}
	a.metadata = meta
	a.svc = svc

	return nil
}

func (a *AWSSES) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (a *AWSSES) parseMetadata(meta bindings.Metadata) (*sesMetadata, error) {
	m := sesMetadata{}
	kitmd.DecodeMetadata(meta.Properties, &m)

	return &m, nil
}

func (a *AWSSES) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	metadata := a.metadata.mergeWithRequestMetadata(req)

	if metadata.EmailFrom == "" {
		return nil, errors.New("SES binding error: emailFrom property not supplied in configuration- or request-metadata")
	}
	if metadata.EmailTo == "" {
		return nil, errors.New("SES binding error: emailTo property not supplied in configuration- or request-metadata")
	}
	if metadata.Subject == "" {
		return nil, errors.New("SES binding error: subject property not supplied in configuration- or request-metadata")
	}

	body, err := strconv.Unquote(string(req.Data))
	if err != nil {
		return nil, fmt.Errorf("SES binding error. Can't unquote data field: %w", err)
	}

	// Assemble the email.
	input := &ses.SendEmailInput{
		Destination: &ses.Destination{
			ToAddresses: aws.StringSlice(strings.Split(metadata.EmailTo, ";")),
		},
		Message: &ses.Message{
			Body: &ses.Body{
				Html: &ses.Content{
					Charset: aws.String(CharSet),
					Data:    aws.String(body),
				},
			},
			Subject: &ses.Content{
				Charset: aws.String(CharSet),
				Data:    aws.String(metadata.Subject),
			},
		},
		Source: aws.String(metadata.EmailFrom),
		// TODO: Add  configuration set: https://docs.aws.amazon.com/ses/latest/DeveloperGuide/using-configuration-sets.html
		// ConfigurationSetName: aws.String(ConfigurationSet),
	}

	if metadata.EmailCc != "" {
		input.SetDestination(&ses.Destination{
			CcAddresses: aws.StringSlice(strings.Split(metadata.EmailCc, ";")),
		})
	}
	if metadata.EmailBcc != "" {
		input.SetDestination(&ses.Destination{
			BccAddresses: aws.StringSlice(strings.Split(metadata.EmailBcc, ";")),
		})
	}

	// Attempt to send the email.
	result, err := a.svc.SendEmail(input)
	if err != nil {
		return nil, fmt.Errorf("SES binding error. Sending email failed: %w", err)
	}

	a.logger.Debug("SES binding: sent email successfully ", result.MessageId)

	return nil, nil
}

// Helper to merge config and request metadata.
func (metadata sesMetadata) mergeWithRequestMetadata(req *bindings.InvokeRequest) sesMetadata {
	merged := metadata
	kitmd.DecodeMetadata(req.Metadata, &merged)
	return merged
}

func (a *AWSSES) getClient(metadata *sesMetadata) (*ses.SES, error) {
	sess, err := awsAuth.GetClient(metadata.AccessKey, metadata.SecretKey, metadata.SessionToken, metadata.Region, "")
	if err != nil {
		return nil, fmt.Errorf("SES binding error: error creating AWS session %w", err)
	}

	// Create an SES instance
	svc := ses.New(sess)

	return svc, nil
}

// GetComponentMetadata returns the metadata of the component.
func (a *AWSSES) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := sesMetadata{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.BindingType)
	return
}

func (a *AWSSES) Close() error {
	return nil
}
