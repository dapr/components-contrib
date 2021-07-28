// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package ses

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ses"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

const (
	// The character encoding for the email.
	CharSet = "UTF-8"
)

// AWSSES is an AWS SNS binding
type AWSSES struct {
	metadata sesMetadata
	logger   logger.Logger
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

// NewAWSSES creates a new AWSSES binding instance
func NewAWSSES(logger logger.Logger) *AWSSES {
	return &AWSSES{logger: logger}
}

// Init does metadata parsing
func (a *AWSSES) Init(metadata bindings.Metadata) error {
	// Parse input metadata
	meta, err := a.parseMetadata(metadata)
	if err != nil {
		return err
	}

	a.metadata = meta

	return nil
}

func (a *AWSSES) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (a *AWSSES) parseMetadata(meta bindings.Metadata) (sesMetadata, error) {
	sesMeta := sesMetadata{}

	if meta.Properties["region"] == "" || meta.Properties["accessKey"] == "" ||
		meta.Properties["secretKey"] == "" {
		return sesMeta, errors.New("SES binding error: region, accessKey or secretKey fields are required in metadata")
	}

	sesMeta.Region = meta.Properties["region"]
	sesMeta.AccessKey = meta.Properties["accessKey"]
	sesMeta.SecretKey = meta.Properties["secretKey"]
	sesMeta.SessionToken = meta.Properties["sessionToken"]

	// Optional properties, these can be set on a per request basis
	sesMeta.EmailTo = meta.Properties["emailTo"]
	sesMeta.EmailFrom = meta.Properties["emailFrom"]
	sesMeta.Subject = meta.Properties["subject"]
	sesMeta.EmailCc = meta.Properties["emailCc"]
	sesMeta.EmailBcc = meta.Properties["emailBcc"]

	return sesMeta, nil
}

func (a *AWSSES) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	metadata := a.metadata.mergeWithRequestMetadata(req)

	if metadata.EmailFrom == "" {
		return nil, fmt.Errorf("SES binding error: emailFrom property not supplied in configuration- or request-metadata")
	}
	if metadata.EmailTo == "" {
		return nil, fmt.Errorf("SES binding error: emailTo property not supplied in configuration- or request-metadata")
	}
	if metadata.Subject == "" {
		return nil, fmt.Errorf("SES binding error: subject property not supplied in configuration- or request-metadata")
	}

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(metadata.Region),
		Credentials: credentials.NewStaticCredentials(metadata.AccessKey, metadata.SecretKey, metadata.SessionToken),
	})
	if err != nil {
		return nil, fmt.Errorf("SES binding error: error creating AWS session %w", err)
	}

	// Create an SES instance
	svc := ses.New(sess)

	body, err := strconv.Unquote(string(req.Data))
	if err != nil {
		return nil, fmt.Errorf("SES binding error: can't unquote data field %w", err)
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
	result, err := svc.SendEmail(input)
	if err != nil {
		return nil, fmt.Errorf("SES binding error: sending email failed: %w", err)
	}

	a.logger.Debug("SES binding: sent email successfully ", result.MessageId)

	return nil, nil
}

// Helper to merge config and request metadata
func (metadata sesMetadata) mergeWithRequestMetadata(req *bindings.InvokeRequest) sesMetadata {
	merged := metadata

	if emailFrom := req.Metadata["emailFrom"]; emailFrom != "" {
		merged.EmailFrom = emailFrom
	}

	if emailTo := req.Metadata["emailTo"]; emailTo != "" {
		merged.EmailTo = emailTo
	}

	if emailCC := req.Metadata["emailCc"]; emailCC != "" {
		merged.EmailCc = emailCC
	}

	if emailBCC := req.Metadata["emailBcc"]; emailBCC != "" {
		merged.EmailBcc = emailBCC
	}

	if subject := req.Metadata["subject"]; subject != "" {
		merged.Subject = subject
	}

	return merged
}
