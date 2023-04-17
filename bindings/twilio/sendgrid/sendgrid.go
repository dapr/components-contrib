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

package sendgrid

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/sendgrid/sendgrid-go"
	"github.com/sendgrid/sendgrid-go/helpers/mail"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

// SendGrid allows sending of emails using the 3rd party SendGrid service.
type SendGrid struct {
	metadata sendGridMetadata
	logger   logger.Logger
}

// Our metadata holds standard email properties.
type sendGridMetadata struct {
	APIKey        string `mapstructure:"apiKey"`
	EmailFrom     string `mapstructure:"emailFrom"`
	EmailFromName string `mapstructure:"emailFromName"`
	EmailTo       string `mapstructure:"emailTo"`
	EmailToName   string `mapstructure:"emailToName"`
	Subject       string `mapstructure:"subject"`
	EmailCc       string `mapstructure:"emailCc"`
	EmailBcc      string `mapstructure:"emailBcc"`
}

// Wrapper to help decode SendGrid API errors.
type sendGridRestError struct {
	Errors []struct {
		Field   interface{} `json:"field"`
		Message interface{} `json:"message"`
		Help    interface{} `json:"help"`
	} `json:"errors"`
}

// NewSendGrid returns a new SendGrid bindings instance.
func NewSendGrid(logger logger.Logger) bindings.OutputBinding {
	return &SendGrid{logger: logger}
}

// Helper to parse metadata.
func (sg *SendGrid) parseMetadata(meta bindings.Metadata) (sendGridMetadata, error) {
	sgMeta := sendGridMetadata{}

	err := metadata.DecodeMetadata(meta.Properties, &sgMeta)
	if err != nil {
		return sgMeta, err
	}

	// Required properties
	if sgMeta.APIKey == "" {
		return sgMeta, errors.New("SendGrid binding error: apiKey field is required in metadata")
	}

	return sgMeta, nil
}

// Init does metadata parsing and not much else :).
func (sg *SendGrid) Init(_ context.Context, metadata bindings.Metadata) error {
	// Parse input metadata
	meta, err := sg.parseMetadata(metadata)
	if err != nil {
		return err
	}

	// Um, yeah that's about it!
	sg.metadata = meta

	return nil
}

func (sg *SendGrid) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

// Write does the work of sending message to SendGrid API.
func (sg *SendGrid) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	// We allow two possible sources of the properties we need,
	// the component metadata or request metadata, request takes priority if present

	// Build email from address, this is required
	var fromAddress *mail.Email
	if sg.metadata.EmailFrom != "" {
		// Optionally set the email from name
		fromName := ""
		if sg.metadata.EmailFromName != "" {
			fromName = sg.metadata.EmailFromName
		}

		fromAddress = mail.NewEmail(fromName, sg.metadata.EmailFrom)
	}
	if req.Metadata["emailFrom"] != "" {
		// Optionally set the email from name
		fromName := ""
		if req.Metadata["emailFromName"] != "" {
			fromName = req.Metadata["emailFromName"]
		}

		fromAddress = mail.NewEmail(fromName, req.Metadata["emailFrom"])
	}
	if fromAddress == nil {
		return nil, fmt.Errorf("error SendGrid from email not supplied")
	}

	// Build email to address, this is required
	var toAddress *mail.Email
	if sg.metadata.EmailTo != "" {
		// Optionally set the email to name
		toName := ""
		if sg.metadata.EmailToName != "" {
			toName = sg.metadata.EmailToName
		}

		toAddress = mail.NewEmail(toName, sg.metadata.EmailTo)
	}
	if req.Metadata["emailTo"] != "" {
		// Optionally set the email to name
		toName := ""
		if req.Metadata["emailToName"] != "" {
			toName = req.Metadata["emailToName"]
		}

		toAddress = mail.NewEmail(toName, req.Metadata["emailTo"])
	}
	if toAddress == nil {
		return nil, fmt.Errorf("error SendGrid to email not supplied")
	}

	// Build email subject, this is required
	subject := ""
	if sg.metadata.Subject != "" {
		subject = sg.metadata.Subject
	}
	if req.Metadata["subject"] != "" {
		subject = req.Metadata["subject"]
	}
	if subject == "" {
		return nil, fmt.Errorf("error SendGrid subject not supplied")
	}

	// Build email cc address, this is optional
	var ccAddress *mail.Email
	if sg.metadata.EmailCc != "" {
		ccAddress = mail.NewEmail("", sg.metadata.EmailCc)
	}
	if req.Metadata["emailCc"] != "" {
		ccAddress = mail.NewEmail("", req.Metadata["emailCc"])
	}

	// Build email bcc address, this is optional
	var bccAddress *mail.Email
	if sg.metadata.EmailBcc != "" {
		bccAddress = mail.NewEmail("", sg.metadata.EmailBcc)
	}
	if req.Metadata["emailBcc"] != "" {
		bccAddress = mail.NewEmail("", req.Metadata["emailBcc"])
	}

	// Email body is held in req.Data, after we tidy it up a bit
	emailBody, err := strconv.Unquote(string(req.Data))
	if err != nil {
		// Unquote will error if the string is not quoted (not exactly graceful!), so fallback using the string as is
		emailBody = string(req.Data)
	}

	// Construct email message
	email := mail.NewV3Mail()
	email.SetFrom(fromAddress)
	email.AddContent(mail.NewContent("text/html", emailBody))

	// Add other fields to email
	personalization := mail.NewPersonalization()
	personalization.AddTos(toAddress)
	personalization.Subject = subject
	if ccAddress != nil {
		personalization.AddCCs(ccAddress)
	}
	if bccAddress != nil {
		personalization.AddBCCs(bccAddress)
	}
	email.AddPersonalizations(personalization)

	// Send the email
	client := sendgrid.NewSendClient(sg.metadata.APIKey)
	resp, err := client.SendWithContext(ctx, email)
	if err != nil {
		return nil, fmt.Errorf("error from SendGrid, sending email failed: %+v", err)
	}

	// Check SendGrid response is OK
	if !(resp.StatusCode >= 200 && resp.StatusCode < 300) {
		// Extract the underlying error message(s) returned from SendGrid REST API
		sendGridError := sendGridRestError{}
		json.NewDecoder(strings.NewReader(resp.Body)).Decode(&sendGridError)
		// Pass it back to the caller, so they have some idea what went wrong
		return nil, fmt.Errorf("error from SendGrid, sending email failed: %d %+v", resp.StatusCode, sendGridError)
	}

	sg.logger.Info("sent email with SendGrid")

	return nil, nil
}

// GetComponentMetadata returns the metadata of the component.
func (sg *SendGrid) GetComponentMetadata() map[string]string {
	metadataStruct := sendGridMetadata{}
	metadataInfo := map[string]string{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType)
	return metadataInfo
}
