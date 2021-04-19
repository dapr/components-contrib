// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package sendgrid

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	"github.com/sendgrid/sendgrid-go"
	"github.com/sendgrid/sendgrid-go/helpers/mail"
)

// SendGrid allows sending of emails using the 3rd party SendGrid service
type SendGrid struct {
	metadata sendGridMetadata
	logger   logger.Logger
}

// Our metadata holds standard email properties
type sendGridMetadata struct {
	APIKey    string `json:"apiKey"`
	EmailFrom string `json:"emailFrom"`
	EmailTo   string `json:"emailTo"`
	Subject   string `json:"subject"`
	EmailCc   string `json:"emailCc"`
	EmailBcc  string `json:"emailBcc"`
}

// Wrapper to help decode SendGrid API errors
type sendGridRestError struct {
	Errors []struct {
		Field   interface{} `json:"field"`
		Message interface{} `json:"message"`
		Help    interface{} `json:"help"`
	} `json:"errors"`
}

// NewSendGrid returns a new SendGrid bindings instance
func NewSendGrid(logger logger.Logger) *SendGrid {
	return &SendGrid{logger: logger}
}

// Helper to parse metadata
func (sg *SendGrid) parseMetadata(meta bindings.Metadata) (sendGridMetadata, error) {
	sgMeta := sendGridMetadata{}

	// Required properties
	if val, ok := meta.Properties["apiKey"]; ok && val != "" {
		sgMeta.APIKey = val
	} else {
		return sgMeta, errors.New("SendGrid binding error: apiKey field is required in metadata")
	}

	// Optional properties, these can be set on a per request basis
	sgMeta.EmailTo = meta.Properties["emailTo"]
	sgMeta.EmailFrom = meta.Properties["emailFrom"]
	sgMeta.Subject = meta.Properties["subject"]
	sgMeta.EmailCc = meta.Properties["emailCc"]
	sgMeta.EmailBcc = meta.Properties["emailBcc"]

	return sgMeta, nil
}

// Init does metadata parsing and not much else :)
func (sg *SendGrid) Init(metadata bindings.Metadata) error {
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

// Write does the work of sending message to SendGrid API
func (sg *SendGrid) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	// We allow two possible sources of the properties we need,
	// the component metadata or request metadata, request takes priority if present

	// Build email from address, this is required
	var fromAddress *mail.Email
	if sg.metadata.EmailFrom != "" {
		fromAddress = mail.NewEmail("", sg.metadata.EmailFrom)
	}
	if req.Metadata["emailFrom"] != "" {
		fromAddress = mail.NewEmail("", req.Metadata["emailFrom"])
	}
	if fromAddress == nil {
		return nil, fmt.Errorf("error SendGrid from email not supplied")
	}

	// Build email to address, this is required
	var toAddress *mail.Email
	if sg.metadata.EmailTo != "" {
		toAddress = mail.NewEmail("", sg.metadata.EmailTo)
	}
	if req.Metadata["emailTo"] != "" {
		toAddress = mail.NewEmail("", req.Metadata["emailTo"])
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
	emailBody, _ := strconv.Unquote(string(req.Data))

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
	resp, err := client.Send(email)
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
