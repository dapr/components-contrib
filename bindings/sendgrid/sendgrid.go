// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package sendgrid

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/sendgrid/sendgrid-go"
	"github.com/sendgrid/sendgrid-go/helpers/mail"
)

// SendGrid allows sending of emails using the 3rd party SendGrid service
type SendGrid struct {
	metadata sendGridMetadata
	logger   logger.Logger
}

type sendGridMetadata struct {
	APIKey    string `json:"apiKey"`
	EmailFrom string `json:"emailFrom"`
	EmailTo   string `json:"emailTo"`
	Subject   string `json:"subject"`
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

	// Optional properties
	sgMeta.EmailTo = meta.Properties["emailTo"]
	sgMeta.EmailFrom = meta.Properties["emailFrom"]
	sgMeta.Subject = meta.Properties["subject"]

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

// Write does the work of sending message to SendGrid API
func (sg *SendGrid) Write(req *bindings.WriteRequest) error {
	// We allow two possible sources of these fields, the component metadata or request metadata
	// Build email from,
	var from *mail.Email
	if len(sg.metadata.EmailFrom) > 0 {
		from = mail.NewEmail("", sg.metadata.EmailFrom)
	}
	if len(req.Metadata["emailFrom"]) > 0 {
		from = mail.NewEmail("", req.Metadata["emailFrom"])
	}
	if from == nil {
		return fmt.Errorf("error SendGrid from email not supplied")
	}

	// Build email to
	var to *mail.Email
	if len(sg.metadata.EmailTo) > 0 {
		to = mail.NewEmail("", sg.metadata.EmailFrom)
	}
	if len(req.Metadata["emailTo"]) > 0 {
		to = mail.NewEmail("", req.Metadata["emailTo"])
	}
	if to == nil {
		return fmt.Errorf("error SendGrid to email not supplied")
	}

	// Build email subject
	subject := ""
	if len(sg.metadata.EmailTo) > 0 {
		subject = sg.metadata.Subject
	}
	if len(req.Metadata["subject"]) > 0 {
		subject = req.Metadata["subject"]
	}
	if len(subject) == 0 {
		return fmt.Errorf("error SendGrid subject not supplied")
	}

	// Email body is held in req.Data, after we tidy it up a bit
	htmlContent, _ := strconv.Unquote(string(req.Data))
	plainContent := "Sorry plain text not supported"

	// Construct message
	message := mail.NewSingleEmail(from, subject, to, plainContent, htmlContent)

	// Send it :)
	client := sendgrid.NewSendClient(sg.metadata.APIKey)
	resp, err := client.Send(message)
	if err != nil {
		return fmt.Errorf("error from SendGrid, sending email failed: %+v", err)
	}

	// Check SendGrid response is OK
	if !(resp.StatusCode >= 200 && resp.StatusCode < 300) {
		return fmt.Errorf("error from SendGrid, sending email failed: %d", resp.StatusCode)
	}

	sg.logger.Info("sent email with SendGrid")
	return nil
}
