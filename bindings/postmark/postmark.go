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

package postmark

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/mrz1836/postmark"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

// Postmark allows sending of emails using the 3rd party Postmark service.
type Postmark struct {
	metadata postmarkMetadata
	logger   logger.Logger
}

// Our metadata holds standard email properties.
type postmarkMetadata struct {
	ServerToken  string `json:"serverToken"`
	AccountToken string `json:"accountToken"`
	EmailFrom    string `json:"emailFrom"`
	EmailTo      string `json:"emailTo"`
	Subject      string `json:"subject"`
	EmailCc      string `json:"emailCc"`
	EmailBcc     string `json:"emailBcc"`
}

// NewPostmark returns a new Postmark bindings instance.
func NewPostmark(logger logger.Logger) bindings.OutputBinding {
	return &Postmark{logger: logger}
}

// Helper to parse metadata.
func (p *Postmark) parseMetadata(meta bindings.Metadata) (postmarkMetadata, error) {
	pMeta := postmarkMetadata{}

	// Required properties
	if val, ok := meta.Properties["serverToken"]; ok && val != "" {
		pMeta.ServerToken = val
	} else {
		return pMeta, errors.New("Postmark binding error: serverToken field is required in metadata")
	}
	if val, ok := meta.Properties["accountToken"]; ok && val != "" {
		pMeta.AccountToken = val
	} else {
		return pMeta, errors.New("Postmark binding error: accountToken field is required in metadata")
	}

	// Optional properties, these can be set on a per request basis
	pMeta.EmailTo = meta.Properties["emailTo"]
	pMeta.EmailFrom = meta.Properties["emailFrom"]
	pMeta.Subject = meta.Properties["subject"]
	pMeta.EmailCc = meta.Properties["emailCc"]
	pMeta.EmailBcc = meta.Properties["emailBcc"]

	return pMeta, nil
}

// Init does metadata parsing and not much else :).
func (p *Postmark) Init(metadata bindings.Metadata) error {
	// Parse input metadata
	meta, err := p.parseMetadata(metadata)
	if err != nil {
		return err
	}

	// Um, yeah that's about it!
	p.metadata = meta

	return nil
}

// Operations returns list of operations supported by Postmark binding.
func (p *Postmark) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

// Invoke does the work of sending message to Postmark API.
func (p *Postmark) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	// We allow two possible sources of the properties we need,
	// the component metadata or request metadata, request takes priority if present

	var email postmark.Email

	// Build email from address, this is required
	if p.metadata.EmailFrom != "" {
		email.From = p.metadata.EmailFrom
	}
	if req.Metadata["emailFrom"] != "" {
		email.From = req.Metadata["emailFrom"]
	}
	if len(email.From) == 0 {
		return nil, fmt.Errorf("error Postmark from email not supplied")
	}

	// Build email to address, this is required
	if p.metadata.EmailTo != "" {
		email.To = p.metadata.EmailTo
	}
	if req.Metadata["emailTo"] != "" {
		email.To = req.Metadata["emailTo"]
	}
	if len(email.To) == 0 {
		return nil, fmt.Errorf("error Postmark to email not supplied")
	}

	// Build email subject, this is required
	if p.metadata.Subject != "" {
		email.Subject = p.metadata.Subject
	}
	if req.Metadata["subject"] != "" {
		email.Subject = req.Metadata["subject"]
	}
	if len(email.Subject) == 0 {
		return nil, fmt.Errorf("error Postmark subject not supplied")
	}

	// Build email cc address, this is optional
	if p.metadata.EmailCc != "" {
		email.Cc = p.metadata.EmailCc
	}
	if req.Metadata["emailCc"] != "" {
		email.Cc = req.Metadata["emailCc"]
	}

	// Build email bcc address, this is optional
	if p.metadata.EmailBcc != "" {
		email.Bcc = p.metadata.EmailBcc
	}
	if req.Metadata["emailBcc"] != "" {
		email.Bcc = req.Metadata["emailBcc"]
	}

	// Email body is held in req.Data, after we tidy it up a bit
	emailBody, _ := strconv.Unquote(string(req.Data))
	email.HTMLBody = emailBody

	// Send the email
	client := postmark.NewClient(p.metadata.ServerToken, p.metadata.AccountToken)
	_, err := client.SendEmail(ctx, email)
	if err != nil {
		return nil, fmt.Errorf("error from Postmark, sending email failed: %+v", err)
	}

	p.logger.Info("sent email with Postmark")

	return nil, nil
}
