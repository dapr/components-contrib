// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package smtp

import (
	"crypto/tls"
	"errors"
	"fmt"
	"strconv"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	"gopkg.in/gomail.v2"
)

// Mailer allows sending of emails using the Simple Mail Transfer Protocol
type Mailer struct {
	metadata Metadata
	logger   logger.Logger
}

// Metadata holds standard email properties
type Metadata struct {
	Host          string `json:"host"`
	Port          int    `json:"port"`
	User          string `json:"user"`
	SkipTLSVerify bool   `json:"skipTLSVerify"`
	Password      string `json:"password"`
	EmailFrom     string `json:"emailFrom"`
	EmailTo       string `json:"emailTo"`
	EmailCC       string `json:"emailCC"`
	EmailBCC      string `json:"emailBCC"`
	Subject       string `json:"subject"`
}

// NewSMTP returns a new smtp binding instance
func NewSMTP(logger logger.Logger) *Mailer {
	return &Mailer{logger: logger}
}

// Init smtp component (parse metadata)
func (s *Mailer) Init(metadata bindings.Metadata) error {
	// parse metadata
	meta, err := s.parseMetadata(metadata)
	if err != nil {
		return err
	}
	s.metadata = meta

	return nil
}

// Operations returns the allowed binding operations
func (s *Mailer) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

// Invoke sends an email message
func (s *Mailer) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	// Merge config metadata with request metadata
	metadata := s.metadata.mergeWithRequestMetadata(req)
	if metadata.EmailFrom == "" {
		return nil, fmt.Errorf("smtp binding error: fromEmail property not supplied in configuration- or request-metadata")
	}
	if metadata.EmailTo == "" {
		return nil, fmt.Errorf("smtp binding error: emailTo property not supplied in configuration- or request-metadata")
	}
	if metadata.Subject == "" {
		return nil, fmt.Errorf("smtp binding error: subject property not supplied in configuration- or request-metadata")
	}

	// Compose message
	msg := gomail.NewMessage()
	msg.SetHeader("From", metadata.EmailFrom)
	msg.SetHeader("To", metadata.EmailTo)
	msg.SetHeader("CC", metadata.EmailCC)
	msg.SetHeader("BCC", metadata.EmailBCC)
	msg.SetHeader("Subject", metadata.Subject)
	body, _ := strconv.Unquote(string(req.Data))
	msg.SetBody("text/html", body)

	// Send message
	dialer := gomail.NewDialer(metadata.Host, metadata.Port, metadata.User, metadata.Password)
	if metadata.SkipTLSVerify {
		/* #nosec */
		dialer.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	}
	if err := dialer.DialAndSend(msg); err != nil {
		return nil, fmt.Errorf("error from smtp binding, sending email failed: %+v", err)
	}

	// Log success
	s.logger.Debug("smtp binding: sent email successfully")

	return nil, nil
}

// Helper to parse metadata
func (s *Mailer) parseMetadata(meta bindings.Metadata) (Metadata, error) {
	smtpMeta := Metadata{}

	// required metadata properties
	if meta.Properties["host"] == "" || meta.Properties["port"] == "" ||
		meta.Properties["user"] == "" || meta.Properties["password"] == "" {
		return smtpMeta, errors.New("smtp binding error: host, port, user and password fields are required in metadata")
	}
	smtpMeta.Host = meta.Properties["host"]
	port, err := strconv.Atoi(meta.Properties["port"])
	if err != nil {
		return smtpMeta, fmt.Errorf("smtp binding error: Unable to parse specified port to integer value")
	}
	smtpMeta.Port = port

	s.logger.Debugf("smtp binding: using server %v:%v", s.metadata.Host, s.metadata.Port)

	smtpMeta.User = meta.Properties["user"]
	smtpMeta.Password = meta.Properties["password"]

	// Optional properties (override per request)
	skipTLSVerify, err := strconv.ParseBool(meta.Properties["skipTLSVerify"])
	if err == nil {
		smtpMeta.SkipTLSVerify = skipTLSVerify
		if smtpMeta.SkipTLSVerify {
			s.logger.Warn("smtp binding warning: Skip TLS Verification is enabled. This is insecure and is NOT recommended for production scenarios.")
		}
	}
	smtpMeta.EmailTo = meta.Properties["emailTo"]
	smtpMeta.EmailCC = meta.Properties["emailCC"]
	smtpMeta.EmailBCC = meta.Properties["emailBCC"]
	smtpMeta.EmailFrom = meta.Properties["emailFrom"]
	smtpMeta.Subject = meta.Properties["subject"]

	return smtpMeta, nil
}

// Helper to merge config and request metadata
func (metadata Metadata) mergeWithRequestMetadata(req *bindings.InvokeRequest) Metadata {
	merged := metadata

	if emailFrom := req.Metadata["emailFrom"]; emailFrom != "" {
		merged.EmailFrom = emailFrom
	}

	if emailTo := req.Metadata["emailTo"]; emailTo != "" {
		merged.EmailTo = emailTo
	}

	if emailCC := req.Metadata["emailCC"]; emailCC != "" {
		merged.EmailCC = emailCC
	}

	if emailBCC := req.Metadata["emailBCC"]; emailBCC != "" {
		merged.EmailBCC = emailBCC
	}

	if subject := req.Metadata["subject"]; subject != "" {
		merged.Subject = subject
	}

	return merged
}
