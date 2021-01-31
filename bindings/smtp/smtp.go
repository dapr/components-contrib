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
	"github.com/dapr/dapr/pkg/logger"
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
	Port          string `json:"port"`
	User          string `json:"user"`
	SkipTLSVerify bool   `json:"skipTLSVerify"`
	Password      string `json:"password"`
	EmailFrom     string `json:"emailFrom"`
	EmailTo       string `json:"emailTo"`
	EmailCC       string `json:"emailCC"`
	EmailBCC       string `json:"emailBCC"`
	Subject       string `json:"subject"`
}

// NewSMTP returns a new SMTP bindings instance
func NewSMTP(logger logger.Logger) *Mailer {
	return &Mailer{logger: logger}
}

// Helper to parse metadata
func (s *Mailer) parseMetadata(meta bindings.Metadata) (Metadata, error) {
	smtpMeta := Metadata{}

	// required metadata properties
	if meta.Properties["host"] == "" || meta.Properties["port"] == "" ||
		meta.Properties["user"] == "" || meta.Properties["password"] == "" {
		return smtpMeta, errors.New("SMTP binding error: host, port, user and password fields are required in metadata")
	}
	smtpMeta.Host = meta.Properties["host"]
	smtpMeta.Port = meta.Properties["port"]
	smtpMeta.User = meta.Properties["user"]
	smtpMeta.Password = meta.Properties["password"]

	// Optional properties (override per request)
	smtpMeta.SkipTLSVerify = false
	skipTLSVerify, err := strconv.ParseBool(meta.Properties["skipTLSVerify"])
	if err == nil {
		smtpMeta.SkipTLSVerify = skipTLSVerify
		if smtpMeta.SkipTLSVerify {
			s.logger.Warn("SMTP Binding warning: Skip TLS Verification is enabled. This is insecure and is NOT recommended for production scenarios.")
		}
	}
	smtpMeta.EmailTo = meta.Properties["emailTo"]
	smtpMeta.EmailCC = meta.Properties["emailCC"]
	smtpMeta.EmailBCC = meta.Properties["emailBCC"]
	smtpMeta.EmailFrom = meta.Properties["emailFrom"]
	smtpMeta.Subject = meta.Properties["subject"]

	return smtpMeta, nil
}

// Init Smtp component (parse metadata)
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
	// host and port
	port, err := strconv.Atoi(s.metadata.Port)
	if err != nil {
		s.logger.Fatal("SMTP binding error: Unable to parse specified port to integer value")
	}
	s.logger.Debugf("SMTP binding: using server %v:%v", s.metadata.Host, port)

	// From
	from := determineFrom(s.metadata, req)
	if from == "" {
		return nil, fmt.Errorf("SMTP binding error: fromEmail property not supplied in metadata or request")
	}

	// To
	to := determineTo(s.metadata, req)
	if to == "" {
		return nil, fmt.Errorf("SMTP binding error: emailTo property not supplied in metadata or request")
	}

	// (B)CC
	cc := determineCC(s.metadata, req)
	bcc := determineBCC(s.metadata, req)

	// Subject
	subject := determineSubject(s.metadata, req)
	if subject == "" {
		return nil, fmt.Errorf("SMTP binding error: subject property not supplied in metadata or request")
	}

	// Compose message
	msg := gomail.NewMessage()
	msg.SetHeader("From", from)
	msg.SetHeader("To", to)
	msg.SetHeader("CC", cc)
	msg.SetHeader("BCC", bcc)
	msg.SetHeader("Subject", subject)
	body, _ := strconv.Unquote(string(req.Data))
	msg.SetBody("text/html", body)

	// Send message
	dialer := gomail.NewDialer(s.metadata.Host, port, s.metadata.User, s.metadata.Password)
	if s.metadata.SkipTLSVerify {
		dialer.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	}
	if err := dialer.DialAndSend(msg); err != nil {
		s.logger.Fatal(err)
	}

	// Log success
	s.logger.Info("SMTP binding: sent email")

	return nil, nil
}

func determineFrom(metadata Metadata, req *bindings.InvokeRequest) string {
	var result string
	if metadata.EmailFrom != "" {
		result = metadata.EmailFrom
	}
	if req.Metadata["emailFrom"] != "" {
		result = req.Metadata["emailFrom"]
	}

	return result
}

func determineTo(metadata Metadata, req *bindings.InvokeRequest) string {
	var to string
	if metadata.EmailTo != "" {
		to = metadata.EmailTo
	}
	if req.Metadata["emailTo"] != "" {
		to = req.Metadata["emailTo"]
	}

	return to
}

func determineCC(metadata Metadata, req *bindings.InvokeRequest) string {
	var cc string
	if metadata.EmailCC != "" {
		cc = metadata.EmailCC
	}
	if req.Metadata["emailCC"] != "" {
		cc = req.Metadata["emailCC"]
	}

	return cc
}

func determineBCC(metadata Metadata, req *bindings.InvokeRequest) string {
	var bcc string
	if metadata.EmailBCC != "" {
		bcc = metadata.EmailBCC
	}
	if req.Metadata["emailBCC"] != "" {
		bcc = req.Metadata["emailBCC"]
	}

	return bcc
}

func determineSubject(metadata Metadata, req *bindings.InvokeRequest) string {
	var subject string
	if metadata.Subject != "" {
		subject = metadata.Subject
	}
	if req.Metadata["subject"] != "" {
		subject = req.Metadata["subject"]
	}

	return subject
}

func buildMessage(toAddress string, subject string, body string) []byte {
	msg := []byte("To: " + toAddress + "\r\n" +
		"Subject: " + subject + "\r\n" +
		"\r\n" +
		body + "\r\n")

	return msg
}
