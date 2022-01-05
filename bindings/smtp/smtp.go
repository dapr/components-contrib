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

package smtp

import (
	"crypto/tls"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"gopkg.in/gomail.v2"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

const (
	defaultPriority = 3
	lowestPriority  = 1
	highestPriority = 5
	mailSeparator   = ";"
)

// Mailer allows sending of emails using the Simple Mail Transfer Protocol.
type Mailer struct {
	metadata Metadata
	logger   logger.Logger
}

// Metadata holds standard email properties.
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
	Priority      int    `json:"priority"`
}

// NewSMTP returns a new smtp binding instance.
func NewSMTP(logger logger.Logger) *Mailer {
	return &Mailer{logger: logger}
}

// Init smtp component (parse metadata).
func (s *Mailer) Init(metadata bindings.Metadata) error {
	// parse metadata
	meta, err := s.parseMetadata(metadata)
	if err != nil {
		return err
	}
	s.metadata = meta

	return nil
}

// Operations returns the allowed binding operations.
func (s *Mailer) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

// Invoke sends an email message.
func (s *Mailer) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	// Merge config metadata with request metadata
	metadata, err := s.metadata.mergeWithRequestMetadata(req)
	if err != nil {
		return nil, err
	}
	if metadata.EmailFrom == "" {
		return nil, fmt.Errorf("smtp binding error: emailFrom property not supplied in configuration- or request-metadata")
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
	msg.SetHeader("To", metadata.parseAddresses(metadata.EmailTo)...)
	if metadata.EmailCC != "" {
		msg.SetHeader("Cc", metadata.parseAddresses(metadata.EmailCC)...)
	}
	if metadata.EmailBCC != "" {
		msg.SetHeader("Bcc", metadata.parseAddresses(metadata.EmailBCC)...)
	}

	msg.SetHeader("Subject", metadata.Subject)
	msg.SetHeader("X-priority", strconv.Itoa(metadata.Priority))

	body, err := strconv.Unquote(string(req.Data))

	if err != nil {
		// When data arrives over gRPC it's not quoted. Unquoting the original data will result in an error.
		// Instead of unquoting it we'll just use the raw string as that one's already in the right format.

		msg.SetBody("text/html", string(req.Data))
	} else {
		msg.SetBody("text/html", body)
	}

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

// Helper to parse metadata.
func (s *Mailer) parseMetadata(meta bindings.Metadata) (Metadata, error) {
	smtpMeta := Metadata{}

	// required metadata properties
	if meta.Properties["host"] == "" || meta.Properties["port"] == "" {
		return smtpMeta, errors.New("smtp binding error: host and port fields are required in metadata")
	}

	if (meta.Properties["user"] != "" && meta.Properties["password"] == "") ||
		(meta.Properties["user"] == "" && meta.Properties["password"] != "") {
		return smtpMeta, errors.New("smtp binding error: both user and password fields are required in metadata")
	}

	if meta.Properties["user"] == "" && meta.Properties["password"] == "" {
		s.logger.Warn("smtp binding warn: User and password are empty")
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
	err = smtpMeta.parsePriority(meta.Properties["priority"])

	if err != nil {
		return smtpMeta, err
	}

	return smtpMeta, nil
}

// Helper to merge config and request metadata.
func (metadata Metadata) mergeWithRequestMetadata(req *bindings.InvokeRequest) (Metadata, error) {
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

	if priority := req.Metadata["priority"]; priority != "" {
		err := merged.parsePriority(priority)
		if err != nil {
			return merged, err
		}
	}

	return merged, nil
}

func (metadata *Metadata) parsePriority(req string) error {
	if req == "" {
		metadata.Priority = defaultPriority
	} else {
		priority, err := strconv.Atoi(req)
		if err != nil {
			return err
		}
		if priority < lowestPriority || priority > highestPriority {
			return fmt.Errorf("smtp binding error:  priority value must be between %d (highest) and %d (lowest)", lowestPriority, highestPriority)
		}
		metadata.Priority = priority
	}

	return nil
}

func (metadata Metadata) parseAddresses(addresses string) []string {
	return strings.Split(addresses, mailSeparator)
}
