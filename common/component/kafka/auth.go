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

package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
	aws2 "github.com/aws/aws-sdk-go-v2/aws"
)

func updatePasswordAuthInfo(config *sarama.Config, metadata *KafkaMetadata, saslUsername, saslPassword string) {
	config.Net.SASL.Enable = true
	config.Net.SASL.User = saslUsername
	config.Net.SASL.Password = saslPassword
	if metadata.SaslMechanism == "SHA-256" {
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
		config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
	} else if metadata.SaslMechanism == "SHA-512" {
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
		config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	} else {
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	}
}

func updateMTLSAuthInfo(config *sarama.Config, metadata *KafkaMetadata) error {
	if metadata.TLSDisable {
		return errors.New("kafka: cannot configure mTLS authentication when TLSDisable is 'true'")
	}
	cert, err := tls.X509KeyPair([]byte(metadata.TLSClientCert), []byte(metadata.TLSClientKey))
	if err != nil {
		return fmt.Errorf("unable to load client certificate and key pair. Err: %w", err)
	}
	config.Net.TLS.Config.Certificates = []tls.Certificate{cert}
	return nil
}

func updateTLSConfig(config *sarama.Config, metadata *KafkaMetadata) error {
	if metadata.TLSDisable || metadata.AuthType == noAuthType {
		config.Net.TLS.Enable = false
		return nil
	}
	config.Net.TLS.Enable = true

	if !metadata.TLSSkipVerify && metadata.TLSCaCert == "" {
		return nil
	}
	//nolint:gosec
	config.Net.TLS.Config = &tls.Config{InsecureSkipVerify: metadata.TLSSkipVerify, MinVersion: tls.VersionTLS12}
	if metadata.TLSCaCert != "" {
		caCertPool := x509.NewCertPool()
		if ok := caCertPool.AppendCertsFromPEM([]byte(metadata.TLSCaCert)); !ok {
			return errors.New("kafka error: unable to load ca certificate")
		}
		config.Net.TLS.Config.RootCAs = caCertPool
	}

	return nil
}

func updateOidcAuthInfo(config *sarama.Config, metadata *KafkaMetadata) error {
	tokenProvider := metadata.getOAuthTokenSource()

	if metadata.TLSCaCert != "" {
		err := tokenProvider.addCa(metadata.TLSCaCert)
		if err != nil {
			return fmt.Errorf("kafka: error setting oauth client trusted CA: %w", err)
		}
	}

	config.Net.SASL.Enable = true
	config.Net.SASL.Mechanism = sarama.SASLTypeOAuth
	config.Net.SASL.TokenProvider = tokenProvider

	return nil
}

func updateAWSIAMAuthInfo(ctx context.Context, config *sarama.Config, metadata *KafkaMetadata) error {
	config.Net.SASL.Enable = true
	config.Net.SASL.Mechanism = sarama.SASLTypeOAuth
	config.Net.SASL.TokenProvider = &mskAccessTokenProvider{
		ctx:                  ctx,
		generateTokenTimeout: 10 * time.Second,
		region:               metadata.AWSRegion,
		accessKey:            metadata.AWSAccessKey,
		secretKey:            metadata.AWSSecretKey,
		sessionToken:         metadata.AWSSessionToken,
		awsIamRoleArn:        metadata.AWSIamRoleArn,
		awsStsSessionName:    metadata.AWSStsSessionName,
	}

	_, err := config.Net.SASL.TokenProvider.Token()
	if err != nil {
		return fmt.Errorf("error validating iam credentials %v", err)
	}
	return nil
}

type mskAccessTokenProvider struct {
	ctx                  context.Context
	generateTokenTimeout time.Duration
	accessKey            string
	secretKey            string
	sessionToken         string
	awsIamRoleArn        string
	awsStsSessionName    string
	region               string
}

func (m *mskAccessTokenProvider) Token() (*sarama.AccessToken, error) {
	// this function can't use the context passed on Init because that context would be cancelled right after Init
	ctx, cancel := context.WithTimeout(m.ctx, m.generateTokenTimeout)
	defer cancel()

	if m.accessKey != "" && m.secretKey != "" {
		token, _, err := signer.GenerateAuthTokenFromCredentialsProvider(ctx, m.region, aws2.CredentialsProviderFunc(func(ctx context.Context) (aws2.Credentials, error) {
			return aws2.Credentials{
				AccessKeyID:     m.accessKey,
				SecretAccessKey: m.secretKey,
				SessionToken:    m.sessionToken,
			}, nil
		}))
		return &sarama.AccessToken{Token: token}, err
	} else if m.awsIamRoleArn != "" {
		token, _, err := signer.GenerateAuthTokenFromRole(ctx, m.region, m.awsIamRoleArn, m.awsStsSessionName)
		return &sarama.AccessToken{Token: token}, err
	}

	token, _, err := signer.GenerateAuthToken(ctx, m.region)
	return &sarama.AccessToken{Token: token}, err
}
