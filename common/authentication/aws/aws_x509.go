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

package aws

import (
	"context"
	"crypto/ecdsa"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	awssh "github.com/aws/rolesanywhere-credential-helper/aws_signing_helper"
	"github.com/aws/rolesanywhere-credential-helper/rolesanywhere"
	"github.com/aws/rolesanywhere-credential-helper/rolesanywhere/rolesanywhereiface"
	cryptopem "github.com/dapr/kit/crypto/pem"
	spiffecontext "github.com/dapr/kit/crypto/spiffe/context"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

type x509Auth struct {
	// todo unexport these fields except the channel
	TrustProfileArn   *string        `json:"trustProfileArn" mapstructure:"trustProfileArn"`
	TrustAnchorArn    *string        `json:"trustAnchorArn" mapstructure:"trustAnchorArn"`
	AssumeRoleArn     *string        `json:"assumeRoleArn" mapstructure:"assumeRoleArn"`
	SessionDuration   *time.Duration `json:"sessionDuration" mapstructure:"sessionDuration"`
	sessionExpiration time.Time

	chainPEM []byte
	keyPEM   []byte

	sessionUpdateChannel chan *session.Session

	rolesAnywhereClient rolesanywhereiface.RolesAnywhereAPI
}

func (a *AWS) getCertPEM(ctx context.Context) error {
	// retrieve svid from spiffe context
	svid, ok := spiffecontext.From(ctx)
	if !ok {
		return errors.New("no SVID found in context")
	}
	// get x.509 svid
	svidx, err := svid.GetX509SVID()
	if err != nil {
		return err
	}

	// marshal x.509 svid to pem format
	chainPEM, keyPEM, err := svidx.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal SVID: %w", err)
	}

	a.x509Auth.chainPEM = chainPEM
	a.x509Auth.keyPEM = keyPEM
	return nil
}

func (a *AWS) getX509Client(ctx context.Context) (*session.Session, error) {
	// retrieve svid from spiffe context
	err := a.getCertPEM(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get x.509 credentials: %v", err)
	}

	if err := a.initializeTrustAnchors(); err != nil {
		return nil, err
	}

	if err := a.initializeRolesAnywhereClient(); err != nil {
		return nil, err
	}

	err = a.createOrRefreshSession(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to refresh token for new session client")
	}

	return a.getTokenClient()
}

func (a *AWS) initializeTrustAnchors() error {
	var (
		trustAnchor arn.ARN
		profile     arn.ARN
		err         error
	)
	if a.x509Auth.TrustAnchorArn != nil {
		trustAnchor, err = arn.Parse(*a.x509Auth.TrustAnchorArn)
		if err != nil {
			return err
		}
		a.region = trustAnchor.Region
	}

	if a.x509Auth.TrustProfileArn != nil {
		profile, err = arn.Parse(*a.x509Auth.TrustProfileArn)
		if err != nil {
			return err
		}

		if profile.Region != "" && trustAnchor.Region != profile.Region {
			return fmt.Errorf("trust anchor and profile must be in the same region: trustAnchor=%s, profile=%s",
				trustAnchor.Region, profile.Region)
		}
	}
	return nil
}

func (a *AWS) initializeRolesAnywhereClient() error {
	if a.x509Auth.rolesAnywhereClient == nil {
		client := &http.Client{Transport: &http.Transport{
			TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
		}}
		mySession, err := session.NewSession()
		if err != nil {
			return err
		}
		config := aws.NewConfig().WithRegion(a.region).WithHTTPClient(client).WithLogLevel(aws.LogOff)
		rolesAnywhereClient := rolesanywhere.New(mySession, config)

		// Set up signing function and handlers
		if err := a.setSigningFunction(rolesAnywhereClient); err != nil {
			return err
		}
		a.x509Auth.rolesAnywhereClient = rolesAnywhereClient
	}
	return nil

}

func (a *AWS) setSigningFunction(rolesAnywhereClient *rolesanywhere.RolesAnywhere) error {
	certs, err := cryptopem.DecodePEMCertificatesChain(a.x509Auth.chainPEM)
	if err != nil {
		return err
	}

	var ints []x509.Certificate
	for i := range certs[1:] {
		ints = append(ints, *certs[i+1])
	}

	key, err := cryptopem.DecodePEMPrivateKey(a.x509Auth.keyPEM)
	if err != nil {
		return err
	}

	keyECDSA := key.(*ecdsa.PrivateKey)
	signFunc := awssh.CreateSignFunction(*keyECDSA, *certs[0], ints)

	agentHandlerFunc := request.MakeAddToUserAgentHandler("dapr", logger.DaprVersion, runtime.Version(), runtime.GOOS, runtime.GOARCH)
	rolesAnywhereClient.Handlers.Build.RemoveByName("core.SDKVersionUserAgentHandler")
	rolesAnywhereClient.Handlers.Build.PushBackNamed(request.NamedHandler{Name: "v4x509.CredHelperUserAgentHandler", Fn: agentHandlerFunc})
	rolesAnywhereClient.Handlers.Sign.Clear()
	rolesAnywhereClient.Handlers.Sign.PushBackNamed(request.NamedHandler{Name: "v4x509.SignRequestHandler", Fn: signFunc})

	return nil
}

func (a *AWS) createOrRefreshSession(ctx context.Context) error {
	var (
		duration             int64
		createSessionRequest rolesanywhere.CreateSessionInput
	)

	if *a.x509Auth.SessionDuration != 0 {
		duration = int64(a.x509Auth.SessionDuration.Seconds())

		createSessionRequest = rolesanywhere.CreateSessionInput{
			Cert:               ptr.Of(string(a.x509Auth.chainPEM)),
			ProfileArn:         a.x509Auth.TrustProfileArn,
			TrustAnchorArn:     a.x509Auth.TrustAnchorArn,
			RoleArn:            a.x509Auth.AssumeRoleArn,
			DurationSeconds:    aws.Int64(duration),
			InstanceProperties: nil,
			SessionName:        nil,
		}
	} else {
		duration = 900 // 15 minutes in seconds by default and be autorefreshed

		createSessionRequest = rolesanywhere.CreateSessionInput{
			Cert:               ptr.Of(string(a.x509Auth.chainPEM)),
			ProfileArn:         a.x509Auth.TrustProfileArn,
			TrustAnchorArn:     a.x509Auth.TrustAnchorArn,
			RoleArn:            a.x509Auth.AssumeRoleArn,
			DurationSeconds:    aws.Int64(duration),
			InstanceProperties: nil,
			SessionName:        nil,
		}
	}

	output, err := a.x509Auth.rolesAnywhereClient.CreateSessionWithContext(ctx, &createSessionRequest)
	if err != nil {
		return fmt.Errorf("failed to create session using dapr app identity: %w", err)
	}
	if len(output.CredentialSet) != 1 {
		return fmt.Errorf("expected 1 credential set from X.509 rolesanyway response, got %d", len(output.CredentialSet))
	}

	a.accessKey = *output.CredentialSet[0].Credentials.AccessKeyId
	a.secretKey = *output.CredentialSet[0].Credentials.SecretAccessKey
	a.sessionToken = *output.CredentialSet[0].Credentials.SessionToken

	// convert expiration time from *string to time.Time
	expirationStr := output.CredentialSet[0].Credentials.Expiration
	if expirationStr == nil {
		return fmt.Errorf("expiration time is nil")
	}

	expirationTime, err := time.Parse(time.RFC3339, *expirationStr)
	if err != nil {
		return fmt.Errorf("failed to parse expiration time: %w", err)
	}

	a.x509Auth.sessionExpiration = expirationTime

	return nil
}

func (a *AWS) startSessionRefresher(ctx context.Context) error {
	a.logger.Debugf("starting session refresher for x509 auth")
	// if there is a set session duration, then exit bc we will not auto refresh the session.
	if *a.x509Auth.SessionDuration != 0 {
		return nil
	}

	errChan := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(2 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				remaining := time.Until(a.x509Auth.sessionExpiration)
				if remaining <= 8*time.Minute {
					a.logger.Infof("Refreshing session as expiration is within %v", remaining)

					// Refresh the session
					err := a.createOrRefreshSession(ctx)
					if err != nil {
						errChan <- fmt.Errorf("failed to refresh session: %w", err)
						return
					}

					a.logger.Debugf("AWS IAM Roles Anywhere session refreshed successfully")
					refreshedSession, err := a.getTokenClient()
					if err != nil {
						errChan <- fmt.Errorf("failed to get token client with refreshed credentials: %v", err)
						return
					}
					a.x509Auth.sessionUpdateChannel <- refreshedSession

				}
			case <-ctx.Done():
				a.logger.Infof("Session refresher stopped due to context cancellation")
				errChan <- nil
				return
			}
		}
	}()

	select {
	case err := <-errChan:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *AWS) GetSessionUpdateChannel() chan *session.Session {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.x509Auth.sessionUpdateChannel
}
