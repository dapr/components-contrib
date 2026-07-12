/*
Copyright 2026 The Dapr Authors
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

package vault

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/hashicorp/vault/api"
	kubernetesauth "github.com/hashicorp/vault/api/auth/kubernetes"
)

const (
	reauthInitialInterval = 5 * time.Second
	reauthMaxInterval     = 60 * time.Second
)

// initKubernetesAuth performs a blocking first login using the Kubernetes
// auth method, then starts a background goroutine that keeps the token
// renewed/re-authenticated for the lifetime of the component.
func (v *vaultSecretStore) initKubernetesAuth(ctx context.Context, client *api.Client, m *VaultMetadata) error {
	secret, err := v.kubernetesLogin(ctx, client, m)
	if err != nil {
		return fmt.Errorf("couldn't log in to vault using kubernetes auth: %w", err)
	}

	v.wg.Add(1)
	go v.renewalLoop(client, m, secret)

	return nil
}

// kubernetesLogin builds a fresh KubernetesAuth instance (which re-reads the
// service account token file) and performs a single login. Must be called
// fresh on every (re-)authentication attempt: KubernetesAuth caches the JWT
// read at construction time and never re-reads it, so reusing an instance
// across retries would authenticate with a stale/expired token.
func (v *vaultSecretStore) kubernetesLogin(ctx context.Context, client *api.Client, m *VaultMetadata) (*api.Secret, error) {
	var opts []kubernetesauth.LoginOption
	if m.VaultKubernetesMountPath != "" {
		opts = append(opts, kubernetesauth.WithMountPath(m.VaultKubernetesMountPath))
	}
	if m.VaultServiceAccountTokenPath != "" {
		opts = append(opts, kubernetesauth.WithServiceAccountTokenPath(m.VaultServiceAccountTokenPath))
	}

	auth, err := kubernetesauth.NewKubernetesAuth(m.VaultKubernetesRole, opts...)
	if err != nil {
		return nil, fmt.Errorf("couldn't build kubernetes auth: %w", err)
	}

	return client.Auth().Login(ctx, auth)
}

// renewalLoop keeps the Vault token alive for as long as the component is
// running: it watches the current login's lease via a LifetimeWatcher, and
// once that lease can no longer be renewed, re-authenticates from scratch
// and starts watching the new lease. It returns once Close() is called.
func (v *vaultSecretStore) renewalLoop(client *api.Client, m *VaultMetadata, secret *api.Secret) {
	defer v.wg.Done()

	for {
		watcher, err := client.NewLifetimeWatcher(&api.LifetimeWatcherInput{Secret: secret})
		if err != nil {
			v.logger.Errorf("hashicorp vault: couldn't create lifetime watcher: %v", err)
		} else {
			go watcher.Start()
			v.watchOnce(watcher)
			watcher.Stop()
		}

		select {
		case <-v.closeCh:
			return
		default:
		}

		var loginErr error
		secret, loginErr = v.reauthenticate(client, m)
		if loginErr != nil {
			// closeCh fired while backoff.Retry was in progress.
			return
		}
	}
}

// watchOnce blocks until the watcher signals it's done renewing (lease
// expired or renewal failed) or the component is closing.
func (v *vaultSecretStore) watchOnce(watcher *api.LifetimeWatcher) {
	for {
		select {
		case <-v.closeCh:
			return
		case renewal := <-watcher.RenewCh():
			v.logger.Debugf("hashicorp vault: successfully renewed token, lease duration %d", renewal.Secret.LeaseDuration)
		case <-watcher.DoneCh():
			return
		}
	}
}

// reauthenticate retries the Kubernetes login with exponential backoff until
// it succeeds or the component is closed.
func (v *vaultSecretStore) reauthenticate(client *api.Client, m *VaultMetadata) (*api.Secret, error) {
	exp := backoff.NewExponentialBackOff()
	exp.InitialInterval = reauthInitialInterval
	exp.MaxInterval = reauthMaxInterval
	exp.MaxElapsedTime = 0 // retry indefinitely until Close()
	ctxBackoff := backoff.WithContext(exp, v.bgCtx)

	var secret *api.Secret
	op := func() error {
		s, err := v.kubernetesLogin(v.bgCtx, client, m)
		if err != nil {
			v.logger.Warnf("hashicorp vault: kubernetes re-authentication failed, retrying: %v", err)
			return err
		}
		secret = s
		return nil
	}

	if err := backoff.Retry(op, ctxBackoff); err != nil {
		return nil, err
	}

	return secret, nil
}
