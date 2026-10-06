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
	"bytes"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/hashicorp/vault/api"
	kubernetesauth "github.com/hashicorp/vault/api/auth/kubernetes"

	"github.com/dapr/kit/retry"
)

const (
	reauthInitialInterval = 5 * time.Second
	reauthMaxInterval     = 60 * time.Second

	defaultServiceAccountTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token" //nolint:gosec
)

// initKubernetesAuth logs in and starts the background renewal loop.
func (v *vaultSecretStore) initKubernetesAuth(ctx context.Context, client *api.Client, m *VaultMetadata) error {
	secret, err := v.kubernetesLogin(ctx, client, m)
	if err != nil {
		return fmt.Errorf("couldn't log in to vault using kubernetes auth: %w", err)
	}

	// Under mu, so Close can't run wg.Wait between the check and wg.Go.
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.bgCtx.Err() != nil {
		return nil
	}
	v.wg.Go(func() {
		v.renewalLoop(client, m, secret)
	})

	return nil
}

// kubernetesLogin re-reads the token file on every call: kubelet rotates it.
func (v *vaultSecretStore) kubernetesLogin(ctx context.Context, client *api.Client, m *VaultMetadata) (*api.Secret, error) {
	tokenPath := m.VaultServiceAccountTokenPath
	if tokenPath == "" {
		tokenPath = defaultServiceAccountTokenPath
	}
	jwt, err := os.ReadFile(tokenPath)
	if err != nil {
		return nil, fmt.Errorf("couldn't read service account token from %s: %w", tokenPath, err)
	}

	opts := []kubernetesauth.LoginOption{
		kubernetesauth.WithServiceAccountToken(string(bytes.TrimSpace(jwt))),
	}
	if m.VaultKubernetesMountPath != "" {
		opts = append(opts, kubernetesauth.WithMountPath(m.VaultKubernetesMountPath))
	}

	auth, err := kubernetesauth.NewKubernetesAuth(m.VaultKubernetesRole, opts...)
	if err != nil {
		return nil, fmt.Errorf("couldn't build kubernetes auth: %w", err)
	}

	return client.Auth().Login(ctx, auth)
}

// renewalLoop renews the token while it can and logs in again once it can't,
// until Close.
func (v *vaultSecretStore) renewalLoop(client *api.Client, m *VaultMetadata, secret *api.Secret) {
	for {
		cycleStart := time.Now()

		watcher, err := client.NewLifetimeWatcher(&api.LifetimeWatcherInput{Secret: secret})
		if err != nil {
			v.logger.Errorf("hashicorp vault: couldn't create lifetime watcher: %v", err)
		} else {
			// An in-flight renew-self can't be canceled, so Close doesn't wait
			// for Start; it returns on its own after Stop.
			go watcher.Start()
			v.watchOnce(watcher)
			watcher.Stop()
		}

		if v.bgCtx.Err() != nil {
			return
		}

		// Without a pause, a role issuing non-renewable or zero-TTL tokens
		// turns this into a tight login loop.
		if wait := reloginFloor(secret) - time.Since(cycleStart); wait > 0 {
			select {
			case <-v.bgCtx.Done():
				return
			case <-time.After(wait):
			}
		}

		secret, err = v.reauthenticate(client, m)
		if err != nil {
			return
		}
	}
}

// reloginFloor never exceeds half the lease, so a short-lived token is
// replaced before it expires.
func reloginFloor(secret *api.Secret) time.Duration {
	if secret == nil || secret.Auth == nil || secret.Auth.LeaseDuration <= 0 {
		return reauthInitialInterval
	}
	return min(reauthInitialInterval, time.Duration(secret.Auth.LeaseDuration)*time.Second/2)
}

func (v *vaultSecretStore) watchOnce(watcher *api.LifetimeWatcher) {
	for {
		select {
		case <-v.bgCtx.Done():
			return
		case renewal := <-watcher.RenewCh():
			if renewal.Secret != nil && renewal.Secret.Auth != nil {
				v.logger.Debugf("hashicorp vault: renewed token, lease duration %ds", renewal.Secret.Auth.LeaseDuration)
			}
		case err := <-watcher.DoneCh():
			if err != nil {
				v.logger.Warnf("hashicorp vault: token renewal stopped, re-authenticating: %v", err)
			}
			return
		}
	}
}

func (v *vaultSecretStore) reauthenticate(client *api.Client, m *VaultMetadata) (*api.Secret, error) {
	cfg := retry.DefaultConfig()
	cfg.Policy = retry.PolicyExponential
	cfg.InitialInterval = reauthInitialInterval
	cfg.MaxInterval = reauthMaxInterval
	cfg.MaxElapsedTime = 0

	return retry.NotifyRecoverWithData(
		func() (*api.Secret, error) {
			return v.kubernetesLogin(v.bgCtx, client, m)
		},
		cfg.NewBackOffWithContext(v.bgCtx),
		func(err error, d time.Duration) {
			v.logger.Warnf("hashicorp vault: kubernetes re-authentication failed, retrying in %s: %v", d, err)
		},
		func() {
			v.logger.Info("hashicorp vault: kubernetes re-authentication succeeded")
		},
	)
}
