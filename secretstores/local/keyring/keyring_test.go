/*
Copyright 2022 The Dapr Authors
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

package keyring

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/99designs/keyring"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
)

func TestInit(t *testing.T) {
	m := secretstores.Metadata{}
	s := keyringSecretStore{
		logger: logger.NewLogger("test"),
	}
	t.Run("Init with invalid backend", func(t *testing.T) {
		backendType := "test"
		m.Properties = map[string]string{
			"backendType": backendType,
		}
		err := s.Init(m)
		assert.Equal(t, err, fmt.Errorf("invalid keyring backend type %s", backendType))
	})

	t.Run("Init with backendType is keychain and serviceName is empty", func(t *testing.T) {
		m.Properties = map[string]string{
			"backendType": string(keyring.KeychainBackend),
			"serviceName": "",
		}
		err := s.Init(m)
		assert.Equal(t, err, errors.New("metadata serviceName is required for keychain backend"))
	})

	t.Run("Init with backendType is file and fileDir is empty", func(t *testing.T) {
		m.Properties = map[string]string{
			"backendType": string(keyring.FileBackend),
			"fileDir":     "",
		}
		err := s.Init(m)
		assert.Equal(t, err, errors.New("metadata fileDir is required for file backend"))
	})

	t.Run("Init with valid metadata", func(t *testing.T) {
		backendType := keyring.FileBackend
		m.Properties = map[string]string{
			"backendType": string(backendType),
			"fileDir":     "./",
		}
		err := s.Init(m)
		assert.Nil(t, err)
	})
}

func TestGetSecretAndBulkGetSecret(t *testing.T) {
	type fields struct {
		ring     keyring.Keyring
		teardown func(t *testing.T)
	}
	tests := []struct {
		name     string
		checkEnv func() error
		setup    func(t *testing.T) (keyring.Keyring, func(t *testing.T))
	}{
		{
			name:     "GetSecret and BulkGetSecret with backend is secret-service",
			checkEnv: checkSecretServiceEnv,
			setup:    secretServiceSetup,
		},
		{
			name:     "GetSecret and BulkGetSecret with backend is keychain",
			checkEnv: checkKeychainEnv,
			setup:    keychainSetup,
		},
		{
			name:     "GetSecret and BulkGetSecret with backend is keyctl",
			checkEnv: checkKeyctlEnv,
			setup:    keyctlSetup,
		},
		{
			name: "GetSecret and BulkGetSecret with backend is kwallet",
			checkEnv: func() error {
				return errors.New("kwallet testing is not currently implemented, TODO")
			},
			setup: func(t *testing.T) (keyring.Keyring, func(t *testing.T)) {
				return nil, func(t *testing.T) {}
			},
		},
		{
			name: "GetSecret and BulkGetSecret with backend is wincred",
			checkEnv: func() error {
				return checkWinCredEnv()
			},
			setup: winCredSetup,
		},
		{
			name: "GetSecret and BulkGetSecret with backend is pass",
			checkEnv: func() error {
				return checkPassEnv()
			},
			setup: passSetup,
		},
		{
			name: "GetSecret and BulkGetSecret with backend is file",
			checkEnv: func() error {
				return nil
			},
			setup: fileSetup,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.checkEnv(); err != nil {
				t.Skipf("Skipping testing because %s", err.Error())
			}

			ring, teardown := tt.setup(t)
			defer teardown(t)

			// mock data
			data := map[string]string{
				"k1": "v1",
				"k2": "v2",
			}
			for key, value := range data {
				err := ring.Set(keyring.Item{Key: key, Data: []byte(value)})
				assert.Nil(t, err)
			}

			s := &keyringSecretStore{
				ring:   ring,
				logger: logger.NewLogger("test"),
			}

			// GetSecret
			getSecretResp, err := s.GetSecret(secretstores.GetSecretRequest{Name: "k1"})
			assert.Nil(t, err)
			assert.Equal(t, getSecretResp.Data["k1"], "v1")

			// BulkGetSecret
			bulkGetSecretResp, err := s.BulkGetSecret(secretstores.BulkGetSecretRequest{})
			assert.Nil(t, err)
			assert.Equal(t, bulkGetSecretResp.Data, map[string]map[string]string{
				"k1": {"k1": "v1"},
				"k2": {"k2": "v2"},
			})
		})
	}
}

func secretServiceSetup(t *testing.T) (keyring.Keyring, func(t *testing.T)) {
	t.Helper()

	ring, err := keyring.Open(keyring.Config{
		AllowedBackends: []keyring.BackendType{keyring.SecretServiceBackend},
		ServiceName:     "keyring-test",
	})
	assert.Nil(t, err)

	return ring, func(t *testing.T) {
		t.Helper()
	}
}

func keychainSetup(t *testing.T) (keyring.Keyring, func(t *testing.T)) {
	t.Helper()

	tempDir := path.Join(os.TempDir(), "/dapr-secretstore-local-keyring-keychain")
	tempFilename := path.Join(tempDir, fmt.Sprintf("keyring-test-%d.keychain", time.Now().UnixNano()))

	ring, err := keyring.Open(keyring.Config{
		AllowedBackends:          []keyring.BackendType{keyring.KeychainBackend},
		KeychainPasswordFunc:     keyring.FixedStringPrompt("test password"),
		ServiceName:              "test",
		KeychainTrustApplication: true,
	})
	assert.Nil(t, err)

	return ring, func(t *testing.T) {
		t.Helper()

		if _, err := os.Stat(tempFilename); os.IsExist(err) {
			err = os.Remove(tempFilename)
			assert.Nil(t, err)
		}

		// Sierra introduced a -db suffix
		dbPath := tempFilename + "-db"
		if _, err := os.Stat(dbPath); os.IsExist(err) {
			err = os.Remove(dbPath)
			assert.Nil(t, err)
		}
	}
}

func keyctlSetup(t *testing.T) (keyring.Keyring, func(t *testing.T)) {
	t.Helper()

	ring, err := keyring.Open(keyring.Config{
		AllowedBackends: []keyring.BackendType{keyring.KeyCtlBackend},
		KeyCtlScope:     "user",
		KeyCtlPerm:      0x3f3f0000, // "alswrvalswrv------------"
	})
	assert.Nil(t, err)

	return ring, func(t *testing.T) {
		t.Helper()
	}
}

func winCredSetup(t *testing.T) (keyring.Keyring, func(t *testing.T)) {
	t.Helper()

	ring, err := keyring.Open(keyring.Config{
		AllowedBackends: []keyring.BackendType{keyring.WinCredBackend},
	})
	assert.Nil(t, err)

	return ring, func(t *testing.T) {
		t.Helper()
	}
}

func passSetup(t *testing.T) (keyring.Keyring, func(t *testing.T)) {
	t.Helper()

	pwd, err := os.Getwd()
	assert.Nil(t, err)

	// the default temp directory can't be used because gpg-agent complains with "socket name too long"
	tempDir, err := os.MkdirTemp("/tmp", "dapr-secretstore-local-keyring-pass")
	assert.Nil(t, err)

	gnupghome := filepath.Join(tempDir, ".gnupg")
	err = os.Mkdir(gnupghome, os.FileMode(int(0o700)))
	assert.Nil(t, err)

	passDir := filepath.Join(tempDir, ".password-store")
	passCmd := "pass"

	os.Setenv("GNUPGHOME", gnupghome)
	os.Setenv("PASSWORD_STORE_DIR", passDir)
	os.Unsetenv("GPG_AGENT_INFO")
	os.Unsetenv("GPG_TTY")
	runCmd(t, "gpg", "--import", filepath.Join(pwd, "testdata", "test-gpg.key"))
	runCmd(t, "gpg", "--import-ownertrust", filepath.Join(pwd, "testdata", "test-ownertrust-gpg.txt"))
	runCmd(t, passCmd, "init", "test@example.com")

	ring, err := keyring.Open(keyring.Config{
		AllowedBackends: []keyring.BackendType{keyring.PassBackend},
		PassCmd:         passCmd,
		PassDir:         passDir,
	})
	assert.Nil(t, err)

	return ring, func(t *testing.T) {
		t.Helper()

		os.RemoveAll(tempDir)
	}
}

func fileSetup(t *testing.T) (keyring.Keyring, func(t *testing.T)) {
	t.Helper()

	tempDir := path.Join(os.TempDir(), "/dapr-secretstore-local-keyring-file")
	ring, err := keyring.Open(keyring.Config{
		AllowedBackends:  []keyring.BackendType{keyring.FileBackend},
		FilePasswordFunc: func(s string) (string, error) { return "", nil },
		FileDir:          tempDir,
	})
	assert.Nil(t, err)

	return ring, func(t *testing.T) {
		t.Helper()

		err = os.RemoveAll(tempDir)
		assert.Nil(t, err)
	}
}

func runCmd(t *testing.T, cmds ...string) {
	t.Helper()

	if len(cmds) == 0 {
		return
	}

	if len(cmds) == 1 {
		err := exec.Command(cmds[0]).Run()
		assert.Nil(t, err)
	} else {
		err := exec.Command(cmds[0], cmds[1:]...).Run()
		assert.Nil(t, err)
	}
}

func cmdExists(cmd string) error {
	_, err := exec.LookPath(cmd)
	return err
}

// Copy from github.com/99designs/keyring secretservice_test.go
//
// NOTE: These tests are not runnable from a headless environment such as
// Docker or a CI pipeline due to the DBus "prompt" interface being called
// by the underlying go-libsecret when creating and unlocking a keychain.
//
// TODO: Investigate a way to automate the prompting. Some ideas:
//
//  1. I've looked extensively but have not found a headless CLI tool that
//     could be run in the background of eg: a docker container
//  2. It might be possible to make a mock prompter that connects to DBus
//     and provides the Prompt interface using the go-libsecret library.
func checkSecretServiceEnv() error {
	if os.Getenv("GITHUB_ACTIONS") != "" {
		return errors.New("secretservice can't testing in CI environment")
	}
	if runtime.GOOS != "linux" {
		return errors.New("current os isn't Linux")
	}
	return nil
}

func checkKeychainEnv() error {
	if runtime.GOOS != "darwin" {
		return errors.New("current os isn't macOS")
	}
	return nil
}

func checkKeyctlEnv() error {
	if runtime.GOOS != "linux" {
		return errors.New("current os isn't Linux")
	}
	return nil
}

func checkWinCredEnv() error {
	if runtime.GOOS != "windows" {
		return errors.New("current os isn't Windows")
	}
	return nil
}

func checkPassEnv() error {
	var err error

	defer func() {
		if err != nil {
			err = errors.New("current environment doesn't have pass or gpg command")
		}
	}()

	err = cmdExists("pass")
	if err != nil {
		return err
	}
	return cmdExists("gpg")
}
