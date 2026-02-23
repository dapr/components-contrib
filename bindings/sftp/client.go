/*
Copyright 2025 The Dapr Authors
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

package sftp

import (
	"errors"
	"fmt"
	"io"
	"os"
	sysPath "path"
	"strings"
	"sync"
	"sync/atomic"

	sftpClient "github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	"github.com/dapr/kit/logger"
)

type Client struct {
	sshClient      *ssh.Client
	sftpClient     *sftpClient.Client
	address        string
	config         *ssh.ClientConfig
	lock           sync.RWMutex
	needsReconnect atomic.Bool
	log            logger.Logger
	sequentialMode bool
}

func newClient(address string, config *ssh.ClientConfig, sequentialMode bool, log logger.Logger) (*Client, error) {
	if address == "" || config == nil {
		return nil, errors.New("sftp binding error: client not initialized")
	}

	sshClient, err := newSSHClient(address, config)
	if err != nil {
		return nil, err
	}

	newSftpClient, err := sftpClient.NewClient(sshClient)
	if err != nil {
		_ = sshClient.Close()
		return nil, fmt.Errorf("sftp binding error: error create sftp client: %w", err)
	}

	return &Client{
		sshClient:      sshClient,
		sftpClient:     newSftpClient,
		address:        address,
		config:         config,
		log:            log,
		sequentialMode: sequentialMode,
	}, nil
}

func (c *Client) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Close SFTP first, then SSH
	var sftpErr, sshErr error
	if c.sftpClient != nil {
		sftpErr = c.sftpClient.Close()
	}
	if c.sshClient != nil {
		sshErr = c.sshClient.Close()
	}

	// Return the first error encountered
	if sftpErr != nil {
		return sftpErr
	}
	return sshErr
}

func (c *Client) list(path string) ([]os.FileInfo, error) {
	var fi []os.FileInfo

	fn := func() error {
		var err error
		fi, err = c.sftpClient.ReadDir(path)
		return err
	}

	err := c.withReconnection(fn)
	if err != nil {
		return nil, err
	}

	return fi, nil
}

func (c *Client) create(data []byte, path string) (string, error) {
	dir, fileName := sftpClient.Split(path)

	createFn := func() error {
		// Only create directory if it doesn't already exist
		// This prevents "not a directory" errors on strict SFTP servers like Axway MFT
		dir = sysPath.Clean(dir)
		if dir != "." {
			if _, statErr := c.sftpClient.Stat(dir); statErr != nil {
				// Directory doesn't exist, create it
				if isDirNotExistError(statErr) {
					if mkdirErr := c.sftpClient.MkdirAll(dir); mkdirErr != nil {
						return fmt.Errorf("error create dir %s: %w", dir, mkdirErr)
					}
				} else {
					return fmt.Errorf("error checking dir %s: %w", dir, statErr)
				}
			}
		}

		file, cErr := c.sftpClient.Create(path)
		if cErr != nil {
			return fmt.Errorf("error create file %s: %w", path, cErr)
		}
		defer file.Close()

		_, wErr := file.Write(data)
		if wErr != nil {
			return fmt.Errorf("error write file %s: %w", path, wErr)
		}

		return nil
	}

	rErr := c.withReconnection(createFn)
	if rErr != nil {
		return "", rErr
	}

	return fileName, nil
}

func (c *Client) get(path string) ([]byte, error) {
	var data []byte

	fn := func() error {
		f, err := c.sftpClient.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()

		data, err = io.ReadAll(f)
		return err
	}

	err := c.withReconnection(fn)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (c *Client) delete(path string) error {
	fn := func() error {
		return c.sftpClient.Remove(path)
	}

	err := c.withReconnection(fn)
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) ping() error {
	_, err := c.sftpClient.Getwd()
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) withReconnection(fn func() error) error {
	err := c.do(fn)
	if !c.shouldReconnect(err) {
		return err
	}

	c.log.Debugf("sftp binding error: %s", err)
	c.needsReconnect.Store(true)

	rErr := c.doReconnect()
	if rErr != nil {
		c.log.Debugf("sftp binding error: reconnect failed: %s", rErr)
		return errors.Join(err, rErr)
	}
	c.log.Debugf("sftp binding: reconnected to %s", c.address)

	c.log.Debugf("sftp binding: retrying operation")
	return c.do(fn)
}

func (c *Client) do(fn func() error) error {
	if c.sequentialMode {
		c.lock.Lock()
		defer c.lock.Unlock()
	} else {
		c.lock.RLock()
		defer c.lock.RUnlock()
	}

	return fn()
}

func (c *Client) doReconnect() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.log.Debugf("sftp binding: reconnecting to %s", c.address)

	if !c.needsReconnect.Load() {
		return nil
	}

	pErr := c.ping()
	if pErr == nil {
		c.needsReconnect.Store(false)
		return nil
	}

	sshClient, err := newSSHClient(c.address, c.config)
	if err != nil {
		return err
	}

	newSftpClient, err := sftpClient.NewClient(sshClient)
	if err != nil {
		_ = sshClient.Close()
		return fmt.Errorf("sftp binding error: error create sftp client: %w", err)
	}

	if c.sftpClient != nil {
		_ = c.sftpClient.Close()
	}
	if c.sshClient != nil {
		_ = c.sshClient.Close()
	}

	c.sftpClient = newSftpClient
	c.sshClient = sshClient

	c.needsReconnect.Store(false)
	return nil
}

func newSSHClient(address string, config *ssh.ClientConfig) (*ssh.Client, error) {
	sshClient, err := ssh.Dial("tcp", address, config)
	if err != nil {
		return nil, fmt.Errorf("sftp binding error: error dialing ssh server: %w", err)
	}
	return sshClient, nil
}

// shouldReconnect returns true if the error looks like a transport-level failure
func (c *Client) shouldReconnect(err error) bool {
	if err == nil {
		return false
	}

	// Check for StatusError using errors.As - if it's a StatusError,
	// it's an SFTP protocol error, not a connection issue
	var statusErr *sftpClient.StatusError
	if errors.As(err, &statusErr) {
		// Any StatusError is a protocol-level response from the server,
		// meaning the connection is working fine
		return false
	}

	// Check sentinel errors for SFTP logical errors
	// These errors indicate application-level issues, not connection problems
	if errors.Is(err, sftpClient.ErrSSHFxPermissionDenied) ||
		errors.Is(err, sftpClient.ErrSSHFxNoSuchFile) ||
		errors.Is(err, sftpClient.ErrSSHFxOpUnsupported) ||
		errors.Is(err, sftpClient.ErrSSHFxFailure) ||
		errors.Is(err, sftpClient.ErrSSHFxBadMessage) ||
		errors.Is(err, sftpClient.ErrSSHFxEOF) {
		return false
	}

	// Fallback: string matching for wrapped errors that may not implement Is/As
	errStr := strings.ToLower(err.Error())
	if strings.Contains(errStr, "permission denied") ||
		strings.Contains(errStr, "no such file") ||
		strings.Contains(errStr, "not a directory") ||
		strings.Contains(errStr, "file exists") ||
		strings.Contains(errStr, "bad message") {
		return false
	}

	return true
}

func isDirNotExistError(err error) bool {
	if err == nil {
		return false
	}

	return errors.Is(err, sftpClient.ErrSSHFxNoSuchFile) ||
		strings.Contains(strings.ToLower(err.Error()), "file does not exist")
}
