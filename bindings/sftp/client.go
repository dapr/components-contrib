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
	"os"
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
}

func newClient(address string, config *ssh.ClientConfig, log logger.Logger) (*Client, error) {
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
		sshClient:  sshClient,
		sftpClient: newSftpClient,
		address:    address,
		config:     config,
		log:        log,
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

func (c *Client) create(path string) (*sftpClient.File, string, error) {
	dir, fileName := sftpClient.Split(path)

	var file *sftpClient.File

	createFn := func() error {
		cErr := c.sftpClient.MkdirAll(dir)
		if cErr != nil {
			return cErr
		}

		file, cErr = c.sftpClient.Create(path)
		if cErr != nil {
			return cErr
		}

		return nil
	}

	rErr := c.withReconnection(createFn)
	if rErr != nil {
		return nil, "", rErr
	}

	return file, fileName, nil
}

func (c *Client) get(path string) (*sftpClient.File, error) {
	var f *sftpClient.File

	fn := func() error {
		var err error
		f, err = c.sftpClient.Open(path)
		return err
	}

	err := c.withReconnection(fn)
	if err != nil {
		return nil, err
	}

	return f, nil
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
	c.lock.RLock()
	defer c.lock.RUnlock()
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

	// SFTP status errors that are logical, not connectivity (avoid reconnect)
	if errors.Is(err, sftpClient.ErrSSHFxPermissionDenied) ||
		errors.Is(err, sftpClient.ErrSSHFxNoSuchFile) ||
		errors.Is(err, sftpClient.ErrSSHFxOpUnsupported) {
		return false
	}

	return true
}
