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
)

type Client struct {
	sshClient         *ssh.Client
	sftpClient        *sftpClient.Client
	address           string
	config            *ssh.ClientConfig
	lock              sync.RWMutex
	reconnectionCount atomic.Uint64
}

type sftpError struct {
	err               error
	reconnectionCount uint64
}

func (s sftpError) Error() string {
	return s.err.Error()
}

func newClient(address string, config *ssh.ClientConfig) (*Client, error) {
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
	}, nil
}

func (c *Client) Close() error {
	_ = c.sshClient.Close()
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.sftpClient.Close()
}

func (c *Client) list(path string) ([]os.FileInfo, error) {
	var fi []os.FileInfo

	fn := func() *sftpError {
		var err error
		c.lock.RLock()
		defer c.lock.RUnlock()
		fi, err = c.sftpClient.ReadDir(path)
		if err != nil {
			return &sftpError{err: err, reconnectionCount: c.reconnectionCount.Load()}
		}
		return nil
	}

	err := withReconnection(c, fn)
	if err != nil {
		return nil, err
	}

	return fi, nil
}

func (c *Client) create(path string) (*sftpClient.File, string, error) {
	dir, fileName := sftpClient.Split(path)

	var file *sftpClient.File

	createFn := func() *sftpError {
		c.lock.RLock()
		defer c.lock.RUnlock()
		cErr := c.sftpClient.MkdirAll(dir)
		if cErr != nil {
			return &sftpError{err: fmt.Errorf("sftp binding error: error create dir %s: %w", dir, cErr), reconnectionCount: c.reconnectionCount.Load()}
		}

		file, cErr = c.sftpClient.Create(path)
		if cErr != nil {
			return &sftpError{err: fmt.Errorf("sftp binding error: error create file %s: %w", path, cErr), reconnectionCount: c.reconnectionCount.Load()}
		}

		return nil
	}

	rErr := withReconnection(c, createFn)
	if rErr != nil {
		return nil, "", rErr
	}

	return file, fileName, nil
}

func (c *Client) get(path string) (*sftpClient.File, error) {
	var f *sftpClient.File

	fn := func() *sftpError {
		var err error
		c.lock.RLock()

		defer c.lock.RUnlock()
		f, err = c.sftpClient.Open(path)
		if err != nil {
			return &sftpError{err: err, reconnectionCount: c.reconnectionCount.Load()}
		}
		return nil
	}

	err := withReconnection(c, fn)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func (c *Client) delete(path string) error {
	fn := func() *sftpError {
		var err error
		c.lock.RLock()
		defer c.lock.RUnlock()
		err = c.sftpClient.Remove(path)
		if err != nil {
			return &sftpError{err: err, reconnectionCount: c.reconnectionCount.Load()}
		}
		return nil
	}

	err := withReconnection(c, fn)
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) ping() error {
	c.lock.RLock()
	defer c.lock.RUnlock()
	_, err := c.sftpClient.Getwd()
	if err != nil {
		return err
	}
	return nil
}

func withReconnection(c *Client, fn func() *sftpError) error {
	err := fn()
	if err == nil {
		return nil
	}

	if !shouldReconnect(err) {
		return err
	}

	rErr := doReconnect(c, err.reconnectionCount)
	if rErr != nil {
		return errors.Join(err, rErr)
	}

	err = fn()
	if err != nil {
		return err
	}

	return nil
}

func doReconnect(c *Client, reconnectionCount uint64) error {
	// No need to reconnect as it has been reconnected
	if reconnectionCount != c.reconnectionCount.Load() {
		return nil
	}

	err := c.ping()
	if err == nil {
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

	// Swap under short lock; close old clients after unlocking.
	// Close new clients if not swapped
	c.lock.Lock()
	var oldSftp *sftpClient.Client
	var oldSSH *ssh.Client
	if reconnectionCount == c.reconnectionCount.Load() {
		oldSftp = c.sftpClient
		oldSSH = c.sshClient
		c.sftpClient = newSftpClient
		c.sshClient = sshClient
		c.reconnectionCount.Add(1)
		sshClient = nil
		newSftpClient = nil
	}
	c.lock.Unlock()

	if oldSftp != nil {
		_ = oldSftp.Close()
	}
	if oldSSH != nil {
		_ = oldSSH.Close()
	}

	if newSftpClient != nil {
		_ = newSftpClient.Close()
	}

	if sshClient != nil {
		_ = sshClient.Close()
	}

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
func shouldReconnect(err error) bool {
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
