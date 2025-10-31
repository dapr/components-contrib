package sftp

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	sftpClient "github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type Client struct {
	sshClient  *ssh.Client
	sftpClient *sftpClient.Client
	address    string
	config     *ssh.ClientConfig
	lock       sync.RWMutex
	rLock      sync.Mutex
}

func newClient(address string, config *ssh.ClientConfig) (*Client, error) {
	if address == "" || config == nil {
		return nil, errors.New("sftp binding error: client not initialized")
	}

	sshClient, err := ssh.Dial("tcp", address, config)
	if err != nil {
		return nil, fmt.Errorf("sftp binding error: error create ssh client: %w", err)
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

	fn := func() error {
		var err error
		c.lock.RLock()
		defer c.lock.RUnlock()
		fi, err = c.sftpClient.ReadDir(path)
		return err
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

	createFn := func() error {
		c.lock.RLock()
		defer c.lock.RUnlock()
		cErr := c.sftpClient.MkdirAll(dir)
		if cErr != nil {
			return fmt.Errorf("sftp binding error: error create dir %s: %w", dir, cErr)
		}

		file, cErr = c.sftpClient.Create(path)
		if cErr != nil {
			return fmt.Errorf("sftp binding error: error create file %s: %w", path, cErr)
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

	fn := func() error {
		var err error
		c.lock.RLock()
		defer c.lock.RUnlock()
		f, err = c.sftpClient.Open(path)
		return err
	}

	err := withReconnection(c, fn)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func (c *Client) delete(path string) error {
	fn := func() error {
		var err error
		c.lock.RLock()
		defer c.lock.RUnlock()
		err = c.sftpClient.Remove(path)
		return err
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

func withReconnection(c *Client, fn func() error) error {
	err := fn()
	if err == nil {
		return nil
	}

	if !shouldReconnect(err) {
		return err
	}

	rErr := doReconnect(c)
	if rErr != nil {
		return errors.Join(err, rErr)
	}

	err = fn()
	if err != nil {
		return err
	}

	return nil
}

func doReconnect(c *Client) error {
	c.rLock.Lock()
	defer c.rLock.Unlock()

	err := c.ping()
	if !shouldReconnect(err) {
		return nil
	}

	sshClient, err := ssh.Dial("tcp", c.address, c.config)
	if err != nil {
		return fmt.Errorf("sftp binding error: error create ssh client: %w", err)
	}

	newSftpClient, err := sftpClient.NewClient(sshClient)
	if err != nil {
		_ = sshClient.Close()
		return fmt.Errorf("sftp binding error: error create sftp client: %w", err)
	}

	// Swap under short lock; close old clients after unlocking.
	c.lock.Lock()
	oldSftp := c.sftpClient
	oldSSH := c.sshClient
	c.sftpClient = newSftpClient
	c.sshClient = sshClient
	c.lock.Unlock()

	if oldSftp != nil {
		_ = oldSftp.Close()
	}
	if oldSSH != nil {
		_ = oldSSH.Close()
	}

	return nil
}

// shouldReconnect returns true if the error looks like a transport-level failure
func shouldReconnect(err error) bool {
	if err == nil {
		return false
	}

	// Network/timeout conditions
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}

	// Common wrapped network error messages
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "use of closed network connection"),
		strings.Contains(msg, "connection reset by peer"),
		strings.Contains(msg, "broken pipe"),
		strings.Contains(msg, "connection refused"),
		strings.Contains(msg, "network is unreachable"),
		strings.Contains(msg, "no such host"):
		return true
	}

	// SFTP status errors that are logical, not connectivity (avoid reconnect)
	if errors.Is(err, sftpClient.ErrSSHFxPermissionDenied) ||
		errors.Is(err, sftpClient.ErrSSHFxNoSuchFile) ||
		errors.Is(err, sftpClient.ErrSSHFxOpUnsupported) {
		return false
	}

	return true
}
