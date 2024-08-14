package sftp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"

	sftpClient "github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

const (
	metadataRootPath = "rootPath"
	metadataFileName = "fileName"
)

// Sftp is a binding for file operations on sftp server.
type Sftp struct {
	metadata   *sftpMetadata
	logger     logger.Logger
	sftpClient *sftpClient.Client
}

// sftpMetadata defines the sftp metadata.
type sftpMetadata struct {
	RootPath              string `json:"rootPath"`
	Address               string `json:"address"`
	Username              string `json:"username"`
	Password              string `json:"password"`
	PrivateKey            []byte `json:"privateKey"`
	PrivateKeyPassphrase  []byte `json:"privateKeyPassphrase"`
	HostPublicKey         []byte `json:"hostPublicKey"`
	KnownHostsFile        string `json:"knownHostsFile"`
	InsecureIgnoreHostKey bool   `json:"insecureIgnoreHostKey"`
}

type createResponse struct {
	FileName string `json:"fileName"`
}

type listResponse struct {
	FileName    string `json:"fileName"`
	IsDirectory bool   `json:"isDirectory"`
}

func NewSftp(logger logger.Logger) bindings.OutputBinding {
	return &Sftp{logger: logger}
}

func (sftp *Sftp) Init(_ context.Context, metadata bindings.Metadata) error {
	m, err := sftp.parseMetadata(metadata)
	if err != nil {
		return fmt.Errorf("failed to parse metadata: %w", err)
	}

	var auth []ssh.AuthMethod
	var hostKeyCallback ssh.HostKeyCallback

	if m.InsecureIgnoreHostKey {
		//nolint:gosec
		hostKeyCallback = ssh.InsecureIgnoreHostKey()
	} else if len(m.KnownHostsFile) > 0 {
		hostKeyCallback, err = knownhosts.New(m.KnownHostsFile)
		if err != nil {
			return fmt.Errorf("sftp binding error: read known host file error: %w", err)
		}
	} else if len(m.HostPublicKey) > 0 {
		var hostPublicKey ssh.PublicKey
		hostPublicKey, _, _, _, err = ssh.ParseAuthorizedKey(m.HostPublicKey)
		if err != nil {
			return fmt.Errorf("sftp binding error: parse host public key error: %w", err)
		}

		hostKeyCallback = ssh.FixedHostKey(hostPublicKey)
	}

	if hostKeyCallback == nil {
		return errors.New("sftp binding error: no host validation method provided")
	}

	if len(m.PrivateKey) > 0 {
		var signer ssh.Signer

		if len(m.PrivateKeyPassphrase) > 0 {
			signer, err = ssh.ParsePrivateKeyWithPassphrase(m.PrivateKey, m.PrivateKeyPassphrase)
			if err != nil {
				return fmt.Errorf("sftp binding error: parse private key error: %w", err)
			}
		} else {
			signer, err = ssh.ParsePrivateKey(m.PrivateKey)
			if err != nil {
				return fmt.Errorf("sftp binding error: parse private key error: %w", err)
			}
		}

		auth = append(auth, ssh.PublicKeys(signer))
	}

	if len(m.Password) > 0 {
		auth = append(auth, ssh.Password(m.Password))
	}

	config := &ssh.ClientConfig{
		User:            m.Username,
		Auth:            auth,
		HostKeyCallback: hostKeyCallback,
	}

	sshClient, err := ssh.Dial("tcp", m.Address, config)
	if err != nil {
		return fmt.Errorf("sftp binding error: error create ssh client: %w", err)
	}

	newSftpClient, err := sftpClient.NewClient(sshClient)
	if err != nil {
		return fmt.Errorf("sftp binding error: error create sftp client: %w", err)
	}

	sftp.metadata = m
	sftp.sftpClient = newSftpClient

	return nil
}

func (sftp *Sftp) parseMetadata(meta bindings.Metadata) (*sftpMetadata, error) {
	var m sftpMetadata
	err := kitmd.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

func (sftp *Sftp) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		bindings.CreateOperation,
		bindings.GetOperation,
		bindings.DeleteOperation,
		bindings.ListOperation,
	}
}

func (sftp *Sftp) create(_ context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	metadata, err := sftp.metadata.mergeWithRequestMetadata(req)
	if err != nil {
		return nil, fmt.Errorf("sftp binding error: error merging metadata: %w", err)
	}

	path, err := metadata.getPath(req.Metadata)
	if err != nil {
		return nil, fmt.Errorf("sftp binding error: %w", err)
	}

	dir, fileName := sftpClient.Split(path)

	err = sftp.sftpClient.MkdirAll(dir)
	if err != nil {
		return nil, fmt.Errorf("sftp binding error: error create dir %s: %w", dir, err)
	}

	file, err := sftp.sftpClient.Create(path)
	if err != nil {
		return nil, fmt.Errorf("sftp binding error: error create file %s: %w", path, err)
	}

	_, err = file.Write(req.Data)
	if err != nil {
		return nil, fmt.Errorf("sftp binding error: error write file: %w", err)
	}

	jsonResponse, err := json.Marshal(createResponse{
		FileName: fileName,
	})
	if err != nil {
		return nil, fmt.Errorf("sftp binding error: error marshalling create response: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: jsonResponse,
		Metadata: map[string]string{
			metadataFileName: fileName,
		},
	}, nil
}

func (sftp *Sftp) list(_ context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	metadata, err := sftp.metadata.mergeWithRequestMetadata(req)
	if err != nil {
		return nil, fmt.Errorf("sftp binding error: error merging metadata: %w", err)
	}

	path, err := metadata.getPath(req.Metadata)
	if err != nil {
		return nil, fmt.Errorf("sftp binding error: %w", err)
	}

	files, err := sftp.sftpClient.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("sftp binding error: error read dir %s: %w", path, err)
	}

	resp := make([]listResponse, len(files))

	for i, file := range files {
		resp[i] = listResponse{
			FileName:    file.Name(),
			IsDirectory: file.IsDir(),
		}
	}

	jsonResponse, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("sftp binding error: cannot marshal list to json: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: jsonResponse,
	}, nil
}

func (sftp *Sftp) get(_ context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	metadata, err := sftp.metadata.mergeWithRequestMetadata(req)
	if err != nil {
		return nil, fmt.Errorf("sftp binding error: error merging metadata: %w", err)
	}

	path, err := metadata.getPath(req.Metadata)
	if err != nil {
		return nil, fmt.Errorf("sftp binding error: %w", err)
	}

	file, err := sftp.sftpClient.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sftp binding error: error open file %s: %w", path, err)
	}

	b, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("sftp binding error: error read file %s: %w", path, err)
	}

	return &bindings.InvokeResponse{
		Data: b,
	}, nil
}

func (sftp *Sftp) delete(_ context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	metadata, err := sftp.metadata.mergeWithRequestMetadata(req)
	if err != nil {
		return nil, fmt.Errorf("sftp binding error: error merging metadata: %w", err)
	}

	path, err := metadata.getPath(req.Metadata)
	if err != nil {
		return nil, fmt.Errorf("sftp binding error: %w", err)
	}

	err = sftp.sftpClient.Remove(path)
	if err != nil {
		return nil, fmt.Errorf("sftp binding error: error remove file %s: %w", path, err)
	}

	return nil, nil
}

func (sftp *Sftp) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	switch req.Operation {
	case bindings.CreateOperation:
		return sftp.create(ctx, req)
	case bindings.GetOperation:
		return sftp.get(ctx, req)
	case bindings.DeleteOperation:
		return sftp.delete(ctx, req)
	case bindings.ListOperation:
		return sftp.list(ctx, req)
	default:
		return nil, fmt.Errorf("unsupported operation %s", req.Operation)
	}
}

func (sftp *Sftp) Close() error {
	return sftp.sftpClient.Close()
}

func (metadata sftpMetadata) getPath(requestMetadata map[string]string) (path string, err error) {
	if val, ok := kitmd.GetMetadataProperty(requestMetadata, metadataFileName); ok && val != "" {
		path = sftpClient.Join(metadata.RootPath, val)
	} else {
		path = metadata.RootPath
	}

	if path == "" {
		err = errors.New("required metadata rootPath or fileName missing")
	}

	return
}

// Helper to merge config and request metadata.
func (metadata sftpMetadata) mergeWithRequestMetadata(req *bindings.InvokeRequest) (sftpMetadata, error) {
	merged := metadata

	if val, ok := kitmd.GetMetadataProperty(req.Metadata, metadataRootPath); ok && val != "" {
		merged.RootPath = val
	}

	return merged, nil
}

// GetComponentMetadata returns the metadata of the component.
func (sftp *Sftp) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := sftpMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType)
	return
}
