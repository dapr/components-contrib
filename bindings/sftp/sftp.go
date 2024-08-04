package sftp

import (
	"context"
	"encoding/json"
	"fmt"

	sftpClient "github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	"github.com/dapr/components-contrib/bindings"
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
	RootPath   string `json:"rootPath"`
	FileName   string `json:"fileName"`
	Address    string `json:"address"`
	Username   string `json:"username"`
	Password   string `json:"password"`
	PrivateKey []byte `json:"privateKey"`
	PublicKey  []byte `json:"publicKey"`
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

	hostKeyCallback := ssh.InsecureIgnoreHostKey()

	sftp.logger.Infof("PrivateKey len: %d", len(m.PrivateKey))

	if len(m.PrivateKey) > 0 {
		signer, err := ssh.ParsePrivateKey(m.PrivateKey)
		if err != nil {
			return fmt.Errorf("sftp binding error: parse private key error: %w", err)
		}

		auth = append(auth, ssh.PublicKeys(signer))
	}

	if len(m.PublicKey) > 0 {
		sftp.logger.Infof("PublicKey len: %d", len(m.PublicKey))

		publicKey, _, _, _, err := ssh.ParseAuthorizedKey(m.PublicKey)
		if err != nil {
			return fmt.Errorf("sftp binding error: parse public key error: %w", err)
		}

		hostKeyCallback = ssh.FixedHostKey(publicKey)
	}

	sftp.logger.Infof("Username: %s", m.Username)
	sftp.logger.Infof("Password: %s", m.Password)

	if len(m.Password) > 0 {
		auth = append(auth, ssh.Password(m.Password))
	}

	sftp.logger.Infof("Auth len: %d", len(auth))

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
		// bindings.GetOperation,
		// bindings.DeleteOperation,
		bindings.ListOperation,
	}
}

func (sftp *Sftp) create(_ context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	metadata, err := sftp.metadata.mergeWithRequestMetadata(req)
	if err != nil {
		return nil, fmt.Errorf("sftp binding error: error merging metadata: %w", err)
	}

	rootPath := metadata.RootPath
	fileName := metadata.FileName

	path := sftp.sftpClient.Join(rootPath, fileName)

	sftp.logger.Infof("Path: %s", path)

	dir, fileName := sftpClient.Split(path)

	sftp.logger.Infof("Dir: %s", dir)
	sftp.logger.Infof("FileName: %s", fileName)

	err = sftp.sftpClient.MkdirAll(dir)
	if err != nil {
		return nil, fmt.Errorf("sftp binding error: error create dir: %s %w", dir, err)
	}

	file, err := sftp.sftpClient.Create(path)
	if err != nil {
		return nil, fmt.Errorf("sftp binding error: error create file: %s %w", path, err)
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

	rootPath := metadata.RootPath
	fileName := metadata.FileName

	path := sftp.sftpClient.Join(rootPath, fileName)

	sftp.logger.Infof("Path: %s", path)

	files, err := sftp.sftpClient.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("sftp binding error: error read dir: %s %w", path, err)
	}

	var resp []listResponse

	for _, file := range files {
		resp = append(resp, listResponse{
			FileName:    file.Name(),
			IsDirectory: file.IsDir(),
		})
	}

	jsonResponse, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("sftp binding error: cannot marshal list to json: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: jsonResponse,
	}, nil
}

func (sftp *Sftp) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	switch req.Operation {
	case bindings.CreateOperation:
		return sftp.create(ctx, req)
	// case bindings.GetOperation:
	// 	return sftp.get(ctx, req)
	// case bindings.DeleteOperation:
	// 	return sftp.delete(ctx, req)
	case bindings.ListOperation:
		return sftp.list(ctx, req)
	default:
		return nil, fmt.Errorf("unsupported operation %s", req.Operation)
	}
}

func (sftp *Sftp) Close() error {
	return sftp.sftpClient.Close()
}

// Helper to merge config and request metadata.
func (metadata sftpMetadata) mergeWithRequestMetadata(req *bindings.InvokeRequest) (sftpMetadata, error) {
	merged := metadata

	if val, ok := req.Metadata[metadataRootPath]; ok && val != "" {
		merged.RootPath = val
	}

	if val, ok := req.Metadata[metadataFileName]; ok && val != "" {
		merged.FileName = val
	}

	return merged, nil
}
