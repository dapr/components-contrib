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

package ftp

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"path"
	"reflect"
	"time"

	ftpclient "github.com/jlaffaye/ftp"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

const (
	metadataRootPath    = "rootPath"
	metadataFileName    = "fileName"
	metadataNewFileName = "newFileName"

	renameOperation bindings.OperationKind = "rename"

	tlsModeNone     = "none"
	tlsModeExplicit = "explicit"
	tlsModeImplicit = "implicit"

	defaultTimeout = 30 * time.Second
)

// Ftp is a binding for file operations on FTP and FTPS servers.
type Ftp struct {
	metadata *ftpMetadata
	logger   logger.Logger
	host     string
	rootCAs  *x509.CertPool
	// dial opens an authenticated session. Set by Init; tests replace it with a fake.
	dial func(ctx context.Context) (conn, error)
}

// ftpMetadata defines the ftp metadata.
type ftpMetadata struct {
	RootPath           string        `json:"rootPath"`
	Address            string        `json:"address"`
	Username           string        `json:"username"`
	Password           string        `json:"password"`
	TLSMode            string        `json:"tlsMode"`
	CACert             string        `json:"caCert"`
	InsecureSkipVerify bool          `json:"insecureSkipVerify"`
	Timeout            time.Duration `json:"timeout"`
}

type createResponse struct {
	FileName string `json:"fileName"`
}

type listResponse struct {
	FileName    string `json:"fileName"`
	IsDirectory bool   `json:"isDirectory"`
}

// conn is the subset of *ftpclient.ServerConn used by the binding.
// Retr is widened to io.ReadCloser so that tests can fake it.
type conn interface {
	List(path string) ([]*ftpclient.Entry, error)
	Retr(path string) (io.ReadCloser, error)
	Stor(path string, r io.Reader) error
	Delete(path string) error
	Rename(from, to string) error
	MakeDir(path string) error
	Quit() error
}

type serverConn struct {
	*ftpclient.ServerConn
}

func (c serverConn) Retr(p string) (io.ReadCloser, error) {
	return c.ServerConn.Retr(p)
}

// NewFtp returns a new ftp output binding.
func NewFtp(logger logger.Logger) bindings.OutputBinding {
	return &Ftp{logger: logger}
}

// Init parses and validates the metadata, then probes the server once.
func (f *Ftp) Init(ctx context.Context, meta bindings.Metadata) error {
	m, err := f.parseMetadata(meta)
	if err != nil {
		return fmt.Errorf("failed to parse metadata: %w", err)
	}

	f.host, _, err = net.SplitHostPort(m.Address)
	if err != nil {
		return fmt.Errorf("ftp binding error: invalid address %q: %w", m.Address, err)
	}

	switch m.TLSMode {
	case "":
		m.TLSMode = tlsModeNone
	case tlsModeNone, tlsModeExplicit, tlsModeImplicit:
	default:
		return fmt.Errorf("ftp binding error: invalid tlsMode %q: expected one of none, explicit, implicit", m.TLSMode)
	}

	if m.CACert != "" {
		f.rootCAs = x509.NewCertPool()
		if !f.rootCAs.AppendCertsFromPEM([]byte(m.CACert)) {
			return errors.New("ftp binding error: caCert contains no valid PEM certificate")
		}
	}

	if m.Timeout <= 0 {
		m.Timeout = defaultTimeout
	}

	f.metadata = m
	f.dial = f.connect

	// Fail fast on an unreachable server or bad credentials, like the sftp binding does.
	c, err := f.dial(ctx)
	if err != nil {
		return fmt.Errorf("ftp binding error: %w", err)
	}
	_ = c.Quit()

	f.logger.Debugf("ftp binding: connected to %s (tlsMode=%s)", m.Address, m.TLSMode)

	return nil
}

func (f *Ftp) parseMetadata(meta bindings.Metadata) (*ftpMetadata, error) {
	var m ftpMetadata
	err := kitmd.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

func (f *Ftp) tlsConfig() *tls.Config {
	return &tls.Config{
		ServerName:         f.host,
		RootCAs:            f.rootCAs,
		InsecureSkipVerify: f.metadata.InsecureSkipVerify, //nolint:gosec
		MinVersion:         tls.VersionTLS12,
	}
}

// connect opens a fresh authenticated session.
// ponytail: connect-per-op; add a persistent conn or pool if login overhead or server connection limits (421) bite.
func (f *Ftp) connect(ctx context.Context) (conn, error) {
	m := f.metadata
	opts := []ftpclient.DialOption{
		ftpclient.DialWithContext(ctx),
		ftpclient.DialWithTimeout(m.Timeout),
	}
	switch m.TLSMode {
	case tlsModeExplicit:
		opts = append(opts, ftpclient.DialWithExplicitTLS(f.tlsConfig()))
	case tlsModeImplicit:
		opts = append(opts, ftpclient.DialWithTLS(f.tlsConfig()))
	}

	c, err := ftpclient.Dial(m.Address, opts...)
	if err != nil {
		return nil, fmt.Errorf("error dialing ftp server %s: %w", m.Address, err)
	}

	err = c.Login(m.Username, m.Password)
	if err != nil {
		_ = c.Quit()
		return nil, fmt.Errorf("error logging in to ftp server %s: %w", m.Address, err)
	}

	return serverConn{c}, nil
}

// withConn runs fn on a fresh session and always closes it.
func (f *Ftp) withConn(ctx context.Context, fn func(c conn) error) error {
	c, err := f.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Quit()

	return fn(c)
}

func (f *Ftp) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		bindings.CreateOperation,
		bindings.GetOperation,
		bindings.DeleteOperation,
		bindings.ListOperation,
		renameOperation,
	}
}

func (f *Ftp) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	switch req.Operation {
	case bindings.CreateOperation:
		return f.create(ctx, req)
	case bindings.GetOperation:
		return f.get(ctx, req)
	case bindings.DeleteOperation:
		return f.delete(ctx, req)
	case bindings.ListOperation:
		return f.list(ctx, req)
	case renameOperation:
		return f.rename(ctx, req)
	default:
		return nil, fmt.Errorf("unsupported operation %s", req.Operation)
	}
}

func (f *Ftp) create(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	p, err := f.metadata.mergeWithRequestMetadata(req).getPath(req.Metadata)
	if err != nil {
		return nil, fmt.Errorf("ftp binding error: %w", err)
	}

	err = f.withConn(ctx, func(c conn) error {
		mErr := mkdirAll(c, path.Dir(p))
		if mErr != nil {
			return mErr
		}

		sErr := c.Stor(p, bytes.NewReader(req.Data))
		if sErr != nil {
			return fmt.Errorf("error create file %s: %w", p, sErr)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("ftp binding error: %w", err)
	}

	fileName := path.Base(p)
	jsonResponse, err := json.Marshal(createResponse{FileName: fileName})
	if err != nil {
		return nil, fmt.Errorf("ftp binding error: error marshalling create response: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: jsonResponse,
		Metadata: map[string]string{
			metadataFileName: fileName,
		},
	}, nil
}

func (f *Ftp) get(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	p, err := f.metadata.mergeWithRequestMetadata(req).getPath(req.Metadata)
	if err != nil {
		return nil, fmt.Errorf("ftp binding error: %w", err)
	}

	var data []byte
	err = f.withConn(ctx, func(c conn) error {
		r, rErr := c.Retr(p)
		if rErr != nil {
			return rErr
		}
		defer r.Close()

		data, rErr = io.ReadAll(r)
		return rErr
	})
	if err != nil {
		return nil, fmt.Errorf("ftp binding error: error reading file %s: %w", p, err)
	}

	return &bindings.InvokeResponse{
		Data: data,
	}, nil
}

func (f *Ftp) delete(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	p, err := f.metadata.mergeWithRequestMetadata(req).getPath(req.Metadata)
	if err != nil {
		return nil, fmt.Errorf("ftp binding error: %w", err)
	}

	err = f.withConn(ctx, func(c conn) error {
		return c.Delete(p)
	})
	if err != nil {
		return nil, fmt.Errorf("ftp binding error: error remove file %s: %w", p, err)
	}

	return nil, nil
}

func (f *Ftp) list(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	p, err := f.metadata.mergeWithRequestMetadata(req).getPath(req.Metadata)
	if err != nil {
		return nil, fmt.Errorf("ftp binding error: %w", err)
	}

	var entries []*ftpclient.Entry
	err = f.withConn(ctx, func(c conn) error {
		var lErr error
		entries, lErr = c.List(p)
		return lErr
	})
	if err != nil {
		return nil, fmt.Errorf("ftp binding error: error read dir %s: %w", p, err)
	}

	resp := make([]listResponse, 0, len(entries))
	for _, e := range entries {
		// MLSD listings may include the directory itself and its parent.
		if e.Name == "." || e.Name == ".." {
			continue
		}
		resp = append(resp, listResponse{
			FileName:    e.Name,
			IsDirectory: e.Type == ftpclient.EntryTypeFolder,
		})
	}

	jsonResponse, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("ftp binding error: cannot marshal list to json: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: jsonResponse,
	}, nil
}

// rename moves fileName to newFileName; both are relative to rootPath.
func (f *Ftp) rename(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	m := f.metadata.mergeWithRequestMetadata(req)

	from, ok := kitmd.GetMetadataProperty(req.Metadata, metadataFileName)
	if !ok || from == "" {
		return nil, errors.New("ftp binding error: required metadata fileName missing")
	}
	to, ok := kitmd.GetMetadataProperty(req.Metadata, metadataNewFileName)
	if !ok || to == "" {
		return nil, errors.New("ftp binding error: required metadata newFileName missing")
	}
	from, to = path.Join(m.RootPath, from), path.Join(m.RootPath, to)

	err := f.withConn(ctx, func(c conn) error {
		return c.Rename(from, to)
	})
	if err != nil {
		return nil, fmt.Errorf("ftp binding error: error rename %s to %s: %w", from, to, err)
	}

	return nil, nil
}

func (f *Ftp) Close() error {
	return nil
}

// mkdirAll creates dir and its missing parents with one MKD per level.
// Servers reply 550 (a few use 521) for a directory that already exists, so those replies are ignored;
// if a directory is genuinely missing the following STOR fails with a clear error anyway.
func mkdirAll(c conn, dir string) error {
	dir = path.Clean(dir)
	if dir == "." || dir == "/" {
		return nil
	}

	err := mkdirAll(c, path.Dir(dir))
	if err != nil {
		return err
	}

	err = c.MakeDir(dir)
	if err != nil && !isDirExistsReply(err) {
		return fmt.Errorf("error create dir %s: %w", dir, err)
	}

	return nil
}

func isDirExistsReply(err error) bool {
	var tpErr *textproto.Error
	if !errors.As(err, &tpErr) {
		return false
	}

	return tpErr.Code == ftpclient.StatusFileUnavailable || tpErr.Code == 521
}

func (m ftpMetadata) getPath(requestMetadata map[string]string) (string, error) {
	p := m.RootPath
	if val, ok := kitmd.GetMetadataProperty(requestMetadata, metadataFileName); ok && val != "" {
		p = path.Join(m.RootPath, val)
	}

	if p == "" {
		return "", errors.New("required metadata rootPath or fileName missing")
	}

	return p, nil
}

// mergeWithRequestMetadata lets a request override rootPath.
func (m ftpMetadata) mergeWithRequestMetadata(req *bindings.InvokeRequest) ftpMetadata {
	if val, ok := kitmd.GetMetadataProperty(req.Metadata, metadataRootPath); ok && val != "" {
		m.RootPath = val
	}

	return m
}

// GetComponentMetadata returns the metadata of the component.
func (f *Ftp) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := ftpMetadata{}
	_ = metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType)
	return
}
