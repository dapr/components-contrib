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
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/big"
	"net"
	"net/textproto"
	"strings"
	"testing"
	"time"

	ftpclient "github.com/jlaffaye/ftp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

// fakeConn records calls and serves canned data.
type fakeConn struct {
	entries  []*ftpclient.Entry
	files    map[string][]byte
	dirs     []string
	deleted  string
	renamed  [2]string
	quits    int
	err      error
	mkdirErr error
	closeErr error
}

// errCloser serves data and then fails on Close, like a transfer the server aborts.
type errCloser struct {
	io.Reader
	err error
}

func (c errCloser) Close() error { return c.err }

func (f *fakeConn) List(string) ([]*ftpclient.Entry, error) {
	return f.entries, f.err
}

func (f *fakeConn) Retr(p string) (io.ReadCloser, error) {
	if f.err != nil {
		return nil, f.err
	}

	return errCloser{bytes.NewReader(f.files[p]), f.closeErr}, nil
}

func (f *fakeConn) Stor(p string, r io.Reader) error {
	if f.err != nil {
		return f.err
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if f.files == nil {
		f.files = map[string][]byte{}
	}
	f.files[p] = data

	return nil
}

func (f *fakeConn) Delete(p string) error {
	f.deleted = p
	return f.err
}

func (f *fakeConn) Rename(from, to string) error {
	f.renamed = [2]string{from, to}
	return f.err
}

func (f *fakeConn) MakeDir(p string) error {
	f.dirs = append(f.dirs, p)
	return f.mkdirErr
}

func (f *fakeConn) Quit() error {
	f.quits++
	return nil
}

func newTestBinding(fc *fakeConn) *Ftp {
	return &Ftp{
		metadata: &ftpMetadata{RootPath: "/root"},
		dial: func(context.Context) (conn, error) {
			return fc, nil
		},
	}
}

func newMetadata(props map[string]string) bindings.Metadata {
	m := bindings.Metadata{}
	m.Properties = props
	return m
}

func TestParseMetadata(t *testing.T) {
	meta, err := (&Ftp{}).parseMetadata(newMetadata(map[string]string{
		"rootPath":           "/path",
		"address":            "ftp.example.com:21",
		"username":           "user",
		"password":           "pass",
		"tlsMode":            "explicit",
		"caCert":             "cert",
		"insecureSkipVerify": "true",
		"timeout":            "10s",
	}))

	require.NoError(t, err)
	assert.Equal(t, "/path", meta.RootPath)
	assert.Equal(t, "ftp.example.com:21", meta.Address)
	assert.Equal(t, "user", meta.Username)
	assert.Equal(t, "pass", meta.Password)
	assert.Equal(t, "explicit", meta.TLSMode)
	assert.Equal(t, "cert", meta.CACert)
	assert.True(t, meta.InsecureSkipVerify)
	assert.Equal(t, 10*time.Second, meta.Timeout)
}

// All cases fail before the server is contacted.
func TestInitValidation(t *testing.T) {
	tests := map[string]struct {
		props   map[string]string
		wantErr string
	}{
		"missing address":      {map[string]string{"username": "user"}, "invalid address"},
		"address without port": {map[string]string{"address": "ftp.example.com"}, "invalid address"},
		"unknown tlsMode":      {map[string]string{"address": "127.0.0.1:21", "tlsMode": "starttls"}, "invalid tlsMode"},
		"invalid caCert":       {map[string]string{"address": "127.0.0.1:21", "caCert": "not a pem"}, "caCert"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := (&Ftp{}).Init(t.Context(), newMetadata(tc.props))
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestTLSConfig(t *testing.T) {
	f := &Ftp{
		host:     "ftp.example.com",
		metadata: &ftpMetadata{InsecureSkipVerify: true},
	}

	cfg := f.tlsConfig()

	assert.Equal(t, "ftp.example.com", cfg.ServerName)
	assert.True(t, cfg.InsecureSkipVerify)
	assert.Nil(t, cfg.RootCAs)
	assert.Equal(t, uint16(tls.VersionTLS12), cfg.MinVersion)
	assert.NotNil(t, cfg.ClientSessionCache, "data connections must resume the control connection's TLS session")
}

func TestGetPath(t *testing.T) {
	t.Run("root path only", func(t *testing.T) {
		p, err := ftpMetadata{RootPath: "/path"}.getPath(map[string]string{})
		require.NoError(t, err)
		assert.Equal(t, "/path", p)
	})

	t.Run("joins file name", func(t *testing.T) {
		p, err := ftpMetadata{RootPath: "/path"}.getPath(map[string]string{"fileName": "/sub/file.txt"})
		require.NoError(t, err)
		assert.Equal(t, "/path/sub/file.txt", p)
	})

	t.Run("error if both empty", func(t *testing.T) {
		_, err := ftpMetadata{}.getPath(map[string]string{})
		require.Error(t, err)
	})
}

func TestJoinUnderRoot(t *testing.T) {
	ok := map[string]string{
		"sub/a.txt": "/root/sub/a.txt",
		"a/../b":    "/root/b",
		"/abs.txt":  "/root/abs.txt",
		"./c":       "/root/c",
	}
	for name, want := range ok {
		p, err := joinUnderRoot("/root", name)
		require.NoError(t, err, name)
		assert.Equal(t, want, p)
	}

	for _, name := range []string{"..", "../x", "a/../../x", "sub/../../x", "/../secret.txt", "//../x", "/a/../../x", "a\r\nDELE /etc/passwd"} {
		_, err := joinUnderRoot("/root", name)
		require.Error(t, err, name)
	}
}

func TestMergeWithRequestMetadata(t *testing.T) {
	m := ftpMetadata{RootPath: "/path", Address: "address", Username: "user"}
	req := &bindings.InvokeRequest{Metadata: map[string]string{
		"rootPath": "/changed",
		"address":  "changedaddress",
		"username": "changeduser",
	}}

	merged := m.mergeWithRequestMetadata(req)

	assert.Equal(t, "/changed", merged.RootPath)
	assert.Equal(t, "address", merged.Address)
	assert.Equal(t, "user", merged.Username)
}

func TestOperations(t *testing.T) {
	assert.ElementsMatch(t, []bindings.OperationKind{
		bindings.CreateOperation,
		bindings.GetOperation,
		bindings.DeleteOperation,
		bindings.ListOperation,
		renameOperation,
	}, (&Ftp{}).Operations())
}

func TestInvoke(t *testing.T) {
	t.Run("create writes file and creates parent dirs", func(t *testing.T) {
		fc := &fakeConn{}
		f := newTestBinding(fc)

		resp, err := f.Invoke(t.Context(), &bindings.InvokeRequest{
			Operation: bindings.CreateOperation,
			Data:      []byte("hello"),
			Metadata:  map[string]string{"fileName": "a/b/test.txt"},
		})

		require.NoError(t, err)
		assert.Equal(t, []string{"/root", "/root/a", "/root/a/b"}, fc.dirs)
		assert.Equal(t, []byte("hello"), fc.files["/root/a/b/test.txt"])
		assert.JSONEq(t, `{"fileName":"test.txt"}`, string(resp.Data))
		assert.Equal(t, "test.txt", resp.Metadata["fileName"])
		assert.Equal(t, 1, fc.quits)
	})

	t.Run("create ignores already existing dir", func(t *testing.T) {
		fc := &fakeConn{mkdirErr: &textproto.Error{Code: ftpclient.StatusFileUnavailable, Msg: "File exists"}}
		f := newTestBinding(fc)

		_, err := f.Invoke(t.Context(), &bindings.InvokeRequest{
			Operation: bindings.CreateOperation,
			Data:      []byte("hello"),
			Metadata:  map[string]string{"fileName": "test.txt"},
		})

		require.NoError(t, err)
		assert.Equal(t, []byte("hello"), fc.files["/root/test.txt"])
	})

	t.Run("create fails on other mkdir errors", func(t *testing.T) {
		fc := &fakeConn{mkdirErr: &textproto.Error{Code: ftpclient.StatusNotLoggedIn, Msg: "Not logged in"}}
		f := newTestBinding(fc)

		_, err := f.Invoke(t.Context(), &bindings.InvokeRequest{
			Operation: bindings.CreateOperation,
			Data:      []byte("hello"),
			Metadata:  map[string]string{"fileName": "test.txt"},
		})

		require.ErrorContains(t, err, "error create dir /root")
		assert.Empty(t, fc.files)
	})

	t.Run("get returns file content", func(t *testing.T) {
		fc := &fakeConn{files: map[string][]byte{"/root/test.txt": []byte("content")}}
		f := newTestBinding(fc)

		resp, err := f.Invoke(t.Context(), &bindings.InvokeRequest{
			Operation: bindings.GetOperation,
			Metadata:  map[string]string{"fileName": "test.txt"},
		})

		require.NoError(t, err)
		assert.Equal(t, []byte("content"), resp.Data)
		assert.Equal(t, 1, fc.quits)
	})

	t.Run("get reports a transfer aborted on close", func(t *testing.T) {
		fc := &fakeConn{
			files:    map[string][]byte{"/root/f.txt": []byte("partial")},
			closeErr: errors.New("426 Transfer aborted"),
		}
		f := newTestBinding(fc)

		_, err := f.Invoke(t.Context(), &bindings.InvokeRequest{
			Operation: bindings.GetOperation,
			Metadata:  map[string]string{"fileName": "f.txt"},
		})

		require.ErrorContains(t, err, "426")
	})

	t.Run("get propagates errors", func(t *testing.T) {
		fc := &fakeConn{err: errors.New("550 no such file")}
		f := newTestBinding(fc)

		_, err := f.Invoke(t.Context(), &bindings.InvokeRequest{
			Operation: bindings.GetOperation,
			Metadata:  map[string]string{"fileName": "missing.txt"},
		})

		require.ErrorContains(t, err, "550 no such file")
		assert.Equal(t, 1, fc.quits)
	})

	t.Run("delete removes file", func(t *testing.T) {
		fc := &fakeConn{}
		f := newTestBinding(fc)

		resp, err := f.Invoke(t.Context(), &bindings.InvokeRequest{
			Operation: bindings.DeleteOperation,
			Metadata:  map[string]string{"fileName": "test.txt"},
		})

		require.NoError(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, "/root/test.txt", fc.deleted)
	})

	t.Run("list maps entries and skips dot dirs", func(t *testing.T) {
		fc := &fakeConn{entries: []*ftpclient.Entry{
			{Name: ".", Type: ftpclient.EntryTypeFolder},
			{Name: "..", Type: ftpclient.EntryTypeFolder},
			{Name: "sub", Type: ftpclient.EntryTypeFolder},
			{Name: "file.txt", Type: ftpclient.EntryTypeFile},
			{Name: "link", Type: ftpclient.EntryTypeLink},
		}}
		f := newTestBinding(fc)

		resp, err := f.Invoke(t.Context(), &bindings.InvokeRequest{Operation: bindings.ListOperation})
		require.NoError(t, err)

		var got []listResponse
		require.NoError(t, json.Unmarshal(resp.Data, &got))
		assert.Equal(t, []listResponse{
			{FileName: "sub", IsDirectory: true},
			{FileName: "file.txt"},
			{FileName: "link"},
		}, got)
	})

	t.Run("rename joins both names with root path", func(t *testing.T) {
		fc := &fakeConn{}
		f := newTestBinding(fc)

		resp, err := f.Invoke(t.Context(), &bindings.InvokeRequest{
			Operation: renameOperation,
			Metadata:  map[string]string{"fileName": "old.txt", "newFileName": "sub/new.txt"},
		})

		require.NoError(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, [2]string{"/root/old.txt", "/root/sub/new.txt"}, fc.renamed)
	})

	t.Run("rename requires fileName and newFileName", func(t *testing.T) {
		fc := &fakeConn{}
		f := newTestBinding(fc)

		_, err := f.Invoke(t.Context(), &bindings.InvokeRequest{
			Operation: renameOperation,
			Metadata:  map[string]string{"fileName": "old.txt"},
		})
		require.ErrorContains(t, err, "newFileName")

		_, err = f.Invoke(t.Context(), &bindings.InvokeRequest{
			Operation: renameOperation,
			Metadata:  map[string]string{"newFileName": "new.txt"},
		})
		require.ErrorContains(t, err, "fileName")

		assert.Equal(t, 0, fc.quits)
	})

	t.Run("names outside rootPath are rejected", func(t *testing.T) {
		fc := &fakeConn{}
		f := newTestBinding(fc)

		_, err := f.Invoke(t.Context(), &bindings.InvokeRequest{
			Operation: bindings.GetOperation,
			Metadata:  map[string]string{"fileName": "../secret.txt"},
		})
		require.ErrorContains(t, err, "escapes rootPath")

		_, err = f.Invoke(t.Context(), &bindings.InvokeRequest{
			Operation: renameOperation,
			Metadata:  map[string]string{"fileName": "old.txt", "newFileName": "../../new.txt"},
		})
		require.ErrorContains(t, err, "escapes rootPath")

		assert.Equal(t, 0, fc.quits)
	})

	t.Run("request rootPath overrides component rootPath", func(t *testing.T) {
		fc := &fakeConn{}
		f := newTestBinding(fc)

		_, err := f.Invoke(t.Context(), &bindings.InvokeRequest{
			Operation: bindings.DeleteOperation,
			Metadata:  map[string]string{"rootPath": "/other", "fileName": "test.txt"},
		})

		require.NoError(t, err)
		assert.Equal(t, "/other/test.txt", fc.deleted)
	})

	t.Run("unsupported operation", func(t *testing.T) {
		f := newTestBinding(&fakeConn{})

		_, err := f.Invoke(t.Context(), &bindings.InvokeRequest{Operation: "copy"})

		require.ErrorContains(t, err, "unsupported operation")
	})

	t.Run("dial error is returned", func(t *testing.T) {
		f := &Ftp{
			metadata: &ftpMetadata{RootPath: "/root"},
			dial: func(context.Context) (conn, error) {
				return nil, errors.New("connection refused")
			},
		}

		_, err := f.Invoke(t.Context(), &bindings.InvokeRequest{Operation: bindings.ListOperation})

		require.ErrorContains(t, err, "connection refused")
	})
}

// serveFakeFTP answers the commands the client sends while connecting and logging in.
// A silent server accepts connections but never replies.
func serveFakeFTP(t *testing.T, l net.Listener, silent bool) {
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				if silent {
					_, _ = io.Copy(io.Discard, c)
					return
				}
				fmt.Fprint(c, "220 fake ftp\r\n")
				r := bufio.NewReader(c)
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					cmd, arg, _ := strings.Cut(strings.TrimSpace(line), " ")
					switch cmd {
					case "USER":
						fmt.Fprint(c, "331 need password\r\n")
					case "PASS":
						if arg == "pass" {
							fmt.Fprint(c, "230 logged in\r\n")
						} else {
							fmt.Fprint(c, "530 login incorrect\r\n")
						}
					case "FEAT":
						fmt.Fprint(c, "211 End\r\n")
					case "TYPE", "PBSZ", "PROT":
						fmt.Fprint(c, "200 ok\r\n")
					case "QUIT":
						fmt.Fprint(c, "221 bye\r\n")
						return
					default:
						fmt.Fprint(c, "502 not implemented\r\n")
					}
				}
			}()
		}
	}()
}

// selfSignedCert returns a certificate for 127.0.0.1 and its PEM, usable as caCert.
func selfSignedCert(t *testing.T) (tls.Certificate, string) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)

	return cert, string(certPEM)
}

func TestConnectImplicitTLS(t *testing.T) {
	cert, caPEM := selfSignedCert(t)
	l, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	require.NoError(t, err)
	serveFakeFTP(t, l, false)

	props := func(overrides map[string]string) bindings.Metadata {
		p := map[string]string{
			"address": l.Addr().String(), "username": "foo", "password": "pass",
			"rootPath": "/", "tlsMode": "implicit", "caCert": caPEM,
		}
		maps.Copy(p, overrides)
		return newMetadata(p)
	}

	t.Run("handshake with trusted caCert", func(t *testing.T) {
		require.NoError(t, (&Ftp{logger: logger.NewLogger("test")}).Init(t.Context(), props(nil)))
	})

	t.Run("untrusted certificate is rejected", func(t *testing.T) {
		err := (&Ftp{}).Init(t.Context(), props(map[string]string{"caCert": ""}))
		require.ErrorContains(t, err, "certificate")
	})

	t.Run("wrong password", func(t *testing.T) {
		err := (&Ftp{}).Init(t.Context(), props(map[string]string{"password": "nope"}))
		require.ErrorContains(t, err, "530")
	})
}

func TestConnectStalledServer(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	serveFakeFTP(t, l, true)
	props := map[string]string{"address": l.Addr().String(), "username": "foo", "password": "pass", "rootPath": "/"}

	t.Run("idle timeout", func(t *testing.T) {
		props["timeout"] = "200ms"
		start := time.Now()
		err := (&Ftp{}).Init(t.Context(), newMetadata(props))
		require.ErrorContains(t, err, "i/o timeout")
		assert.Less(t, time.Since(start), 5*time.Second)
	})

	t.Run("cancellation closes the connection", func(t *testing.T) {
		props["timeout"] = "30s"
		ctx, cancel := context.WithCancel(t.Context())
		time.AfterFunc(100*time.Millisecond, cancel)
		start := time.Now()
		err := (&Ftp{}).Init(ctx, newMetadata(props))
		require.Error(t, err)
		assert.Less(t, time.Since(start), 5*time.Second)
	})
}
