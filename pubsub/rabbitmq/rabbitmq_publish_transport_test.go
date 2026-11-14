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

package rabbitmq

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mdata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

// This is a minimal in-process AMQP protocol peer, not a RabbitMQ broker. It
// exercises the pinned client's real handshake, writes and confirmations over
// net.Pipe without inspecting or modifying any AMQP private fields.
type publishObservedConn struct {
	net.Conn
	closed   chan struct{}
	once     sync.Once
	deadline atomic.Int64
}

func (c *publishObservedConn) SetDeadline(deadline time.Time) error {
	if !deadline.IsZero() {
		c.deadline.CompareAndSwap(0, deadline.UnixNano())
	}
	return c.Conn.SetDeadline(deadline)
}

func (c *publishObservedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() { close(c.closed) })
	return err
}

type publishProtocolPeer struct {
	net.Conn
	clientClosed <-chan struct{}
}

func (p publishProtocolPeer) frame() (byte, []byte, error) {
	header := make([]byte, 7)
	if _, err := io.ReadFull(p.Conn, header); err != nil {
		return 0, nil, err
	}
	size := binary.BigEndian.Uint32(header[3:])
	if size > 1<<20 {
		return 0, nil, fmt.Errorf("unexpected AMQP frame size %d", size)
	}
	body := make([]byte, int(size)+1)
	if _, err := io.ReadFull(p.Conn, body); err != nil {
		return 0, nil, err
	}
	if body[len(body)-1] != 0xce {
		return 0, nil, errors.New("invalid AMQP frame end")
	}
	return header[0], body[:len(body)-1], nil
}

func (p publishProtocolPeer) method(class, method uint16) ([]byte, error) {
	kind, body, err := p.frame()
	if err != nil {
		return nil, err
	}
	if kind != 1 || len(body) < 4 || binary.BigEndian.Uint16(body) != class || binary.BigEndian.Uint16(body[2:]) != method {
		return nil, fmt.Errorf("expected AMQP method %d.%d, got type=%d body=%x", class, method, kind, body)
	}
	return body[4:], nil
}

func (p publishProtocolPeer) send(channel, class, method uint16, args []byte) error {
	frame := make([]byte, 7+4+len(args)+1)
	frame[0] = 1
	binary.BigEndian.PutUint16(frame[1:], channel)
	binary.BigEndian.PutUint32(frame[3:], uint32(4+len(args))) //nolint:gosec // test frames are far below 4 GiB
	binary.BigEndian.PutUint16(frame[7:], class)
	binary.BigEndian.PutUint16(frame[9:], method)
	copy(frame[11:], args)
	frame[len(frame)-1] = 0xce
	_, err := p.Write(frame)
	return err
}

func amqpLongString(s string) []byte {
	value := make([]byte, 4+len(s))
	binary.BigEndian.PutUint32(value, uint32(len(s))) //nolint:gosec // test strings are far below 4 GiB
	copy(value[4:], s)
	return value
}

func (p publishProtocolPeer) stall(entered chan<- struct{}) error {
	close(entered)
	// Cancellation must close the client's exact raw transport and release this
	// read as well as the client operation. The caller joins both workers.
	<-p.clientClosed
	return nil
}

func (p publishProtocolPeer) run(stage string, passive bool, entered chan<- struct{}) error {
	if stage == "TLS handshake" {
		first := make([]byte, 1)
		if _, err := io.ReadFull(p.Conn, first); err != nil {
			return err
		}
		return p.stall(entered)
	}
	header := make([]byte, 8)
	if _, err := io.ReadFull(p.Conn, header); err != nil {
		return err
	}
	if string(header) != "AMQP\x00\x00\x09\x01" {
		return fmt.Errorf("invalid AMQP protocol header %x", header)
	}
	if stage == "AMQP handshake" {
		return p.stall(entered)
	}
	start := slices.Concat([]byte{0, 9, 0, 0, 0, 0}, amqpLongString("PLAIN"), amqpLongString("en_US"))
	if err := p.send(0, 10, 10, start); err != nil {
		return err
	}
	if _, err := p.method(10, 11); err != nil {
		return err
	}
	if stage == "authentication handshake" {
		return p.stall(entered)
	}
	if err := p.send(0, 10, 30, []byte{0, 0, 0, 2, 0, 0, 0, 0}); err != nil {
		return err
	}
	if _, err := p.method(10, 31); err != nil {
		return err
	}
	if _, err := p.method(10, 40); err != nil {
		return err
	}
	if err := p.send(0, 10, 41, []byte{0}); err != nil {
		return err
	}
	if _, err := p.method(20, 10); err != nil {
		return err
	}
	if stage == "channel open" {
		return p.stall(entered)
	}
	if err := p.send(1, 20, 11, []byte{0, 0, 0, 0}); err != nil {
		return err
	}
	if !strings.HasPrefix(stage, "unconfirmed") {
		if _, err := p.method(85, 10); err != nil {
			return err
		}
		if stage == "confirm setup" {
			return p.stall(entered)
		}
		if err := p.send(1, 85, 11, nil); err != nil {
			return err
		}
	}
	declaration, err := p.method(40, 10)
	if err != nil {
		return err
	}
	if len(declaration) < 3 {
		return errors.New("short exchange.declare")
	}
	kindOffset := 3 + int(declaration[2])
	if len(declaration) <= kindOffset {
		return errors.New("short exchange kind")
	}
	flagsOffset := kindOffset + 1 + int(declaration[kindOffset])
	if len(declaration) <= flagsOffset {
		return errors.New("missing exchange flags")
	}
	if (declaration[flagsOffset]&1 != 0) != passive {
		return errors.New("incorrect passive declaration flag")
	}
	if stage == "active declaration" || stage == "passive declaration" {
		return p.stall(entered)
	}
	if err = p.send(1, 40, 11, nil); err != nil {
		return err
	}
	if _, err = p.method(60, 40); err != nil {
		return err
	}
	if stage == "write" || stage == "unconfirmed write" {
		return p.stall(entered)
	}
	if err = p.content(); err != nil {
		return err
	}
	if stage == "unconfirmed success" {
		close(entered)
		if _, err = p.method(60, 40); err != nil {
			return err
		}
		if err = p.content(); err != nil {
			return err
		}
		<-p.clientClosed
		return nil
	}
	if stage == "channel access refused" || stage == "connection access refused" {
		reason := "ACCESS_REFUSED - write permission denied"
		args := slices.Concat([]byte{1, 147, byte(len(reason))}, []byte(reason), []byte{0, 60, 0, 40}) //nolint:gosec // constant reason is shorter than 256 bytes
		channel, class, closeMethod := uint16(1), uint16(20), uint16(40)
		if stage == "connection access refused" {
			channel, class, closeMethod = 0, 10, 50
		}
		if err = p.send(channel, class, closeMethod, args); err != nil {
			return err
		}
		if _, err = p.method(class, closeMethod+1); err != nil {
			return err
		}
		close(entered)
		<-p.clientClosed
		return nil
	}
	if stage == "confirmation" {
		return p.stall(entered)
	}
	if stage == "shutdown during confirmation" {
		close(entered)
		return nil
	}
	confirmation := make([]byte, 9)
	binary.BigEndian.PutUint64(confirmation, 1)
	method := uint16(80)
	if stage == "nack" || stage == "nack then disconnect" {
		method = 120
	}
	if err = p.send(1, 60, method, confirmation); err != nil {
		return err
	}
	close(entered)
	if stage == "nack then disconnect" {
		return nil
	}
	<-p.clientClosed
	return nil
}

func (p publishProtocolPeer) content() error {
	kind, content, err := p.frame()
	if err != nil {
		return err
	}
	if kind != 2 || len(content) < 12 {
		return errors.New("expected content header")
	}
	remaining := binary.BigEndian.Uint64(content[4:])
	for remaining > 0 {
		kind, body, readErr := p.frame()
		if readErr != nil {
			return readErr
		}
		if kind != 3 || uint64(len(body)) > remaining {
			return errors.New("invalid content body")
		}
		remaining -= uint64(len(body))
	}
	return nil
}

func TestPublishPublicNativeTransportCancellation(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"dial", "TLS handshake", "AMQP handshake", "authentication handshake", "channel open", "confirm setup", "active declaration", "passive declaration", "write", "confirmation"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			clientPipe, server := net.Pipe()
			client := &publishObservedConn{Conn: clientPipe, closed: make(chan struct{})}
			defer client.Close()
			defer server.Close()
			entered := make(chan struct{})
			peerDone := make(chan error, 1)
			if stage == "dial" {
				peerDone <- nil
			} else {
				go func() {
					defer server.Close()
					peerDone <- (publishProtocolPeer{Conn: server, clientClosed: client.closed}).run(stage, stage == "passive declaration", entered)
				}()
			}
			r := newPublicPublisher(newPublishTestBroker())
			r.connection, r.channel, r.connectionCount = nil, nil, 0
			r.metadata.PublisherConfirm = true
			if stage == "passive declaration" {
				r.metadata.ExchangeDeclareMode = exchangeDeclareModePassive
			}
			r.connectionDial = func(ctx context.Context, _ string, _ string, _ string, _ time.Duration, _ *tls.Config, _ bool) (rabbitMQConnectionBroker, rabbitMQChannelBroker, error) {
				uri := "amqp://guest:guest@localhost/?heartbeat=0"
				if stage == "TLS handshake" {
					uri = "amqps://guest:guest@localhost/?heartbeat=0"
				}
				tlsConfig := &tls.Config{InsecureSkipVerify: true} //nolint:gosec // loopback test peer without a certificate
				return dialWithConfig(ctx, uri, amqp.Config{TLSClientConfig: tlsConfig}, func(ctx context.Context, _, _ string) (net.Conn, error) {
					if stage == "dial" {
						close(entered)
						<-ctx.Done()
						return nil, ctx.Err()
					}
					return client, nil
				})
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- r.Publish(ctx, &pubsub.PublishRequest{Topic: "topic", Data: make([]byte, 16*1024)}) }()
			awaitPublishGate(t, entered)
			cancel()
			err := awaitPublish(t, done)
			require.ErrorIs(t, err, context.Canceled)
			require.NotContains(t, err.Error(), "ACCESS_REFUSED")
			require.NotContains(t, err.Error(), "username or password not allowed")
			select {
			case peerErr := <-peerDone:
				require.NoError(t, peerErr)
			case <-time.After(time.Second):
				t.Fatal("protocol peer did not terminate after cancellation")
			}
		})
	}
}

func TestPublishPublicNativeConfirmationOutcomes(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"ack", "nack", "nack then disconnect", "shutdown during confirmation"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			clientPipe, server := net.Pipe()
			client := &publishObservedConn{Conn: clientPipe, closed: make(chan struct{})}
			defer client.Close()
			defer server.Close()
			entered := make(chan struct{})
			peerDone := make(chan error, 1)
			go func() {
				defer server.Close()
				peerDone <- (publishProtocolPeer{Conn: server, clientClosed: client.closed}).run(stage, false, entered)
			}()
			r := newPublicPublisher(newPublishTestBroker())
			r.connection, r.channel, r.connectionCount = nil, nil, 0
			r.metadata.PublisherConfirm = true
			r.connectionDial = func(ctx context.Context, _ string, _ string, _ string, _ time.Duration, _ *tls.Config, _ bool) (rabbitMQConnectionBroker, rabbitMQChannelBroker, error) {
				r.metadata.ReconnectWait = time.Hour
				return dialWithConfig(ctx, "amqp://guest:guest@localhost/?heartbeat=0", amqp.Config{}, func(context.Context, string, string) (net.Conn, error) { return client, nil })
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := startPublicPublish(r, ctx, "topic")
			awaitPublishGate(t, entered)
			// The native confirmation has to leave publishSync before cancellation.
			require.NoError(t, r.channelMutex.Lock(t.Context()))
			r.channelMutex.Unlock()
			if stage == "ack" {
				require.NoError(t, awaitPublish(t, done))
			} else {
				cancel()
				err := awaitPublish(t, done)
				if stage == "nack" || stage == "nack then disconnect" {
					require.EqualError(t, err, "did not receive confirmation of publishing")
				} else {
					require.Error(t, err)
					require.NotContains(t, err.Error(), "did not receive confirmation of publishing")
				}
			}
			// A finished ACK/NACK no longer owns a cancellation callback. Explicitly
			// close the raw transport to join the protocol peer, without a Close RPC.
			require.NoError(t, r.channelMutex.Lock(t.Context()))
			if r.connection != nil {
				_ = r.connection.Abort()
			}
			r.channelMutex.Unlock()
			select {
			case peerErr := <-peerDone:
				require.NoError(t, peerErr)
			case <-time.After(time.Second):
				t.Fatal("protocol peer did not terminate")
			}
		})
	}
}

func nativePublishPeer(t *testing.T, stage string) (*publishObservedConn, <-chan struct{}, <-chan error) {
	t.Helper()
	clientPipe, server := net.Pipe()
	client := &publishObservedConn{Conn: clientPipe, closed: make(chan struct{})}
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	entered, done := make(chan struct{}), make(chan error, 1)
	go func() {
		defer server.Close()
		done <- (publishProtocolPeer{Conn: server, clientClosed: client.closed}).run(stage, false, entered)
	}()
	return client, entered, done
}

func awaitNativePeer(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("native protocol peer did not terminate")
	}
}

func TestPublishPublicNativeAccessRefused(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"channel access refused", "connection access refused"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			client, entered, peerDone := nativePublishPeer(t, stage)
			r := newPublicPublisher(newPublishTestBroker())
			r.connection, r.channel, r.connectionCount = nil, nil, 0
			r.metadata.PublisherConfirm = true
			var dials atomic.Int32
			r.connectionDial = func(ctx context.Context, _ string, _ string, _ string, _ time.Duration, _ *tls.Config, _ bool) (rabbitMQConnectionBroker, rabbitMQChannelBroker, error) {
				dials.Add(1)
				r.metadata.ReconnectWait = time.Hour
				return dialWithConfig(ctx, "amqp://guest:guest@localhost/?heartbeat=0", amqp.Config{}, func(context.Context, string, string) (net.Conn, error) { return client, nil })
			}
			done := startPublicPublish(r, t.Context(), "topic")
			awaitPublishGate(t, entered)
			err := awaitPublish(t, done)
			var denied *amqp.Error
			require.ErrorAs(t, err, &denied)
			require.Equal(t, amqp.AccessRefused, denied.Code)
			require.True(t, denied.Server)
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
			require.Equal(t, "ACCESS_REFUSED - write permission denied", denied.Reason)
			require.Equal(t, int32(1), dials.Load())
			_ = client.Close()
			awaitNativePeer(t, peerDone)
		})
	}
}

func TestPublishPublicNativeUnconfirmed(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"unconfirmed success", "unconfirmed write"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			client, entered, peerDone := nativePublishPeer(t, stage)
			r := newPublicPublisher(newPublishTestBroker())
			r.connection, r.channel, r.connectionCount = nil, nil, 0
			r.metadata.PublisherConfirm = false
			r.connectionDial = func(ctx context.Context, _ string, _ string, _ string, _ time.Duration, _ *tls.Config, _ bool) (rabbitMQConnectionBroker, rabbitMQChannelBroker, error) {
				return dialWithConfig(ctx, "amqp://guest:guest@localhost/?heartbeat=0", amqp.Config{}, func(context.Context, string, string) (net.Conn, error) { return client, nil })
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- r.Publish(ctx, &pubsub.PublishRequest{Topic: "topic", Data: make([]byte, 16*1024)}) }()
			awaitPublishGate(t, entered)
			if stage == "unconfirmed write" {
				cancel()
				require.ErrorIs(t, awaitPublish(t, done), context.Canceled)
			} else {
				require.NoError(t, awaitPublish(t, done))
				cancel()
				select {
				case <-client.closed:
					t.Fatal("completed publication retained a cancellation callback")
				default:
				}
				require.NoError(t, r.Publish(t.Context(), &pubsub.PublishRequest{Topic: "topic"}))
				require.NoError(t, r.Close())
			}
			awaitNativePeer(t, peerDone)
		})
	}
}

func TestPublishPublicNativeCloseAndRecovery(t *testing.T) {
	t.Parallel()
	for range 20 {
		r := newPublicPublisher(newPublishTestBroker())
		r.connection, r.channel, r.connectionCount = nil, nil, 0
		r.metadata.PublisherConfirm = true
		peers := make([]<-chan error, 0, 2)
		r.connectionDial = func(ctx context.Context, _ string, _ string, _ string, _ time.Duration, _ *tls.Config, _ bool) (rabbitMQConnectionBroker, rabbitMQChannelBroker, error) {
			client, _, done := nativePublishPeer(t, "ack")
			peers = append(peers, done)
			return dialWithConfig(ctx, "amqp://guest:guest@localhost/?heartbeat=0", amqp.Config{}, func(context.Context, string, string) (net.Conn, error) { return client, nil })
		}
		require.NoError(t, r.Publish(t.Context(), &pubsub.PublishRequest{Topic: "topic"}))
		require.NoError(t, r.reconnect(t.Context(), r.connectionCount))
		awaitNativePeer(t, peers[0])
		require.NoError(t, r.Publish(t.Context(), &pubsub.PublishRequest{Topic: "topic"}))
		require.Equal(t, 2, r.connectionCount)
		require.NoError(t, r.Close())
		awaitNativePeer(t, peers[1])
	}
}

func TestInitNativeCancellationAndURITimeout(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"AMQP handshake", "confirm setup", "URI timeout"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			peerStage := stage
			if stage == "URI timeout" {
				peerStage = "AMQP handshake"
			}
			client, entered, peerDone := nativePublishPeer(t, peerStage)
			r := NewRabbitMQ(logger.NewLogger("test")).(*rabbitMQ)
			r.connectionDial = func(ctx context.Context, _ string, _ string, _ string, _ time.Duration, _ *tls.Config, _ bool) (rabbitMQConnectionBroker, rabbitMQChannelBroker, error) {
				uri := "amqp://guest:guest@localhost/?heartbeat=0"
				if stage == "URI timeout" {
					uri += "&connection_timeout=20"
				}
				return dialWithConfig(ctx, uri, amqp.Config{}, func(context.Context, string, string) (net.Conn, error) { return client, nil })
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			started := time.Now()
			go func() {
				done <- r.Init(ctx, pubsub.Metadata{Base: mdata.Base{Properties: map[string]string{metadataHostnameKey: "anyhost", metadataPublisherConfirmKey: "true"}}})
			}()
			awaitPublishGate(t, entered)
			if stage != "URI timeout" {
				if stage == "AMQP handshake" {
					remaining := time.Until(time.Unix(0, client.deadline.Load()))
					require.Greater(t, remaining, 25*time.Second)
					require.LessOrEqual(t, remaining, 30*time.Second)
				}
				cancel()
				require.ErrorIs(t, awaitPublish(t, done), context.Canceled)
			} else {
				require.Error(t, awaitPublish(t, done))
				require.NoError(t, ctx.Err(), "URI timeout must bound handshake independently of the caller")
				require.Less(t, time.Since(started), time.Second)
				deadline := time.Unix(0, client.deadline.Load())
				require.LessOrEqual(t, deadline.Sub(started), 100*time.Millisecond)
			}
			require.Nil(t, r.connection)
			require.Nil(t, r.channel)
			require.Zero(t, r.connectionCount)
			awaitPublishGate(t, client.closed)
			awaitNativePeer(t, peerDone)
		})
	}
}
