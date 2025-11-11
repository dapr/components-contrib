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

package sftpproxy

import (
	"errors"
	"io"
	"log"
	"net"
	"sync/atomic"
	"time"
)

type Proxy struct {
	ListenAddr        string
	UpstreamAddr      string
	Client            net.Conn
	Server            net.Conn
	ReconnectionCount atomic.Int32
	Listener          net.Listener
}

func (p *Proxy) ListenAndServe() error {
	ln, err := net.Listen("tcp", p.ListenAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("Proxy listening on %s -> %s", p.ListenAddr, p.UpstreamAddr)
	p.Listener = ln

	for {
		client, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			log.Printf("accept error: %v", err)
			continue
		}
		go p.handle(client)
	}
}

func (p *Proxy) handle(client net.Conn) {
	defer client.Close()

	// Connect to upstream SFTP server
	var server net.Conn
	var err error
	for i := 0; i < 10 && server == nil; i++ {
		server, err = net.Dial("tcp", p.UpstreamAddr)
		if err != nil {
			log.Printf("dial upstream: %v", err)
			time.Sleep(200 * time.Millisecond)
		}
	}

	if server == nil {
		log.Printf("failed to connect to upstream after 5 attempts")
		return
	}
	defer server.Close()

	p.Client = client
	p.Server = server
	p.ReconnectionCount.Add(1)
	errCh := make(chan error, 2)

	// client -> server
	go func() {
		_, cErr := io.Copy(server, client)
		errCh <- cErr
	}()

	// server -> client
	go func() {
		_, cErr := io.Copy(client, server)
		errCh <- cErr
	}()

	// When either direction ends, close both ends
	if err := <-errCh; err != nil && !isUsefullyClosed(err) {
		log.Printf("proxy stream ended with error: %v", err)
	}
}

func (p *Proxy) KillServerConn() error {
	return p.Server.Close()
}

func (p *Proxy) Close() {
	if p.Client != nil {
		_ = p.Client.Close()
	}

	if p.Server != nil {
		_ = p.Server.Close()
	}

	if p.Listener != nil {
		_ = p.Listener.Close()
	}

	p.ReconnectionCount.Store(0)
}

// isUsefullyClosed filters common close conditions from logging noise
func isUsefullyClosed(err error) bool {
	return err == io.EOF || errors.Is(err, net.ErrClosed)
}
