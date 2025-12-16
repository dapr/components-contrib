/*
Copyright 2021 The Dapr Authors
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

package statechange

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	r "github.com/dancannon/gorethink"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

// Binding represents RethinkDB change state input binding which fires handler with
// both the previous and current state store content each time there is a change.
type Binding struct {
	logger  logger.Logger
	session *r.Session
	config  StateConfig
	closed  atomic.Bool
	closeCh chan struct{}
	wg      sync.WaitGroup
}

// StateConfig is the binding config.
type StateConfig struct {
	ConnectOptsWrapper `mapstructure:",squash"`
	Table              string `mapstructure:"table"`
}

// ConnectOptsWrapper wraps r.ConnectOpts but excludes TLSConfig
// This is needed because the metadata decoder does not support nested structs with tags as inputs in the metadata.yaml file
type ConnectOptsWrapper struct {
	Address             string        `gorethink:"address,omitempty"`
	Addresses           []string      `gorethink:"addresses,omitempty"`
	Database            string        `gorethink:"database,omitempty"`
	Username            string        `gorethink:"username,omitempty"`
	Password            string        `gorethink:"password,omitempty"`
	AuthKey             string        `gorethink:"authkey,omitempty"`
	Timeout             time.Duration `gorethink:"timeout,omitempty"`
	WriteTimeout        time.Duration `gorethink:"write_timeout,omitempty"`
	ReadTimeout         time.Duration `gorethink:"read_timeout,omitempty"`
	KeepAlivePeriod     time.Duration `gorethink:"keep_alive_timeout,omitempty"`
	HandshakeVersion    int           `gorethink:"handshake_version,omitempty"`
	MaxIdle             int           `gorethink:"max_idle,omitempty"`
	InitialCap          int           `gorethink:"initial_cap,omitempty"`
	MaxOpen             int           `gorethink:"max_open,omitempty"`
	DiscoverHosts       bool          `gorethink:"discover_hosts,omitempty"`
	NodeRefreshInterval time.Duration `gorethink:"node_refresh_interval,omitempty"`
	UseJSONNumber       bool          `gorethink:"use_json_number,omitempty"`
	NumRetries          int           `gorethink:"num_retries,omitempty"`
	HostDecayDuration   time.Duration `gorethink:"host_decay_duration,omitempty"`
	UseOpentracing      bool          `gorethink:"use_opentracing,omitempty"`
	// TLS fields must be brought in as separate fields as they will not be processed by the metadata decoder properly without this
	EnableTLS  bool   `gorethink:"enable_tls,omitempty"`
	ClientCert string `gorethink:"client_cert,omitempty"`
	ClientKey  string `gorethink:"client_key,omitempty"`
}

// NewRethinkDBStateChangeBinding returns a new RethinkDB actor event input binding.
func NewRethinkDBStateChangeBinding(logger logger.Logger) bindings.InputBinding {
	return &Binding{
		logger:  logger,
		closeCh: make(chan struct{}),
	}
}

// Init initializes the RethinkDB binding.
func (b *Binding) Init(ctx context.Context, metadata bindings.Metadata) error {
	cfg, err := metadataToConfig(metadata.Properties, b.logger)
	if err != nil {
		return fmt.Errorf("unable to parse metadata properties: %w", err)
	}
	b.config = cfg

	// Convert wrapper to r.ConnectOpts
	connectOpts := r.ConnectOpts{
		Address:             cfg.Address,
		Addresses:           cfg.Addresses,
		Database:            cfg.Database,
		Username:            cfg.Username,
		Password:            cfg.Password,
		AuthKey:             cfg.AuthKey,
		Timeout:             cfg.Timeout,
		WriteTimeout:        cfg.WriteTimeout,
		ReadTimeout:         cfg.ReadTimeout,
		KeepAlivePeriod:     cfg.KeepAlivePeriod,
		HandshakeVersion:    r.HandshakeVersion(cfg.HandshakeVersion),
		MaxIdle:             cfg.MaxIdle,
		InitialCap:          cfg.InitialCap,
		MaxOpen:             cfg.MaxOpen,
		DiscoverHosts:       cfg.DiscoverHosts,
		NodeRefreshInterval: cfg.NodeRefreshInterval,
		UseJSONNumber:       cfg.UseJSONNumber,
		NumRetries:          cfg.NumRetries,
		HostDecayDuration:   cfg.HostDecayDuration,
		UseOpentracing:      cfg.UseOpentracing,
	}

	// Configure TLS if enabled
	if cfg.EnableTLS {
		tlsConfig, tlsErr := createTLSConfig(cfg.ClientCert, cfg.ClientKey)
		if tlsErr != nil {
			return fmt.Errorf("error creating TLS config: %w", tlsErr)
		}
		connectOpts.TLSConfig = tlsConfig
	}

	ses, err := r.Connect(connectOpts)
	if err != nil {
		return fmt.Errorf("error connecting to the database: %w", err)
	}
	b.session = ses

	return nil
}

// createTLSConfig creates a tls.Config from client certificate and key
func createTLSConfig(clientCert, clientKey string) (*tls.Config, error) {
	if clientCert == "" || clientKey == "" {
		return nil, errors.New("both client certificate and key are required for TLS")
	}

	cert, err := tls.X509KeyPair([]byte(clientCert), []byte(clientKey))
	if err != nil {
		return nil, fmt.Errorf("error parsing client certificate and key: %w", err)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// Read triggers the RethinkDB scheduler.
func (b *Binding) Read(ctx context.Context, handler bindings.Handler) error {
	if b.closed.Load() {
		return errors.New("binding is closed")
	}

	b.logger.Infof("subscribing to state changes in %s.%s...", b.config.Database, b.config.Table)
	cursor, err := r.DB(b.config.Database).
		Table(b.config.Table).
		Changes(r.ChangesOpts{
			IncludeTypes: true,
		}).
		Run(b.session, r.RunOpts{
			Context: ctx,
		})
	if err != nil {
		return fmt.Errorf("error connecting to table '%s': %w", b.config.Table, err)
	}

	readCtx, cancel := context.WithCancel(ctx)
	b.wg.Add(2)

	go func() {
		defer b.wg.Done()
		defer cancel()
		select {
		case <-b.closeCh:
		case <-readCtx.Done():
		}
	}()

	go func() {
		defer b.wg.Done()
		for readCtx.Err() == nil {
			var change any
			ok := cursor.Next(&change)
			if !ok {
				b.logger.Errorf("error detecting change: %v", cursor.Err())
				continue
			}

			data, err := json.Marshal(change)
			if err != nil {
				b.logger.Errorf("error marshalling change handler: %v", err)
			}
			b.logger.Debugf("event: %s", string(data))

			resp := &bindings.ReadResponse{
				Data: data,
				Metadata: map[string]string{
					"store-address":  b.config.Address,
					"store-database": b.config.Database,
					"store-table":    b.config.Table,
				},
			}

			if _, err := handler(readCtx, resp); err != nil {
				b.logger.Errorf("error invoking change handler: %v", err)
				continue
			}
		}

		cursor.Close()
	}()

	return nil
}

func (b *Binding) Close() error {
	if b.closed.CompareAndSwap(false, true) {
		close(b.closeCh)
	}
	defer b.wg.Wait()
	return b.session.Close()
}

func metadataToConfig(cfg map[string]string, _ logger.Logger) (StateConfig, error) {
	c := StateConfig{}

	// prepare metadata keys for decoding
	for k, v := range cfg {
		cfg[strings.ReplaceAll(k, "_", "")] = v
		delete(cfg, k)
	}

	err := kitmd.DecodeMetadata(cfg, &c)
	if err != nil {
		return c, err
	}

	return c, nil
}

// GetComponentMetadata returns the metadata of the component.
func (b *Binding) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := StateConfig{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType)
	return
}
