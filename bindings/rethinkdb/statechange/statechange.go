// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package statechange

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	r "github.com/dancannon/gorethink"
	"github.com/dapr/components-contrib/bindings"
	db "github.com/dapr/components-contrib/state/rethinkdb"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/pkg/errors"
)

func init() {
	r.Log.Out = os.Stderr
	r.SetTags("rethinkdb", "json")
}

// ActorBinding represents RethinkDB input binding for actors
type ActorBinding struct {
	logger  logger.Logger
	session *r.Session
	config  *BindingConfig
	stopCh  chan bool
}

// BindingConfig represents configuration for NewRethinkDB
type BindingConfig struct {
	r.ConnectOpts
	Table string `json:"table"`
}

var _ = bindings.InputBinding(&ActorBinding{})

// NewRethinkDBActorBinding returns a new RethinkDB actor event input binding
func NewRethinkDBActorBinding(logger logger.Logger) *ActorBinding {
	return &ActorBinding{
		logger: logger,
		stopCh: make(chan bool),
	}
}

// Init initializes the RethinkDB binding
func (b *ActorBinding) Init(metadata bindings.Metadata) error {
	cfg, err := metadataToConfig(metadata.Properties)
	if err != nil {
		return errors.Wrap(err, "unable to parse metadata properties")
	}

	ses, err := r.Connect(cfg.ConnectOpts)
	if err != nil {
		return errors.Wrap(err, "error connecting to the database")
	}

	b.session = ses
	b.config = cfg

	return nil
}

// Read triggers the RethinkDB scheduler
func (b *ActorBinding) Read(handler func(*bindings.ReadResponse) error) error {
	b.logger.Infof("subscribing to changes in table: %s", b.config.Table)
	cursor, err := r.DB(b.config.Database).Table(b.config.Table).Changes(r.ChangesOpts{
		IncludeTypes: true,
	}).Run(b.session)
	if err != nil {
		return errors.Wrapf(err, "error connecting to table: %s", b.config.Table)
	}
	defer cursor.Close()

	go func() {
		var change struct {
			NewState   *db.StateRecord `rethinkdb:"new_val" json:"new_val"`
			OldState   *db.StateRecord `rethinkdb:"old_val" json:"old_val"`
			ChangeType string          `rethinkdb:"type" json:"change_type"`
			ChangeTime int64           `rethinkdb:"-" json:"change_time"`
		}

		for {
			ok := cursor.Next(&change)
			if !ok {
				b.logger.Errorf("error detecting change: %v", cursor.Err())
				return
			}

			if change.NewState != nil {
				change.ChangeTime = time.Now().UTC().UnixNano()
				data, err := json.Marshal(change)
				if err != nil {
					b.logger.Errorf("error detecting change: %v", err)
					return
				}
				resp := &bindings.ReadResponse{
					Data: data,
					Metadata: map[string]string{
						"database":    b.config.Database,
						"table":       b.config.Table,
						"change-type": change.ChangeType,
					},
				}

				if err := handler(resp); err != nil {
					b.logger.Errorf("error invoking handler: %v", err)
				}
			}
		}
	}()

	<-b.stopCh
	b.logger.Debugf("stopping change notification for: %s", b.config.Table)
	return nil
}

// Invoke exposes way to stop previously started cron
func (b *ActorBinding) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	b.logger.Debugf("operation: %v", req.Operation)
	if req.Operation != bindings.DeleteOperation {
		return nil, fmt.Errorf("invalid operation: '%v', only '%v' supported",
			req.Operation, bindings.DeleteOperation)
	}
	b.stopCh <- true
	return &bindings.InvokeResponse{
		Metadata: map[string]string{
			"database": b.config.Database,
			"table":    b.config.Table,
		},
	}, nil
}

// Operations method returns the supported operations by this binding
func (b *ActorBinding) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		bindings.DeleteOperation,
	}
}

func metadataToConfig(cfg map[string]string) (*BindingConfig, error) {

	c := BindingConfig{}
	for k, v := range cfg {
		switch k {
		case "table": //string
			c.Table = v
		case "address": //string
			c.Address = v
		case "addresses": // []string
			c.Addresses = strings.Split(v, ",")
		case "database": //string
			c.Database = v
		case "username": //string
			c.Username = v
		case "password": //string
			c.Password = v
		case "authkey": //string
			c.AuthKey = v
		case "timeout": //time.Duration
			d, err := time.ParseDuration(v)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid timeout format: %v", v)
			}
			c.Timeout = d
		case "write_timeout": //time.Duration
			d, err := time.ParseDuration(v)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid write timeout format: %v", v)
			}
			c.WriteTimeout = d
		case "read_timeout": //time.Duration
			d, err := time.ParseDuration(v)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid read timeout format: %v", v)
			}
			c.ReadTimeout = d
		case "keep_alive_timeout": //time.Duration
			d, err := time.ParseDuration(v)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid keep alive timeout format: %v", v)
			}
			c.KeepAlivePeriod = d
		case "initial_cap": //int
			i, err := strconv.Atoi(v)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid keep initial cap format: %v", v)
			}
			c.InitialCap = i
		case "max_open": //int
			i, err := strconv.Atoi(v)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid keep max open format: %v", v)
			}
			c.MaxOpen = i
		case "discover_hosts": //bool
			b, err := strconv.ParseBool(v)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid discover hosts format: %v", v)
			}
			c.DiscoverHosts = b
		case "use-open-tracing": //bool
			b, err := strconv.ParseBool(v)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid use open tracing format: %v", v)
			}
			c.UseOpentracing = b
		case "node_refresh_interval": //time.Duration
			d, err := time.ParseDuration(v)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid keep node refresh interval format: %v", v)
			}
			c.NodeRefreshInterval = d
		case "max_idle": //int
			i, err := strconv.Atoi(v)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid keep max idle format: %v", v)
			}
			c.MaxIdle = i
		default:
			return nil, errors.Errorf("unrecognized metadata: %s", k)
		}
	}

	if c.Address == "" && len(c.Addresses) < 1 {
		return nil, errors.New("invalid configuration, address or addresses required")
	}

	if c.Table == "" {
		return nil, errors.New("invalid configuration, table required")
	}

	return &c, nil
}
