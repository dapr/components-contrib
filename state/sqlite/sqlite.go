package sqlite

import (
	"fmt"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

// SQLite Database state store.
type Sqlite struct {
	features []state.Feature
	logger   logger.Logger
	dbaccess dbAccess
}

// NewSqliteStateStore creates a new instance of Sqlite state store.
func NewSqliteStateStore(logger logger.Logger) *Sqlite {
	dba := newSqliteDBAccess(logger)

	return newSqliteStateStore(logger, dba)
}

// newSqliteStateStore creates a newSqliteStateStore instance of an Sqlite state store.
// This unexported constructor allows injecting a dbAccess instance for unit testing.
func newSqliteStateStore(logger logger.Logger, dba dbAccess) *Sqlite {
	return &Sqlite{
		features: []state.Feature{state.FeatureETag, state.FeatureTransactional},
		logger:   logger,
		dbaccess: dba,
	}
}

// Init initializes the Sql server state store.
func (s *Sqlite) Init(metadata state.Metadata) error {
	return s.dbaccess.Init(metadata)
}

func (s *Sqlite) Ping() error {
	return s.dbaccess.Ping()
}

// Features returns the features available in this state store.
func (s *Sqlite) Features() []state.Feature {
	return s.features
}

// Delete removes an entity from the store.
func (s *Sqlite) Delete(req *state.DeleteRequest) error {
	return s.dbaccess.Delete(req)
}

// BulkDelete removes multiple entries from the store.
func (s *Sqlite) BulkDelete(req []state.DeleteRequest) error {
	return s.dbaccess.ExecuteMulti(nil, req)
}

// Get returns an entity from store.
func (s *Sqlite) Get(req *state.GetRequest) (*state.GetResponse, error) {
	return s.dbaccess.Get(req)
}

// BulkGet performs a bulks get operations.
func (s *Sqlite) BulkGet(req []state.GetRequest) (bool, []state.BulkGetResponse, error) {
	// TODO: replace with ExecuteMulti for performance.
	return false, nil, nil
}

// Set adds/updates an entity on store.
func (s *Sqlite) Set(req *state.SetRequest) error {
	return s.dbaccess.Set(req)
}

// BulkSet adds/updates multiple entities on store.
func (s *Sqlite) BulkSet(req []state.SetRequest) error {
	return s.dbaccess.ExecuteMulti(req, nil)
}

// Multi handles multiple transactions. Implements TransactionalStore.
func (s *Sqlite) Multi(request *state.TransactionalStateRequest) error {
	var deletes []state.DeleteRequest
	var sets []state.SetRequest
	for _, req := range request.Operations {
		switch req.Operation {
		case state.Upsert:
			if setReq, ok := req.Request.(state.SetRequest); ok {
				sets = append(sets, setReq)
			} else {
				return fmt.Errorf("expecting set request")
			}

		case state.Delete:
			if delReq, ok := req.Request.(state.DeleteRequest); ok {
				deletes = append(deletes, delReq)
			} else {
				return fmt.Errorf("expecting delete request")
			}

		default:
			return fmt.Errorf("unsupported operation: %s", req.Operation)
		}
	}

	if len(sets) > 0 || len(deletes) > 0 {
		return s.dbaccess.ExecuteMulti(sets, deletes)
	}

	return nil
}

// Close implements io.Closer.
func (s *Sqlite) Close() error {
	if s.dbaccess != nil {
		return s.dbaccess.Close()
	}

	return nil
}
