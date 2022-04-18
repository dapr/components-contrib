package sqlite

import (
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
	var ops = make([]state.TransactionalStateOperation, len(req))
	for _, r := range req {
		ops = append(ops, state.TransactionalStateOperation{
			Operation: state.Delete,
			Request:   r,
		})
	}
	return s.dbaccess.ExecuteMulti(ops)
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
	var ops = make([]state.TransactionalStateOperation, len(req))
	for _, r := range req {
		ops = append(ops, state.TransactionalStateOperation{
			Operation: state.Upsert,
			Request:   r,
		})
	}
	return s.dbaccess.ExecuteMulti(ops)
}

// Multi handles multiple transactions. Implements TransactionalStore.
func (s *Sqlite) Multi(request *state.TransactionalStateRequest) error {
	return s.dbaccess.ExecuteMulti(request.Operations)
}

// Close implements io.Closer.
func (s *Sqlite) Close() error {
	if s.dbaccess != nil {
		return s.dbaccess.Close()
	}

	return nil
}
