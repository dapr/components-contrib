package sqlite

import (
	"github.com/dapr/components-contrib/state"
)

// dbAccess is a private interface which enables unit testing of SQLite.
type dbAccess interface {
	Init(metadata state.Metadata) error
	Ping() error
	Set(req *state.SetRequest) error
	Get(req *state.GetRequest) (*state.GetResponse, error)
	Delete(req *state.DeleteRequest) error
	ExecuteMulti(reqs []state.TransactionalStateOperation) error
	Close() error
}
