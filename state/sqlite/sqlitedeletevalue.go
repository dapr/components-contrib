package sqlite

import (
	"database/sql"
	"fmt"

	"github.com/dapr/components-contrib/state"
)

// Parsed DeleteRequest.
type deleteRequest struct {
	dbAccess *sqliteDBAccess

	tx          *sql.Tx
	key         string
	concurrency *string
	etag        *string
}

func prepareDeleteRequest(a *sqliteDBAccess, tx *sql.Tx, req *state.DeleteRequest) (*deleteRequest, error) {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return nil, err
	}

	if req.Key == "" {
		return nil, fmt.Errorf("missing key in delete operation")
	}

	if req.Options.Concurrency == state.FirstWrite && (req.ETag == nil || len(*req.ETag) == 0) {
		a.logger.Debugf("when FirstWrite is to be enforced, a value must be provided for the ETag")
		return nil, fmt.Errorf("when FirstWrite is to be enforced, a value must be provided for the ETag")
	}
	return &deleteRequest{
		dbAccess: a,
		tx:       tx,

		key:         req.Key,
		concurrency: &req.Options.Concurrency,
		etag:        req.ETag,
	}, nil
}

// Returns if any value deleted, or an execution error.
func (req *deleteRequest) deleteValue() (bool, error) {
	tableName := req.dbAccess.tableName
	tx := req.tx

	var result sql.Result
	var err error
	if *req.concurrency != state.FirstWrite {
		// Sprintf is required for table name because sql.DB does not substitute parameters for table names.
		stmt := fmt.Sprintf(delValueTpl, tableName)
		result, err = tx.Exec(stmt, req.key)
	} else {
		// Sprintf is required for table name because sql.DB does not substitute parameters for table names.
		stmt := fmt.Sprintf(delValueWithETagTpl, tableName)
		result, err = tx.Exec(stmt, req.key, *req.etag)
	}

	if err != nil {
		return false, err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows == 1, nil
}
