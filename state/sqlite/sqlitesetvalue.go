package sqlite

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/google/uuid"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
)

// Parsed Set Request.
type setRequest struct {
	dbAccess sqliteDBAccess
	tx       *sql.Tx

	key         string
	value       string
	isBinary    bool
	ttlSeconds  *int64
	concurrency *string
	etag        *string
}

func prepareSetRequest(a *sqliteDBAccess, tx *sql.Tx, req *state.SetRequest) (*setRequest, error) {
	err := checkRequestOptions(a, req)
	if err != nil {
		return nil, err
	}

	if req.Key == "" {
		return nil, fmt.Errorf("missing key in set option")
	}

	if v, ok := req.Value.(string); ok && v == "" {
		return nil, fmt.Errorf("empty string is not allowed in st operation")
	}

	ttlSeconds, err := parseTTL(req.Metadata, a.logger)
	if err != nil {
		return nil, fmt.Errorf("error in parsing TTL %w", err)
	}

	requestValue := req.Value
	byteArray, isBinary := req.Value.([]uint8)
	if isBinary {
		requestValue = base64.StdEncoding.EncodeToString(byteArray)
	}

	// Convert to json string.
	bt, err := utils.Marshal(requestValue, json.Marshal)
	if err != nil {
		return nil, err
	}
	value := string(bt)

	return &setRequest{
		dbAccess:    *a,
		tx:          tx,
		key:         req.Key,
		value:       value,
		concurrency: &req.Options.Concurrency,
		ttlSeconds:  ttlSeconds,
		isBinary:    isBinary,
		etag:        req.ETag,
	}, nil
}

func (req *setRequest) setValue() (bool, error) {
	newEtag := uuid.New().String()
	tx := req.tx
	tableName := req.dbAccess.tableName

	// Only check for etag if FirstWrite specified (ref oracledatabaseaccess)
	var res sql.Result
	var err error
	if req.etag == nil || *req.etag == "" {
		// Reset expiration time in case of an update
		var expiration string
		if req.ttlSeconds != nil {
			expiration = fmt.Sprintf(expirationTpl, *req.ttlSeconds)
		} else {
			expiration = "NULL"
		}
		// Sprintf is required for table name because sql.DB does not substitute parameters for table names.
		// And the same is for DATETIME function's seconds parameter.
		stmt := fmt.Sprintf(setValueTpl, tableName, expiration, tableName)
		res, err = tx.Exec(stmt, req.key, req.value, req.isBinary, newEtag, req.key)
	} else {
		// First write, existing record has to be updated
		var expiration string
		if req.ttlSeconds != nil {
			expiration = fmt.Sprintf(expirationTpl, *req.ttlSeconds)
		} else {
			expiration = "NULL"
		}
		// Sprintf is required for table name because sql.DB does not substitute parameters for table names.
		// And the same is for DATETIME function's seconds parameter.
		stmt := fmt.Sprintf(setValueWithETagTpl, tableName, expiration)
		res, err = tx.Exec(stmt, req.value, newEtag, req.isBinary, req.key, *req.etag)
	}

	if err != nil {
		return false, err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows == 1, nil
}

func checkRequestOptions(a *sqliteDBAccess, req *state.SetRequest) error {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}

	if req.Options.Concurrency == state.FirstWrite {
		if req.ETag == nil || len(*req.ETag) == 0 {
			a.logger.Debugf("when FirstWrite is to be enforced, a value must be provided for the ETag")
			return fmt.Errorf("when FirstWrite is to be enforced, a value must be provided for the ETag")
		}
	} else if req.Options.Concurrency == state.LastWrite {
		if req.ETag != nil {
			a.logger.Warn("when LastWrite is set, ignore the etag")
			req.ETag = nil
		}
	}

	return nil
}

// Returns nil or non-negative value, nil means never expire.
func parseTTL(requestMetadata map[string]string, logger logger.Logger) (*int64, error) {
	if val, found := requestMetadata[metadataTTLKey]; found && val != "" {
		parsedInt, err := strconv.ParseInt(val, 10, 0)
		if err != nil {
			return nil, fmt.Errorf("error in parsing ttl metadata : %w", err)
		}

		if parsedInt < -1 {
			return nil, fmt.Errorf("incorrect value for %s %d", metadataTTLKey, parsedInt)
		} else if parsedInt == -1 {
			logger.Debugf("TTL is set to -1; this means: never expire.")
			return nil, nil
		}

		return &parsedInt, nil
	}

	return nil, nil
}
