/*
Copyright 2023 The Dapr Authors
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

package transactions

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dapr/kit/logger"
)

// ExecuteInTransaction executes a function in a transaction.
// If the handler returns an error, the transaction is rolled back automatically.
func ExecuteInTransaction[T any](ctx context.Context, log logger.Logger, db *sql.DB, fn func(ctx context.Context, tx *sql.Tx) (T, error)) (res T, err error) {
	// Start the transaction
	// Note that the context here is tied to the entire transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return res, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Rollback in case of failure
	var success bool
	defer func() {
		if success {
			return
		}
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			// Log errors only
			log.Errorf("Error while attempting to roll back transaction: %v", rollbackErr)
		}
	}()

	// Execute the callback
	res, err = fn(ctx, tx)
	if err != nil {
		return res, err
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		return res, fmt.Errorf("failed to commit transaction: %w", err)
	}
	success = true

	return res, nil
}
