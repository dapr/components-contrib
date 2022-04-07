package sqlite

const (
	connectionStringKey         = "connectionString"
	metadataTTLKey              = "ttlInSeconds"
	errMissingConnectionString  = "missing connection string"
	tableNameKey                = "tableName"
	cleanupIntervalKey          = "cleanupIntervalInSeconds"
	defaultTableName            = "state"
	defaultCleanupInternalInSec = 3600

	createTableTpl = `
      CREATE TABLE %s (
				key TEXT NOT NULL PRIMARY KEY,
				value TEXT NOT NULL,
				is_binary BOOLEAN NOT NULL,
				etag TEXT NOT NULL,
				creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
				expiration_time TIMESTAMP DEFAULT NULL,
				update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
			);`

	createTableExpirationTimeIdx = `
			CREATE INDEX idx_%s_expiration_time ON %s(expiration_time)`

	tableExistsStmt = `
		SELECT EXISTS (
			SELECT name FROM sqlite_master WHERE type='table' AND name = ?
		) AS 'exists'`

	cleanupTimeoutStmtTpl = `
		DELETE FROM %s WHERE expiration_time IS NOT NULL AND expiration_time < CURRENT_TIMESTAMP`

	getValueTpl = `
		SELECT value, is_binary, etag FROM %s
	  	WHERE key = ?
	      AND (expiration_time IS NULL OR expiration_time > CURRENT_TIMESTAMP)`

	delValueTpl         = "DELETE FROM %s WHERE key = ?"
	delValueWithETagTpl = "DELETE FROM %s WHERE key = ? and etag = ?"

	expirationTpl = "DATETIME(CURRENT_TIMESTAMP, '+%d seconds')"

	setValueTpl = `
		INSERT OR REPLACE INTO %s (
			key, value, is_binary, etag, update_time, expiration_time, creation_time)
		VALUES(?, ?, ?, ?, CURRENT_TIMESTAMP, %s,
			(SELECT creation_time FROM %s WHERE key=?));`
	setValueWithETagTpl = `
		UPDATE %s SET value = ?, etag = ?, is_binary = ?, update_time = CURRENT_TIMESTAMP, expiration_time = %s
		WHERE key = ? AND eTag = ?;`
)
