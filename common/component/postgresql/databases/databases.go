package databases

import (
	"context"
	"fmt"
	"time"

	pginterfaces "github.com/dapr/components-contrib/common/component/postgresql/interfaces"
	"github.com/jackc/pgx/v5"
)

const (
	createDatabaseTmpl = "CREATE DATABASE $1"
	deleteDatabaseTmpl = "DROP DATABASE IF EXISTS $1 WITH (FORCE)"
	databaseExistsTmpl = "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)"
	userExistsTmpl     = "SELECT CAST((SELECT 1 FROM pg_roles WHERE rolname = $1) AS BOOLEAN)"
	createUserTmpl     = "CREATE USER $1"
	checkRoleTmpl      = "SELECT 1 FROM pg_roles WHERE rolname = $1 AND 'rds_iam' = rolname"
	grantRoleTmpl      = "GRANT $1 TO $2"

	awsRole = "rds_iam"
)

func CreateDatabaseIfNeeded(ctx context.Context, timeout time.Duration, connectionString string, db pginterfaces.PGXPoolConn) error {
	config, err := pgx.ParseConfig(connectionString)
	if err != nil {
		return fmt.Errorf("failed to parse PostgreSQL config: %w", err)
	}

	// check if database exists in connection string
	var dbExists bool
	dbExistsCtx, dbExistsCancel := context.WithTimeout(ctx, timeout)
	err = db.QueryRow(dbExistsCtx, databaseExistsTmpl, config.Database).Scan(&dbExists)
	dbExistsCancel()
	if err != nil && err != pgx.ErrNoRows {
		return fmt.Errorf("failed to check if the PostgreSQL database %s exists: %w", config.Database, err)
	}

	// Create database if needed using master password in connection string
	if !dbExists {
		createDBCtx, createDBCancel := context.WithTimeout(ctx, timeout)
		_, err := db.Exec(createDBCtx, createDatabaseTmpl, config.Database)
		createDBCancel()
		if err != nil {
			return fmt.Errorf("failed to create PostgreSQL user: %w", err)
		}
	}
	return nil
}

func CreateUserAndRoleIfNeeded(ctx context.Context, timeout time.Duration, connectionString string, db pginterfaces.PGXPoolConn) error {
	config, err := pgx.ParseConfig(connectionString)
	if err != nil {
		return fmt.Errorf("failed to parse PostgreSQL config: %w", err)
	}

	var userExists bool
	userExistsCtx, userExistsCancel := context.WithTimeout(ctx, timeout)
	err = db.QueryRow(userExistsCtx, userExistsTmpl, config.User).Scan(&userExists)
	userExistsCancel()
	if err != nil && err != pgx.ErrNoRows {
		// If user does not exist, create the user without pwd for now.
		// This will be rotated to use proper iam credentials before connecting again.
		// However, this will also prevent the user from being locked out in the event the sidecar has startup issues.
		_, err = db.Exec(ctx, createUserTmpl, config.User)
		if err != nil {
			return fmt.Errorf("failed to create PostgreSQL user: %w", err)
		}
	}

	// Check if the role is already granted
	var roleGranted bool
	roleGrantedCtx, roleGrantedCancel := context.WithTimeout(ctx, timeout)
	err = db.QueryRow(roleGrantedCtx, checkRoleTmpl, config.User).Scan(&roleGranted)
	roleGrantedCancel()
	if err != nil && err != pgx.ErrNoRows {
		return fmt.Errorf("failed to check if the role %v is already granted to the PostgreSQL user %s: %w", awsRole, config.User, err)
	}

	// Grant the role if it's not already granted
	// Note: granting the postgres (ie root) user AWS IAM auth may lock the user out of the database,
	// so we should prevent this.
	if !roleGranted && config.User != "postgres" {
		grantRoleCtx, grantRoleCancel := context.WithTimeout(ctx, timeout)
		_, err := db.Exec(grantRoleCtx, grantRoleTmpl, awsRole, config.User)
		grantRoleCancel()
		if err != nil {
			return fmt.Errorf("failed to grant PostgreSQL user role: %w", err)
		}
	}
	return nil
}
