package awsiam

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	aws_config "github.com/aws/aws-sdk-go-v2/config"
	aws_credentials "github.com/aws/aws-sdk-go-v2/credentials"
	aws_auth "github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	pginterfaces "github.com/dapr/components-contrib/common/component/postgresql/interfaces"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	createDatabaseTmpl = "CREATE DATABASE %s"
	deleteDatabaseTmpl = "DROP DATABASE IF EXISTS %s WITH (FORCE)"
	databaseExistsTmpl = "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)"
	userExistsTmpl     = "SELECT CAST((SELECT 1 FROM pg_roles WHERE rolname = $1) AS BOOLEAN)"
	createUserTmpl     = "CREATE USER %v"
	checkRoleTmpl      = "SELECT 1 FROM pg_roles WHERE rolname = $1 AND 'rds_iam' = rolname"
	grantRoleTmpl      = "GRANT %v TO %v"

	awsRole = "rds_iam"
)

var (
	// Define a regular expression to match the 'dbname' parameter in the connection string
	databaseNameRegex = regexp.MustCompile(`\bdbname=([^ ]+)\b`)
	userRegex         = regexp.MustCompile(`\buser=([^ ]+)\b`)
	passwordRegex     = regexp.MustCompile(`\bpassword=([^ ]+)\b`)
)

type AWSIAM struct {
	// Ignored by metadata parser because included in built-in authentication profile
	// access key to use for accessing sqs/sns.
	AWSAccessKey string `json:"awsAccessKey" mapstructure:"awsAccessKey" mdignore:"true"`
	// secret key to use for accessing sqs/sns.
	AWSSecretKey string `json:"awsSecretKey" mapstructure:"awsSecretKey" mdignore:"true"`
	// aws session token to use.
	AWSSessionToken string `mapstructure:"awsSessionToken" mdignore:"true"`
	// aws region in which SNS/SQS should create resources.
	AWSRegion string `mapstructure:"awsRegion"`
}

func GetAccessToken(ctx context.Context, pgCfg *pgx.ConnConfig, accessKey, secretKey string) (string, error) {
	var dbEndpoint string = fmt.Sprintf("%s:%d", pgCfg.Host, pgCfg.Port)
	var authenticationToken string

	// https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/UsingWithRDS.IAMDBAuth.Connecting.Go.html
	// Default to load default config through aws credentials file (~/.aws/credentials)
	awsCfg, err := aws_config.LoadDefaultConfig(ctx)
	if err != nil {
		// otherwise use metadata fields
		// Check if access key and secret access key are set
		accessKey := accessKey
		secretKey := secretKey

		// Validate if access key and secret access key are provided
		if accessKey == "" || secretKey == "" {
			return "", fmt.Errorf("failed to load default configuration for AWS using accessKey and secretKey")
		}

		// Set credentials explicitly
		var awsCfg2 aws.CredentialsProvider
		awsCfg2 = aws_credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")
		if awsCfg2 == nil {
			return "", fmt.Errorf("failed to get accessKey and secretKey for AWS")
		}

		authenticationToken, err = aws_auth.BuildAuthToken(
			ctx, dbEndpoint, awsCfg.Region, pgCfg.User, awsCfg2)
		if err != nil {
			return "", fmt.Errorf("failed to create AWS authentication token: %v", err)
		}

		return authenticationToken, nil
	}

	authenticationToken, err = aws_auth.BuildAuthToken(
		ctx, dbEndpoint, awsCfg.Region, pgCfg.User, awsCfg.Credentials)
	if err != nil {
		return "", fmt.Errorf("failed to create AWS authentication token: %v", err)
	}

	return authenticationToken, nil
}

// Replace the value of 'database' with 'postgres'
func GetPostgresDBConnString(connString string) string {
	newConnStr := userRegex.ReplaceAllString(connString, "user=postgres")
	return databaseNameRegex.ReplaceAllString(newConnStr, "dbname=postgres")
}

func parseDatabaseFromConnectionString(connectionString string) (string, error) {
	match := databaseNameRegex.FindStringSubmatch(connectionString)
	if len(match) < 2 {
		return "", fmt.Errorf("unable to find database name ('dbname' field) in the connection string HERE SAM %v MATCH: %v", connectionString, match)
	}

	// Return the value after "dbname="
	return match[1], nil
}

func CreateDatabaseIfNeeded(ctx context.Context, timeout time.Duration, connectionString string, db pginterfaces.PGXPoolConn) error {
	// Parse database name from connection string
	dbName, err := parseDatabaseFromConnectionString(connectionString)
	if err != nil {
		return fmt.Errorf("failed to parse database name from connection string to create database %v", err)
	}

	// check if database exists in connection string
	var dbExists bool
	dbExistsCtx, dbExistsCancel := context.WithTimeout(ctx, timeout)
	err = db.QueryRow(dbExistsCtx, databaseExistsTmpl, dbName).Scan(&dbExists)
	dbExistsCancel()
	if err != nil && err != pgx.ErrNoRows {
		return fmt.Errorf("failed to check if the PostgreSQL database %s exists: %v", dbName, err)
	}

	// Create database if needed using master password in connection string
	if !dbExists {
		createDbCtx, createDbCancel := context.WithTimeout(ctx, timeout)
		_, err := db.Exec(createDbCtx, fmt.Sprintf(createDatabaseTmpl, dbName))
		createDbCancel()
		if err != nil {
			return fmt.Errorf("failed to create PostgreSQL user: %v", err)
		}
	}
	return nil
}

func CreateUserAndRoleIfNeeded(ctx context.Context, timeout time.Duration, connectionString string, db pginterfaces.PGXPoolConn) error {
	// Parse database name from connection string
	dbName, err := parseDatabaseFromConnectionString(connectionString)
	if err != nil {
		return fmt.Errorf("failed to parse database name from connection string to create database %v", err)
	}

	var userExists bool
	userExistsCtx, userExistsCancel := context.WithTimeout(ctx, timeout)
	err = db.QueryRow(userExistsCtx, userExistsTmpl, dbName).Scan(&userExists)
	userExistsCancel()
	if err != nil && err != pgx.ErrNoRows {
		return fmt.Errorf("failed to check if the PostgreSQL user %s exists: %v", dbName, err)
	}

	// Create the user if it doesn't exist
	if !userExists {
		_, err := db.Exec(ctx, fmt.Sprintf(createUserTmpl, dbName))
		if err != nil {
			return fmt.Errorf("failed to create PostgreSQL user: %v", err)
		}
	}

	// Check if the role is already granted
	var roleGranted bool
	roleGrantedCtx, roleGrantedCancel := context.WithTimeout(ctx, timeout)
	err = db.QueryRow(roleGrantedCtx, checkRoleTmpl, dbName).Scan(&roleGranted)
	roleGrantedCancel()
	if err != nil && err != pgx.ErrNoRows {
		return fmt.Errorf("failed to check if the role %v is already granted to the PostgreSQL user %s: %v", awsRole, dbName, err)
	}

	// Grant the role if it's not already granted
	if !roleGranted {
		grantRoleCtx, grantRoleCancel := context.WithTimeout(ctx, timeout)
		_, err := db.Exec(grantRoleCtx, fmt.Sprintf(grantRoleTmpl, awsRole, dbName))
		grantRoleCancel()
		if err != nil {
			return fmt.Errorf("failed to grant PostgreSQL user role: %v", err)
		}
	}
	return nil
}

func InitAWSDatabase(ctx context.Context, config *pgxpool.Config, db pginterfaces.PGXPoolConn, timeout time.Duration, connString string, awsAccessKey, awsSecretKey string) error {
	err := CreateDatabaseIfNeeded(ctx, timeout, connString, db)
	if err != nil {
		return fmt.Errorf("failed create AWS database if needed %v", err)
	}

	err = CreateUserAndRoleIfNeeded(ctx, timeout, connString, db)
	if err != nil {
		return fmt.Errorf("failed create AWS user and grant role if needed %v", err)
	}

	// Set max connection lifetime to 14 minutes in postgres connection pool configuration.
	// Note: this will refresh connections before the 15 min expiration on the IAM AWS auth token,
	// while leveraging the BeforeConnect hook to recreate the token in time dynamically.
	config.MaxConnLifetime = time.Minute * 14

	// Setup connection pool config needed for AWS IAM authentication
	config.BeforeConnect = func(ctx context.Context, pgConfig *pgx.ConnConfig) error {

		// Manually reset auth token with aws and reset the config password using the new iam token
		pwd, err := GetAccessToken(ctx, pgConfig, awsAccessKey, awsSecretKey)
		if err != nil {
			return fmt.Errorf("failed to refresh access token for iam authentication with postgresql: %v", err)
		}

		pgConfig.Password = pwd
		return nil
	}

	// TODO: add clean up hooks on user/db?
	return nil
}
