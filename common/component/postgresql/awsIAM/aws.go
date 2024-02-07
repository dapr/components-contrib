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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	pginterfaces "github.com/dapr/components-contrib/common/component/postgresql/interfaces"
)

const (
	createDatabaseTmpl = "CREATE DATABASE %s"
	deleteDatabaseTmpl = "DROP DATABASE IF EXISTS %s WITH (FORCE)"
	databaseExistsTmpl = "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)"
	userExistsTmpl     = "SELECT CAST((SELECT 1 FROM pg_roles WHERE rolname = $1) AS BOOLEAN)"
	createUserTmpl     = "CREATE USER %v WITH PASSWORD '%v'"
	checkRoleTmpl      = "SELECT 1 FROM pg_roles WHERE rolname = $1 AND 'rds_iam' = rolname"
	grantRoleTmpl      = "GRANT %v TO %v"

	awsRole = "rds_iam"
)

var (
	// Define a regular expression to match the 'dbname' parameter in the connection string
	databaseNameRegex     = regexp.MustCompile(`\bdbname=([^ ]+)\b`)
	databaseUserRegex     = regexp.MustCompile(`\buser=([^ ]+)\b`)
	databasePasswordRegex = regexp.MustCompile(`\bpassword=([^ ]+)\b`)
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

func GetAccessToken(ctx context.Context, pgCfg *pgx.ConnConfig, region, accessKey, secretKey string) (string, error) {
	var dbEndpoint = fmt.Sprintf("%s:%d", pgCfg.Host, pgCfg.Port)
	var authenticationToken string

	// https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/UsingWithRDS.IAMDBAuth.Connecting.Go.html
	// Default to load default config through aws credentials file (~/.aws/credentials)
	awsCfg, err := aws_config.LoadDefaultConfig(ctx)
	if err != nil {
		// otherwise use metadata fields

		// Validate if access key and secret access key are provided
		if accessKey == "" || secretKey == "" {
			return "", fmt.Errorf("failed to load default configuration for AWS using accessKey and secretKey")
		}

		// Set credentials explicitly
		var awsCfg2 aws.CredentialsProvider
		awsCfg2 = aws_credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")
		authenticationToken, err = aws_auth.BuildAuthToken(
			ctx, dbEndpoint, region, pgCfg.User, awsCfg2)
		if err != nil {
			return "", fmt.Errorf("failed to create AWS authentication token: %v", err)
		}

		return authenticationToken, nil
	}

	authenticationToken, err = aws_auth.BuildAuthToken(
		ctx, dbEndpoint, region, pgCfg.User, awsCfg.Credentials)
	if err != nil {
		return "", fmt.Errorf("failed to create AWS authentication token: %v", err)
	}

	return authenticationToken, nil
}

func parseDatabaseFromConnectionString(connectionString string) (string, error) {
	match := databaseNameRegex.FindStringSubmatch(connectionString)
	if len(match) < 2 {
		return "", fmt.Errorf("unable to find database name ('dbname' field) in the connection string")
	}

	// Return the value after "dbname="
	return match[1], nil
}

func parseUserFromConnectionString(connectionString string) (string, error) {
	match := databaseUserRegex.FindStringSubmatch(connectionString)
	if len(match) < 2 {
		return "", fmt.Errorf("unable to find database user in the connection string")
	}

	// Return the value after "user="
	return match[1], nil
}

func parsePasswordFromConnectionString(connectionString string) (string, error) {
	match := databasePasswordRegex.FindStringSubmatch(connectionString)
	if len(match) < 2 {
		return "", fmt.Errorf("unable to find database password field in the connection string")
	}

	// Return the value after "password="
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
		createDBCtx, createDBCancel := context.WithTimeout(ctx, timeout)
		_, err := db.Exec(createDBCtx, fmt.Sprintf(createDatabaseTmpl, dbName))
		createDBCancel()
		if err != nil {
			return fmt.Errorf("failed to create PostgreSQL user: %v", err)
		}
	}
	return nil
}

func CreateUserAndRoleIfNeeded(ctx context.Context, timeout time.Duration, connectionString string, db pginterfaces.PGXPoolConn) error {
	// Parse database user from connection string
	dbUser, err := parseUserFromConnectionString(connectionString)
	if err != nil {
		return fmt.Errorf("failed to parse database user from connection string to create database %v", err)
	}

	var userExists bool
	userExistsCtx, userExistsCancel := context.WithTimeout(ctx, timeout)
	err = db.QueryRow(userExistsCtx, userExistsTmpl, dbUser).Scan(&userExists)
	userExistsCancel()
	if err != nil && err != pgx.ErrNoRows {
		// If user does not exist, create the user with pwd = user name for now.
		// This will be rotated to use proper iam credentials before connecting again.
		_, err = db.Exec(ctx, fmt.Sprintf(createUserTmpl, dbUser, dbUser))
		if err != nil {
			return fmt.Errorf("failed to create PostgreSQL user: %v", err)
		}
	}

	// Check if the role is already granted
	var roleGranted bool
	roleGrantedCtx, roleGrantedCancel := context.WithTimeout(ctx, timeout)
	err = db.QueryRow(roleGrantedCtx, checkRoleTmpl, dbUser).Scan(&roleGranted)
	roleGrantedCancel()
	if err != nil && err != pgx.ErrNoRows {
		return fmt.Errorf("failed to check if the role %v is already granted to the PostgreSQL user %s: %v", awsRole, dbUser, err)
	}

	// Grant the role if it's not already granted
	// Note: granting the postgres (ie root) user AWS IAM auth may lock the user out of the database,
	// so we should prevent this.
	if !roleGranted && dbUser != "postgres" {
		grantRoleCtx, grantRoleCancel := context.WithTimeout(ctx, timeout)
		_, err := db.Exec(grantRoleCtx, fmt.Sprintf(grantRoleTmpl, awsRole, dbUser))
		grantRoleCancel()
		if err != nil {
			return fmt.Errorf("failed to grant PostgreSQL user role: %v", err)
		}
	}
	return nil
}

func InitAWSDatabase(ctx context.Context, config *pgxpool.Config, timeout time.Duration, connString string, region string, awsAccessKey, awsSecretKey string) error {
	// Note: check if password supplied, otherwise auto generate the AWS access token immediately
	var (
		err  error
		pass string
		db   *pgxpool.Pool
	)
	pass, err = parsePasswordFromConnectionString(connString)
	if err != nil || pass == "" {
		pwd, err := GetAccessToken(ctx, config.ConnConfig, region, awsAccessKey, awsSecretKey)
		if err != nil || pwd == "" {
			return fmt.Errorf("failed to refresh access token for iam authentication with postgresql: %v", err)
		}
		config.ConnConfig.Password = pwd
	}

	db, err = pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return fmt.Errorf("postgres configuration store connection error: %w", err)
	}

	// Create database and user with proper iam role if not using an already created iam user
	if pass != "" {
		err = CreateDatabaseIfNeeded(ctx, timeout, connString, db)
		if err != nil {
			return fmt.Errorf("failed create AWS database if needed %v", err)
		}

		err = CreateUserAndRoleIfNeeded(ctx, timeout, connString, db)
		if err != nil {
			return fmt.Errorf("failed create AWS user and grant role if needed %v", err)
		}
	}

	// Set max connection lifetime to 14 minutes in postgres connection pool configuration.
	// Note: this will refresh connections before the 15 min expiration on the IAM AWS auth token,
	// while leveraging the BeforeConnect hook to recreate the token in time dynamically.
	config.MaxConnLifetime = time.Minute * 14

	// Setup connection pool config needed for AWS IAM authentication
	config.BeforeConnect = func(ctx context.Context, pgConfig *pgx.ConnConfig) error {
		// Manually reset auth token with aws and reset the config password using the new iam token
		pwd, err := GetAccessToken(ctx, pgConfig, region, awsAccessKey, awsSecretKey)
		if err != nil {
			return fmt.Errorf("failed to refresh access token for iam authentication with postgresql: %v", err)
		}

		pgConfig.Password = pwd
		return nil
	}

	// TODO: add clean up hooks on user/db?
	return nil
}
