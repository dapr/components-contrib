package awsiam

import (
	"context"
	"fmt"
	"strconv"
	"time"

	aws_config "github.com/aws/aws-sdk-go-v2/config"
	aws_credentials "github.com/aws/aws-sdk-go-v2/credentials"
	aws_auth "github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type AWSIAM struct {
	// Ignored by metadata parser because included in built-in authentication profile
	// access key to use for accessing postgresql.
	AWSAccessKey string `json:"awsAccessKey" mapstructure:"awsAccessKey"`
	// secret key to use for accessing postgresql.
	AWSSecretKey string `json:"awsSecretKey" mapstructure:"awsSecretKey"`
	// aws session token to use.
	AWSSessionToken string `mapstructure:"awsSessionToken"`
	// aws region in which postgresql should create resources.
	AWSRegion string `mapstructure:"awsRegion"`
}

func GetAccessToken(ctx context.Context, pgCfg *pgx.ConnConfig, region, accessKey, secretKey string) (string, error) {
	dbEndpoint := pgCfg.Host + ":" + strconv.Itoa(int(pgCfg.Port))
	var authenticationToken string

	// https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/UsingWithRDS.IAMDBAuth.Connecting.Go.html
	// Default to load default config through aws credentials file (~/.aws/credentials)
	awsCfg, err := aws_config.LoadDefaultConfig(ctx)
	if err != nil {
		// otherwise use metadata fields

		// Validate if access key and secret access key are provided
		if accessKey == "" || secretKey == "" {
			return "", fmt.Errorf("failed to load default configuration for AWS using accessKey and secretKey: %w", err)
		}

		// Set credentials explicitly
		awsCfg := aws_credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")
		authenticationToken, err := aws_auth.BuildAuthToken(
			ctx, dbEndpoint, region, pgCfg.User, awsCfg)
		if err != nil {
			return "", fmt.Errorf("failed to create AWS authentication token: %w", err)
		}

		return authenticationToken, nil
	}

	authenticationToken, err = aws_auth.BuildAuthToken(
		ctx, dbEndpoint, region, pgCfg.User, awsCfg.Credentials)
	if err != nil {
		return "", fmt.Errorf("failed to create AWS authentication token: %w", err)
	}

	return authenticationToken, nil
}

func InitiateAWSIAMAuth(ctx context.Context, config *pgxpool.Config, connString string, region string, awsAccessKey, awsSecretKey string) error {
	// Set max connection lifetime to 14 minutes in postgres connection pool configuration.
	// Note: this will refresh connections before the 15 min expiration on the IAM AWS auth token,
	// while leveraging the BeforeConnect hook to recreate the token in time dynamically.
	config.MaxConnLifetime = time.Minute * 14

	// Setup connection pool config needed for AWS IAM authentication
	config.BeforeConnect = func(ctx context.Context, pgConfig *pgx.ConnConfig) error {
		// Manually reset auth token with aws and reset the config password using the new iam token
		pwd, errGetAccessToken := GetAccessToken(ctx, pgConfig, region, awsAccessKey, awsSecretKey)
		if errGetAccessToken != nil {
			return fmt.Errorf("failed to refresh access token for iam authentication with PostgreSQL: %w", errGetAccessToken)
		}

		pgConfig.Password = pwd
		config.ConnConfig.Password = pwd

		return nil
	}

	return nil
}
