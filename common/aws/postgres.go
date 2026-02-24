package aws

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	awsAuth "github.com/dapr/components-contrib/common/aws/auth"
)

// ConfigurePostgresIAM mutates a pgxpool.Config so that it will obtain
// temporary credentials from AWS IAM each time a new connection is opened.
func ConfigurePostgresIAM(ctx context.Context, poolConfig *pgxpool.Config, opts awsAuth.Options) error {
	provider, err := awsAuth.NewCredentialProvider(ctx, opts, nil)
	if err != nil {
		return err
	}

	poolConfig.MaxConnLifetime = 8 * time.Minute

	region := opts.Region

	poolConfig.BeforeConnect = func(ctx context.Context, pgConfig *pgx.ConnConfig) error {
		dbEndpoint := fmt.Sprintf("%s:%d", pgConfig.Host, pgConfig.Port)

		pwd, err := auth.BuildAuthToken(ctx, dbEndpoint, region, pgConfig.User, provider)
		if err != nil {
			return fmt.Errorf("failed to create AWS authentication token: %w", err)
		}
		pgConfig.Password = pwd
		return nil
	}
	return nil
}
