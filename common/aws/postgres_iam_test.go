package aws

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	awsAuth "github.com/dapr/components-contrib/common/aws/auth"
	"github.com/dapr/kit/logger"
)

func TestConfigurePostgresIAM(t *testing.T) {
	ctx := t.Context()
	pgCfg, err := pgx.ParseConfig("host=localhost port=5432 user=user")
	require.NoError(t, err)
	poolCfg := &pgxpool.Config{ConnConfig: pgCfg}
	authOpts := awsAuth.Options{
		Logger:    logger.NewLogger("test"),
		Region:    "us-west-2",
		AccessKey: "k",
		SecretKey: "s",
	}

	err = ConfigurePostgresIAM(ctx, poolCfg, authOpts)
	require.NoError(t, err)
	assert.Equal(t, 8*time.Minute, poolCfg.MaxConnLifetime)
	assert.NotNil(t, poolCfg.BeforeConnect)

	pgCfg, err = pgx.ParseConfig("host=localhost port=5432 user=user")
	require.NoError(t, err)
	err = poolCfg.BeforeConnect(ctx, pgCfg)
	require.NoError(t, err)
	assert.NotEmpty(t, pgCfg.Password)
}
