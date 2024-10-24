package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	pgauth "github.com/dapr/components-contrib/common/authentication/postgresql"
	"github.com/dapr/components-contrib/configuration"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/tests/utils/configupdater"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/utils"
)

type ConfigUpdater struct {
	client      *pgxpool.Pool
	configTable string
	logger      logger.Logger
}

func NewPostgresConfigUpdater(logger logger.Logger) configupdater.Updater {
	return &ConfigUpdater{
		logger: logger,
	}
}

func createAndSetTable(ctx context.Context, client *pgxpool.Pool, configTable string) error {
	// Creating table if not exists
	_, err := client.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+configTable+" (KEY VARCHAR NOT NULL, VALUE VARCHAR NOT NULL, VERSION VARCHAR NOT NULL, METADATA JSON)")
	if err != nil {
		return fmt.Errorf("error creating table : %w", err)
	}

	// Deleting existing data
	_, err = client.Exec(ctx, "TRUNCATE TABLE "+configTable)
	if err != nil {
		return fmt.Errorf("error truncating table : %w", err)
	}

	return nil
}

func (r *ConfigUpdater) CreateTrigger(channel string) error {
	ctx := context.Background()
	procedureName := "configuration_event_" + channel
	createConfigEventSQL := `CREATE OR REPLACE FUNCTION ` + procedureName + `() RETURNS TRIGGER AS $$
		DECLARE
			data json;
			notification json;

		BEGIN

			IF (TG_OP = 'DELETE') THEN
				data = row_to_json(OLD);
			ELSE
				data = row_to_json(NEW);
			END IF;

			notification = json_build_object(
							'table',TG_TABLE_NAME,
							'action', TG_OP,
							'data', data);

			PERFORM pg_notify('` + channel + `' ,notification::text);
			RETURN NULL;
		END;
	$$ LANGUAGE plpgsql;
	`
	_, err := r.client.Exec(ctx, createConfigEventSQL)
	if err != nil {
		return fmt.Errorf("error creating config event function : %w", err)
	}
	triggerName := "configTrigger_" + channel
	createTriggerSQL := "CREATE OR REPLACE TRIGGER " + triggerName + " AFTER INSERT OR UPDATE OR DELETE ON " + r.configTable + " FOR EACH ROW EXECUTE PROCEDURE " + procedureName + "();"
	_, err = r.client.Exec(ctx, createTriggerSQL)
	if err != nil {
		return fmt.Errorf("error creating config trigger : %w", err)
	}
	return nil
}

func (r *ConfigUpdater) Init(props map[string]string) error {
	connString, _ := metadata.GetMetadataProperty(props, "connectionString")
	useAzureAd, _ := metadata.GetMetadataProperty(props, "useAzureAD")
	useAwsIam, _ := metadata.GetMetadataProperty(props, "useAWSIAM")

	md := pgauth.PostgresAuthMetadata{
		ConnectionString: connString,
		UseAzureAD:       utils.IsTruthy(useAzureAd),
		UseAWSIAM:        utils.IsTruthy(useAwsIam),
	}
	err := md.InitWithMetadata(props, pgauth.InitWithMetadataOpts{AzureADEnabled: true, AWSIAMEnabled: true})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	if tbl, ok := props["table"]; ok && tbl != "" {
		r.configTable = tbl
	} else {
		return errors.New("missing postgreSQL configuration table name")
	}

	config, err := md.GetPgxPoolConfig()
	if err != nil {
		return fmt.Errorf("postgres configuration store connection error : %w", err)
	}

	r.client, err = pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return fmt.Errorf("postgres configuration store connection error : %w", err)
	}
	err = r.client.Ping(ctx)
	if err != nil {
		return fmt.Errorf("postgres configuration store ping error : %w", err)
	}

	err = createAndSetTable(ctx, r.client, r.configTable)
	if err != nil {
		return fmt.Errorf("postgres configuration store table creation error : %w", err)
	}
	return nil
}

func buildAddQuery(items map[string]*configuration.Item, configTable string) (string, []interface{}, error) {
	query := ""
	paramWildcard := make([]string, 0, len(items))
	params := make([]interface{}, 0, 4*len(items))
	if len(items) == 0 {
		return query, params, errors.New("empty list of items")
	}
	var queryBuilder strings.Builder
	queryBuilder.WriteString("INSERT INTO " + configTable + " (KEY, VALUE, VERSION, METADATA) VALUES ")

	paramPosition := 1
	for key, item := range items {
		paramWildcard = append(paramWildcard, "($"+strconv.Itoa(paramPosition)+", $"+strconv.Itoa(paramPosition+1)+", $"+strconv.Itoa(paramPosition+2)+", $"+strconv.Itoa(paramPosition+3)+")")
		params = append(params, key, item.Value, item.Version, item.Metadata)
		paramPosition += 4
	}
	queryBuilder.WriteString(strings.Join(paramWildcard, " , "))
	query = queryBuilder.String()
	return query, params, nil
}

func (r *ConfigUpdater) AddKey(items map[string]*configuration.Item) error {
	query, params, err := buildAddQuery(items, r.configTable)
	if err != nil {
		return err
	}
	_, err = r.client.Exec(context.Background(), query, params...)
	if err != nil {
		return err
	}
	return nil
}

func (r *ConfigUpdater) UpdateKey(items map[string]*configuration.Item) error {
	if len(items) == 0 {
		return errors.New("empty list of items")
	}
	for key, item := range items {
		var params []interface{}
		query := "UPDATE " + r.configTable + " SET VALUE = $1, VERSION = $2, METADATA = $3 WHERE KEY = $4"
		params = append(params, item.Value, item.Version, item.Metadata, key)
		_, err := r.client.Exec(context.Background(), query, params...)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *ConfigUpdater) DeleteKey(keys []string) error {
	if len(keys) == 0 {
		return errors.New("empty list of items")
	}
	for _, key := range keys {
		var params []interface{}
		query := "DELETE FROM " + r.configTable + " WHERE KEY = $1"
		params = append(params, key)
		_, err := r.client.Exec(context.Background(), query, params...)
		if err != nil {
			return err
		}
	}
	return nil
}
