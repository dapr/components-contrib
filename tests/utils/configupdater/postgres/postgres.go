package postgres

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/dapr/components-contrib/configuration"
	"github.com/dapr/components-contrib/tests/utils/configupdater"
	"github.com/dapr/kit/logger"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	connectionStringKey          = "connectionString"
	configtablekey               = "table"
	ErrorMissingConnectionString = "missing postgreSQL connection string"
	ErrorMissingTableName        = "missing postgreSQL configuration table name"
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
	// Deleting the existing table if it exists
	_, err := client.Exec(ctx, "DROP TABLE IF EXISTS "+configTable)
	if err != nil {
		return fmt.Errorf("error dropping table : %w", err)
	}

	_, err = client.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+configTable+" (KEY VARCHAR NOT NULL, VALUE VARCHAR NOT NULL, VERSION VARCHAR NOT NULL, METADATA JSON)")
	if err != nil {
		return fmt.Errorf("error creating table : %w", err)
	}

	createConfigEventSQL := `CREATE OR REPLACE FUNCTION configuration_event() RETURNS TRIGGER AS $$
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

			PERFORM pg_notify('config',notification::text);
			RETURN NULL;
		END;
	$$ LANGUAGE plpgsql;
	`
	_, err = client.Exec(ctx, createConfigEventSQL)
	if err != nil {
		return fmt.Errorf("error creating config event function : %w", err)
	}

	createTriggerSQL := "CREATE TRIGGER configTrigger AFTER INSERT OR UPDATE OR DELETE ON " + configTable + " FOR EACH ROW EXECUTE PROCEDURE configuration_event();"
	_, err = client.Exec(ctx, createTriggerSQL)
	if err != nil {
		return fmt.Errorf("error creating config trigger : %w", err)
	}
	return nil
}

func (r *ConfigUpdater) Init(props map[string]string) error {
	var conn string
	ctx := context.Background()
	if val, ok := props[connectionStringKey]; ok && val != "" {
		conn = val
	} else {
		return fmt.Errorf(ErrorMissingConnectionString)
	}
	if tbl, ok := props[configtablekey]; ok && tbl != "" {
		r.configTable = tbl
	} else {
		return fmt.Errorf(ErrorMissingTableName)
	}
	config, err := pgxpool.ParseConfig(conn)
	if err != nil {
		return fmt.Errorf("postgres configuration store connection error : %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return fmt.Errorf("postgres configuration store connection error : %w", err)
	}
	err = pool.Ping(ctx)
	if err != nil {
		return fmt.Errorf("postgres configuration store ping error : %w", err)
	}
	r.client = pool
	err = createAndSetTable(ctx, r.client, r.configTable)
	if err != nil {
		return fmt.Errorf("postgres configuration store table creation error : %w", err)
	}
	return nil
}

func buildAddQuery(items map[string]*configuration.Item, configTable string) (string, []interface{}, error) {
	query := ""
	var params []interface{}
	if len(items) == 0 {
		return query, params, fmt.Errorf("empty list of items")
	}
	var queryBuilder strings.Builder
	queryBuilder.WriteString("INSERT INTO " + configTable + " (KEY, VALUE, VERSION, METADATA) VALUES ")
	var paramWildcard []string
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
		return fmt.Errorf("empty list of items")
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
		return fmt.Errorf("empty list of items")
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
