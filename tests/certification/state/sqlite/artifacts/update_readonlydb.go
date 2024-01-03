// This small utility can be used to update the "readonly.db" file when a new migration is added to the state store component.
package main

import (
	"context"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	state_sqlite "github.com/dapr/components-contrib/state/sqlite"
	"github.com/dapr/kit/logger"
)

func main() {
	log := logger.NewLogger("updatedb")
	log.SetOutputLevel(logger.DebugLevel)

	log.Info("Initializing the component to perform migrations")
	store := state_sqlite.NewSQLiteStateStore(log).(*state_sqlite.SQLiteStore)
	err := store.Init(context.Background(), state.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				"connectionString": "file:readonly.db",
				"disableWAL":       "true",
			},
		},
	})
	if err != nil {
		log.Fatalf("Failed to perform migrations: %v", err)
	}

	log.Info("Vacuuming DB")
	_, err = store.GetDBAccess().GetConnection().Exec("VACUUM")
	if err != nil {
		log.Fatalf("Failed to vacuum database: %v", err)
	}

	log.Info("You are all set 😁")
}
