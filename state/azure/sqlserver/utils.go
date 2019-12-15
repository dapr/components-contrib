package sqlserver

import (
	"database/sql"
	"encoding/hex"
	"fmt"
)

func hexStringToBytes(s string) ([]byte, error) {
	// c0000c0578 -> to bytes
	return hex.DecodeString(s)
}

func runCommand(tsql string, db *sql.DB) error {
	_, err := db.Exec(tsql)
	if err != nil {
		fmt.Printf("error in command '%s': %s", tsql, err.Error())
		return err
	}

	return nil
}
