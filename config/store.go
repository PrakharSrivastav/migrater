package config

import (
	"database/sql"
	"os"
)

type Store interface {
	Init() (string, *os.File, *sql.DB, error)
}
