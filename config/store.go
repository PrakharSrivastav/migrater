package config

import (
	"database/sql"
	"os"
)

type StoreType int

const (
	FileType StoreType = iota + 1
	DBType
)

func (s StoreType) String() string {
	switch s {
	case FileType:
		return "FileType"
	case DBType:
		return "DBType"
	default:
		return ""
	}
}

type Store interface {
	Init() (*os.File, *sql.DB, error)
}
