package migrate

import (
	"database/sql"
	"fmt"
	"os"
)

type MigrationType int

const (
	FileToFile MigrationType = iota + 1
	FileToDB
	DBToDB
	DBToFile
)

type Migrater struct {
	SourceFile  *os.File
	TargetFile  *os.File
	SourceDB    *sql.DB
	SourceTable string
	SourceSQL   string
	TargetDB    *sql.DB
	TargetTable string
	Type        MigrationType
}

func (m *Migrater) Migrate() {
	fmt.Println("migrating")
	defer m.SourceDB.Close()
	defer m.SourceFile.Close()
	defer m.TargetDB.Close()
	defer m.TargetFile.Close()
}
