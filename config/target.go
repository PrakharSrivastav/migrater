package config

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/lib/pq"
)

type Target struct {
	FileType      string
	FilePath      string
	FileSeperator string
	DBUser        string
	DBType        string
	DBSchema      string
	DBTable       string
	DBHost        string
	DBPort        string
	DBPass        string
	SourceType    string
}

func (t *Target) Validate() (bool, error) {
	fmt.Println("Validating target configurations")

	// exactly one of FileType or DBType should be set
	if (t.FileType != "" && t.DBType != "") ||
		(t.FileType == "" && t.DBType == "") {
		return false, errors.New("Use either target.File.type OR target.DB.type")
	}

	if t.FileType != "" {
		// validate if File seperator is provided for csv Files
		if strings.ToLower(t.FileType) == "csv" && t.FileSeperator == "" {
			return false, errors.New("Please provide a seperator for csv File (',' OR ';')")
		}

		// validate File path
		stat, err := os.Stat(t.FilePath)
		if t.FilePath == "" || (err == nil && stat.IsDir()) { // should be provided and should not be a directory
			return false, errors.New("Please provide a valid File path")
		}
		if err != nil { // any other File error
			return false, err
		}
		t.SourceType = "File"
		return true, nil
	}

	if t.DBUser == "" {
		return false, errors.New("Please provide database user")
	}
	if t.DBHost == "" {
		return false, errors.New("Please provide database host")
	}
	if t.DBPort == "" {
		return false, errors.New("Please provide database port")
	}
	if t.DBSchema == "" {
		return false, errors.New("Please provide database schema")
	}
	if t.DBPass == "" {
		return false, errors.New("Please provide database password")
	}
	if t.DBTable == "" {
		return false, errors.New("Please provide target table ")
	}
	t.SourceType = "DB"
	return true, nil
}

func (s *Target) Init() (string, *os.File, *sql.DB, error) {
	if s.SourceType == "file" {
		f, err := os.Open(s.FilePath)
		if err != nil {
			f.Close()
			return s.SourceType, nil, nil, err
		}
		return s.SourceType, f, nil, nil
	}

	switch s.DBType {
	case "pgsql":
		psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
			s.DBHost,
			s.DBPort,
			s.DBUser,
			s.DBPass,
			s.DBSchema,
		)

		db, err := sql.Open("postgres", psqlInfo)
		if err != nil {
			return s.SourceType, nil, nil, err
		}
		err = db.Ping()
		if err != nil {
			return s.SourceType, nil, nil, err
		}
		log.Println("Successfully connected!")
		return s.SourceType, nil, db, nil

	}
	return s.SourceType, nil, nil, errors.New("Invalid database source")
}
