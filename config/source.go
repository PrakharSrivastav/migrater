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

type Source struct {
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
	DBSQL         string
	SourceType    string
}

func (s *Source) Validate() (bool, error) {
	fmt.Println("Validating source configurations")

	// exactly one of FileType or DBType should be set
	if (s.FileType != "" && s.DBType != "") ||
		(s.FileType == "" && s.DBType == "") {
		return false, errors.New("Use either source.File.type OR source.DB.type")
	}

	if s.FileType != "" {
		// validate if File seperator is provided for csv Files
		if strings.ToLower(s.FileType) == "csv" && s.FileSeperator == "" {
			return false, errors.New("Please provide a seperator for csv File (',' OR ';')")
		}

		// validate File path
		stat, err := os.Stat(s.FilePath)
		if s.FilePath == "" || (err == nil && stat.IsDir()) { // should be provided and should not be a directory
			return false, errors.New("Please provide a valid File path")
		}
		if err != nil { // any other File error
			return false, err
		}
		s.SourceType = "File"
		return true, nil
	}

	if s.DBUser == "" {
		return false, errors.New("Please provide database user")
	}
	if s.DBHost == "" {
		return false, errors.New("Please provide database host")
	}
	if s.DBPort == "" {
		return false, errors.New("Please provide database port")
	}
	if s.DBSchema == "" {
		return false, errors.New("Please provide database schema")
	}
	if s.DBPass == "" {
		return false, errors.New("Please provide database password")
	}
	if (s.DBSQL == "" && s.DBTable == "") ||
		(s.DBSQL != "" && s.DBTable != "") {
		return false, errors.New("For database, either provide source.DB.table OR source.table.sql")
	}
	s.SourceType = "DB"
	return true, nil
}

func (s *Source) Init() (string, *os.File, *sql.DB, error) {
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
