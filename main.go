package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/PrakharSrivastav/sql-query-builder/qb"
	"github.com/PrakharSrivastav/sql-query-builder/qb/core"

	"github.com/PrakharSrivastav/migrater/config"
	"github.com/PrakharSrivastav/migrater/migrate"
	"github.com/spf13/viper"
)

func main() {
	var err error
	var source *config.Source
	var target *config.Target
	// Read all configurations
	configPath := flag.String("configPath", "", "Path for the configuration file")
	flag.Parse()

	switch strings.TrimSpace(*configPath) {
	case "":
		if source, target, err = loadFromFlags(); err != nil {
			panic(fmt.Sprintf("Error loding configurations from flags (%s)", err.Error()))
		}
	default:
		var err error
		fmt.Println("Loading from config path")
		if source, target, err = loadFromConfigPath(*configPath); err != nil {
			fmt.Printf("Error loding configurations from config file [%s]\n", err.Error())
			os.Exit(1)
		}
	}

	// initialize source
	sourceFile, sourceDB, err := source.Init()
	if err != nil {
		fmt.Printf("Error initializing source [%v]\n", err)
		os.Exit(1)
	}

	// initialize target
	targetFile, targetDB, err := target.Init()
	if err != nil {
		fmt.Printf("Error initializing target [%v]\n", err)
		os.Exit(1)
	}
	builder, err := qb.NewQueryBuilder(core.ANSI)
	if err != nil {
		fmt.Printf("Error creating a query builder[%v]\n", err)
		os.Exit(1)
	}

	migrater := new(migrate.Migrater)
	migrater.SourceFile = sourceFile
	migrater.SourceDB = sourceDB
	migrater.SourceTable = source.DBTable
	migrater.SourceSQL = source.DBSQL
	migrater.TargetDB = targetDB
	migrater.TargetTable = target.DBTable
	migrater.TargetFile = targetFile
	migrater.TargetFileType = target.FileType
	migrater.QB = builder

	if source.SourceType == config.FileType && target.SourceType == config.FileType {
		migrater.Type = migrate.FileToFile
	}

	if source.SourceType == config.FileType && target.SourceType == config.DBType {
		migrater.Type = migrate.FileToDB
	}

	if source.SourceType == config.DBType && target.SourceType == config.DBType {
		migrater.Type = migrate.DBToDB
	}

	if source.SourceType == config.DBType && target.SourceType == config.FileType {
		migrater.Type = migrate.DBToFile
	}

	migrater.Migrate()
}

func loadFromConfigPath(configPath string) (*config.Source, *config.Target, error) {
	var err error
	// Load configuration files
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(configPath)
	if err = viper.ReadInConfig(); err != nil {
		log.Panicf("Error reading configurations (%s)\n", err.Error())
		return nil, nil, err
	}

	// parse and validate source configs
	source := config.Source{
		FileType:      strings.TrimSpace(viper.GetString("source.file.type")),
		FilePath:      strings.TrimSpace(viper.GetString("source.file.path")),
		FileSeperator: strings.TrimSpace(viper.GetString("source.file.seperator")),
		DBType:        strings.TrimSpace(viper.GetString("source.db.type")),
		DBUser:        strings.TrimSpace(viper.GetString("source.db.user")),
		DBPass:        strings.TrimSpace(viper.GetString("source.db.pass")),
		DBPort:        strings.TrimSpace(viper.GetString("source.db.port")),
		DBHost:        strings.TrimSpace(viper.GetString("source.db.host")),
		DBSchema:      strings.TrimSpace(viper.GetString("source.db.schema")),
		DBTable:       strings.TrimSpace(viper.GetString("source.db.table")),
		DBSQL:         strings.TrimSpace(viper.GetString("source.db.sql")),
	}

	if _, err = source.Validate(); err != nil {
		return nil, nil, err
	}

	// parse and validate target configs
	target := config.Target{
		FileType:      strings.TrimSpace(viper.GetString("target.file.type")),
		FilePath:      strings.TrimSpace(viper.GetString("target.file.path")),
		FileSeperator: strings.TrimSpace(viper.GetString("target.file.seperator")),
		DBType:        strings.TrimSpace(viper.GetString("target.db.type")),
		DBUser:        strings.TrimSpace(viper.GetString("target.db.user")),
		DBPass:        strings.TrimSpace(viper.GetString("target.db.pass")),
		DBPort:        strings.TrimSpace(viper.GetString("target.db.port")),
		DBHost:        strings.TrimSpace(viper.GetString("target.db.host")),
		DBSchema:      strings.TrimSpace(viper.GetString("target.db.schema")),
		DBTable:       strings.TrimSpace(viper.GetString("target.db.table")),
	}
	if _, err = target.Validate(); err != nil {
		return nil, nil, err
	}
	return &source, &target, err
}

func loadFromFlags() (*config.Source, *config.Target, error) {
	var err error
	return nil, nil, err
}
