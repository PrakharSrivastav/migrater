package migrate

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/PrakharSrivastav/sql-query-builder/qb/builder"

	"github.com/PrakharSrivastav/sql-query-builder/qb/core"
)

type MigrationType int

const (
	FileToFile MigrationType = iota + 1
	FileToDB
	DBToDB
	DBToFile
)

type Migrater struct {
	SourceFile     *os.File
	TargetFile     *os.File
	TargetFileType string
	SourceDB       *sql.DB
	SourceTable    string
	SourceSQL      string
	TargetDB       *sql.DB
	TargetTable    string
	Type           MigrationType
	QB             *core.SQL
}

func (m *Migrater) Migrate() {
	defer m.cleanUp()
	switch m.Type {
	case FileToFile:
		m.migrateF2F()
	case FileToDB:
		m.migrateF2D()
	case DBToFile:
		m.migrateD2F()
	case DBToDB:
		m.migrateD2D()
	default:
		fmt.Println("Nothing to run")
	}
}

func (m *Migrater) cleanUp() {
	if m.SourceDB != nil {
		m.SourceDB.Close()
	}
	if m.SourceFile != nil {
		m.SourceFile.Close()
	}
	if m.TargetDB != nil {
		m.TargetDB.Close()
	}
	if m.TargetFile != nil {
		m.TargetFile.Close()
	}
}
func (m *Migrater) migrateF2F() { fmt.Println("migrating F2F") }
func (m *Migrater) migrateF2D() { fmt.Println("migrating F2D") }
func (m *Migrater) migrateD2F() {
	var err error
	var selectSQL string
	var rows *sql.Rows
	var cols []string
	switch m.SourceSQL {
	case "":
		selectSQL = m.QB.Reader.Select("*").From(m.SourceTable).Limit(1).Build()
	default:
		selectSQL = m.SourceSQL + "LIMIT 1"
	}
	if rows, err = m.SourceDB.Query(selectSQL); err != nil {
		fmt.Println(err)
	}
	for rows.Next() {
		if cols, err = rows.Columns(); err != nil {
			fmt.Println(err)
		}
	}
	rows.Close()
	sort.Strings(cols)
	switch m.SourceSQL {
	case "":
		selectSQL = m.QB.Reader.Select(strings.Join(cols, ",")).From(m.SourceTable).Build()
	default:
		selectSQL = m.SourceSQL
	}
	if rows, err = m.SourceDB.Query(selectSQL); err != nil {
		fmt.Println(err)
	}
	defer rows.Close()

	writer := bufio.NewWriter(m.TargetFile)
	csvWriter := csv.NewWriter(writer)
	switch m.TargetFileType {
	case "csv":
		m.writeCSV(csvWriter, [][]string{cols})
	}

	var data [][]string
	var temp []string
	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range cols {
			columnPointers[i] = &columns[i]
		}
		if err := rows.Scan(columnPointers...); err != nil {
			fmt.Println(err)
		}

		for i := range cols {
			val := columnPointers[i].(*interface{})

			switch (*val).(type) {
			case []uint8:
				str := string((*val).([]byte))
				temp = append(temp, str)
			default:
				temp = append(temp, (*val).(string))
			}
		}
		data = append(data, temp)
		temp = []string{}
		if len(data) == 1000 {
			fmt.Println("Dumping 1000 records")
			m.writeCSV(csvWriter, data)
			data = [][]string{}
		}
	}
	if len(data) > 0 {
		fmt.Println("Dumping remaining records")

	}
}

func (m *Migrater) writeCSV(w *csv.Writer, data [][]string) {
	// c.Comma = ';'
	w.WriteAll(data)
	if err := w.Error(); err != nil {
		log.Fatalln("error writing csv:", err)
	}
}

func (m *Migrater) migrateD2D() {
	// Source configuration
	fmt.Println("migrating D2D")

	if !m.tableExists(m.TargetTable) {
		fmt.Println("Table does not exist")
		if _, err := m.createTable(m.TargetTable); err != nil {
			fmt.Println(err)
			m.cleanUp()
			os.Exit(1)
		}
	}
	fmt.Println("Table exists")
	if _, err := m.begingD2DImport(); err != nil {
		fmt.Println(err)
		m.cleanUp()
		os.Exit(1)
	}
}

func (m *Migrater) tableExists(table string) bool {
	rows, err := m.TargetDB.Query(fmt.Sprintf("SELECT to_regclass('%s')", table))
	if err != nil {
		log.Printf("Error %s checking if the table (%s) exists", err, table)
	}
	defer rows.Close()

	for rows.Next() {
		var data sql.NullString
		err = rows.Scan(&data)
		if err != nil {
			log.Printf("Error %s scanning values if table (%s) exists \n", err, table)
		}
		return data.Valid
	}
	return false
}

func (m *Migrater) createTable(tableName string) (bool, error) {
	var err error
	var rows *sql.Rows
	var selectSQL string
	var columns []*sql.ColumnType
	var tableCols []builder.Columns

	switch m.SourceSQL {
	case "":
		selectSQL = m.QB.Reader.Select("*").From(m.SourceTable).Limit(1).Build()
	default:
		selectSQL = m.SourceSQL + "LIMIT 1"
	}

	if rows, err = m.SourceDB.Query(selectSQL); err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		if columns, err = rows.ColumnTypes(); err != nil {
			return false, err
		}
	}
	for _, item := range columns {
		tableCols = append(tableCols, builder.Columns{Name: item.Name(), Datatype: item.DatabaseTypeName()})
	}

	createSQL := m.QB.Creater.
		Table(m.TargetTable).
		SetColumns(tableCols).
		Build()

	if _, err := m.TargetDB.Exec(createSQL); err != nil {
		return false, err
	}
	return true, nil
}

func (m *Migrater) begingD2DImport() (bool, error) {
	var err error
	var selectSQL string
	var rows *sql.Rows
	var cols []string
	switch m.SourceSQL {
	case "":
		selectSQL = m.QB.Reader.Select("*").From(m.SourceTable).Limit(1).Build()
	default:
		selectSQL = m.SourceSQL + "LIMIT 1"
	}
	if rows, err = m.SourceDB.Query(selectSQL); err != nil {
		return false, err
	}
	for rows.Next() {
		if cols, err = rows.Columns(); err != nil {
			return false, err
		}
	}
	rows.Close()
	sort.Strings(cols)
	switch m.SourceSQL {
	case "":
		selectSQL = m.QB.Reader.Select(strings.Join(cols, ",")).From(m.SourceTable).Build()
	default:
		selectSQL = m.SourceSQL
	}
	if rows, err = m.SourceDB.Query(selectSQL); err != nil {
		return false, err
	}
	defer rows.Close()
	insertSQLBuilder := m.QB.Inserter.Table(m.TargetTable).Columns(cols)
	insert := insertSQLBuilder
	count := 0
	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range cols {
			columnPointers[i] = &columns[i]
		}
		if err := rows.Scan(columnPointers...); err != nil {
			return false, err
		}
		mp := make(map[string]interface{})
		for i, colName := range cols {
			val := columnPointers[i].(*interface{})
			switch (*val).(type) {
			case []uint8:
				str := string((*val).([]byte))
				mp[colName] = str
			default:
				mp[colName] = *val
			}
		}
		insert.Values(mp)
		count++
		if count == 1000 {
			fmt.Println("Dumping 1000 records")
			if _, err := m.TargetDB.Exec(insert.Build()); err != nil {
				fmt.Printf("Error (%s) in executing  \n", err)
			}
			insert = insertSQLBuilder
			count = 0
		}
	}
	if count > 0 {
		fmt.Println("Dumping remaining records")
		if _, err := m.TargetDB.Exec(insert.Build()); err != nil {
			fmt.Printf("Error (%s) in executing  \n", err)
		}
	}
	fmt.Println("Complete")
	return true, nil
}
