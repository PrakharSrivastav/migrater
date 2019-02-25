package migrate

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/PrakharSrivastav/sql-query-builder/qb/builder"
	"github.com/clbanning/mxj"

	"github.com/PrakharSrivastav/sql-query-builder/qb/core"
)

// MigrationType determines the type of migration to execute between source and target
type MigrationType int

const (
	// FileToFile migrates from source file to target file
	FileToFile MigrationType = iota + 1
	// FileToDB migrates from source file to target db
	FileToDB
	// DBToDB migrates from source db to target db
	DBToDB
	// DBToFile migrates from source db to target file
	DBToFile
)

// Migrater is the basic structure that holds the internal source and target information
type Migrater struct {
	SourceDB       *sql.DB
	SourceSQL      string
	SourceFile     *bufio.Reader
	SourceTable    string
	SourceFileType string
	TargetDB       *sql.DB
	TargetFile     *bufio.Writer
	TargetTable    string
	TargetFileType string
	Type           MigrationType
	QB             *core.SQL
}

// Migrate performs the migration based on the migration Type
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
		m.SourceFile = nil
	}
	if m.TargetDB != nil {
		m.TargetDB.Close()
	}
	if m.TargetFile != nil {
		m.TargetFile = nil
	}
}
func (m *Migrater) migrateF2F() {
	fmt.Println("migrating F2F")

	if m.SourceFileType == "csv" && m.TargetFileType == "xml" {
		m.csvToXML()
	}

	if m.SourceFileType == "xml" && m.TargetFileType == "csv" {
		m.xmlToCSV()
	}

}

func (m *Migrater) xmlToCSV() {
	fmt.Println("migrating to xml to csv")
	var mp map[string]interface{}
	var err error
	// mp, err = mxj.NewMapXml(m.SourceFile)

	// m.SourceFile.ReadLine(m.SourceFile){

	// }

	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(mp)
}

func (m *Migrater) csvToXML() {
	fmt.Println("migrating from csv to xml")
	reader := csv.NewReader(m.SourceFile)
	var err error
	record := []string{}
	// read the header first
	if record, err = reader.Read(); err != nil || err == io.EOF {
		fmt.Println("Error reading from source")
		panic(err.Error())
	}
	columns := record
	// sort.Strings(columns)
	xmlTemp := map[string]interface{}{}
	xmlData := []map[string]interface{}{}
	for {
		record, err = reader.Read()
		if err == io.EOF || err != nil {
			fmt.Println("Error reading from csv")
			if len(xmlData) > 0 {
				m.writeXML(xmlData)
			}
			break
		}
		for i := range record {
			xmlTemp[columns[i]] = record[i]
		}
		xmlData = append(xmlData, xmlTemp)
		xmlTemp = map[string]interface{}{}
		// check the length
		if len(xmlData) == 1000 {
			m.writeXML(xmlData)
			xmlData = []map[string]interface{}{}
		}
	}
}
func (m *Migrater) migrateF2D() { fmt.Println("migrating F2D") }
func (m *Migrater) migrateD2F() {
	var err error
	var selectSQL string
	var rows *sql.Rows
	var cols []string
	if cols, err = m.getColumnsFromSourceTable(); err != nil {
		fmt.Println(err)
	}
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

	var csvData [][]string
	var csvTemp []string
	csvData = append(csvData, cols)

	xmlData := []map[string]interface{}{}
	xmlTemp := map[string]interface{}{}
	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range cols {
			columnPointers[i] = &columns[i]
		}
		if err := rows.Scan(columnPointers...); err != nil {
			fmt.Println(err)
		}

		switch m.TargetFileType {
		case "csv":
			for i := range cols {
				val := columnPointers[i].(*interface{})
				switch (*val).(type) {
				case []uint8:
					csvTemp = append(csvTemp, string((*val).([]byte)))
				default:
					csvTemp = append(csvTemp, (*val).(string))
				}
			}
			csvData = append(csvData, csvTemp)
			csvTemp = []string{}
			if len(csvData) == 100 {
				fmt.Println("Dumping 1000 records")
				csv.NewWriter(m.TargetFile).WriteAll(csvData)
				csvData = [][]string{}
			}
		case "xml":
			for i := range cols {
				// value := v
				val := columnPointers[i].(*interface{})
				switch (*val).(type) {
				case []uint8:
					xmlTemp[cols[i]] = string((*val).([]byte))
				default:
					xmlTemp[cols[i]] = (*val).(string)
				}
			}
			xmlData = append(xmlData, xmlTemp)
			xmlTemp = map[string]interface{}{}
			if len(xmlData) == 100 {
				fmt.Println("writing xml")
				m.writeXML(xmlData)
				xmlData = []map[string]interface{}{}
			}
		}

	}
	if len(csvData) > 1 {
		fmt.Println("Dumping remaining records")
		csv.NewWriter(m.TargetFile).WriteAll(csvData)
	}
	if len(xmlData) > 0 {
		fmt.Println("Dumping remaining  xml")
		m.writeXML(xmlData)
	}
}
func (m *Migrater) writeXML(data []map[string]interface{}) {

	for _, value := range data {
		if err := mxj.Map(value).XmlIndentWriter(m.TargetFile, "", "  ", "Root"); err != nil {
			fmt.Println("Error writing xml", err)
		}
	}
	m.TargetFile.Flush()
}

func (m *Migrater) getColumnsFromSourceTable() ([]string, error) {
	var selectSQL string
	var rows *sql.Rows
	var err error
	var cols []string
	switch m.SourceSQL {
	case "":
		selectSQL = m.QB.Reader.Select("*").From(m.SourceTable).Limit(1).Build()
	default:
		selectSQL = m.SourceSQL + "LIMIT 1"
	}
	if rows, err = m.SourceDB.Query(selectSQL); err != nil {
		return nil, err
	}
	for rows.Next() {
		if cols, err = rows.Columns(); err != nil {
			return nil, err
		}
	}
	rows.Close()
	return cols, nil
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
