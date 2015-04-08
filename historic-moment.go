package main

import (
    "fmt"
    "log"
    //"strconv"
    "database/sql"
    _ "github.com/lib/pq"
)

type columnStruct struct {
    columnName string
    dataType string
    characterMaximumLength int
    numericPrecision int
    numericScale int
    constraintType string
}

var historicMomentId int
var tableNames []string
var newCount int
var updatedCount int
var deletedCount int
var errorCount int
var workLog string


func main() {
    tableNames = make([]string, 0, 100)
    db, err := sql.Open("postgres", "user=bradwilliams dbname=fbi_development sslmode=disable")
    if err != nil {
        log.Fatal(err)
    }

    s := `SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public'
        AND table_name NOT LIKE '%_historic'
        AND table_name NOT LIKE 'historic_moments'
        ORDER BY table_name`

    rows, err := db.Query(s)
    if err != nil {
        log.Fatal(err)
    }

    defer rows.Close()
    for rows.Next() {
        var tableName string
        err := rows.Scan(&tableName)
        if err != nil {
            log.Fatal(err)
        }

        tableNames = append(tableNames, tableName)
    }

    err = rows.Err()
    if err != nil {
        log.Fatal(err)
    }

    if !tableExists(db, "historic_moments") {
        s := `CREATE TABLE historic_moments (
            id serial PRIMARY KEY,
            context varchar(200),
            new_count integer,
            updated_count integer,
            deleted_count integer,
            error_count integer,
            work_log text,
            started_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
            completed_at timestamp
            )`
        _, err := db.Query(s)
        if err != nil {
            log.Fatal(err)
        }
    }

    rows, err = db.Query("INSERT INTO historic_moments (context) VALUES ('standard') RETURNING id")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    rows.Next()
    err = rows.Scan(&historicMomentId)
    if err != nil {
        log.Fatal(err)
    }

    log.Println(historicMomentId)



    //processTable(db, "bmg_rk_masters")


    for _, tableName := range tableNames {
        log.Println(tableName)
        processTable(db, tableName)
    }
}


func processTable(db *sql.DB, tableName string) {
    createOrUpdateHistoricTable(db, tableName)
}


func createOrUpdateHistoricTable(db *sql.DB, tableName string) {
    columns, primaryKeyColumns := getTableInfo(db, tableName)

    historicTableName := tableName + "_historic"

    if tableExists(db, historicTableName) {
        log.Println("historic table already exists :-)")
        //addMissingColumns(historicTableName, columns)
    } else {
        historicColumns := make([]columnStruct, len(columns), len(columns) + 2)
        copy(historicColumns, columns)
        historicColumns = append([]columnStruct{columnStruct{"last_historic_moment_id", "integer", 0, 0, 0, ""}}, historicColumns...)
        historicColumns = append([]columnStruct{columnStruct{"first_historic_moment_id", "integer", 0, 0, 0, ""}}, historicColumns...)

        if len(primaryKeyColumns) > 0 {
            historicPrimaryKeyColumns := append(primaryKeyColumns, columnStruct{"first_historic_moment_id", "int", 0, 0, 0, ""})
            createTable(db, historicTableName, historicColumns, historicPrimaryKeyColumns)
        } else {
            createTable(db, historicTableName, historicColumns, primaryKeyColumns)
        }

        createForeignKey(db, historicTableName, primaryKeyColumns, tableName)
        copyAllRecordsToHistoricTable(db, tableName, columns, historicTableName, primaryKeyColumns)
    }
}


func getTableInfo(db *sql.DB, tableName string) ([]columnStruct, []columnStruct)  {
    columns := make([]columnStruct, 0, 100)
    primaryKeyColumns := make([]columnStruct, 0, 2)

    s := fmt.Sprintf(`SELECT columns.column_name, data_type, COALESCE(character_maximum_length, 0) character_maximum_length, COALESCE(numeric_precision, 0) numeric_precision, COALESCE(numeric_scale, 0) numeric_scale, COALESCE(constraint_type, '') constraint_type
        FROM information_schema.columns
        LEFT JOIN information_schema.key_column_usage ON (
            columns.table_catalog = key_column_usage.table_catalog AND
            columns.table_schema = key_column_usage.table_schema AND
            columns.table_name = key_column_usage.table_name AND
            columns.column_name = key_column_usage.column_name
        )
        LEFT JOIN information_schema.table_constraints ON (
            key_column_usage.table_catalog = table_constraints.table_catalog AND
            key_column_usage.table_schema = table_constraints.table_schema AND
            key_column_usage.table_name = table_constraints.table_name AND
            key_column_usage.constraint_name = table_constraints.constraint_name AND
            table_constraints.constraint_type = 'PRIMARY KEY'
        )
        WHERE columns.table_schema = 'public'
        AND columns.table_name = '%s'
        ORDER BY columns.ordinal_position`,
        tableName)

log.Println(s)

    rows, err := db.Query(s)
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    for rows.Next() {
        columnInfo := columnStruct{}
        err := rows.Scan(&columnInfo.columnName, &columnInfo.dataType, &columnInfo.characterMaximumLength, &columnInfo.numericPrecision, &columnInfo.numericScale, &columnInfo.constraintType)
        if err != nil {
            log.Fatal(err)
        }
        columns = append(columns, columnInfo)
        if columnInfo.constraintType == "PRIMARY KEY" {
            primaryKeyColumns = append(primaryKeyColumns, columnInfo)
        }
    }
    err = rows.Err()
    if err != nil {
        log.Fatal(err)
    }

    return columns, primaryKeyColumns
}


func copyAllRecordsToHistoricTable(db *sql.DB, tableName string, columns []columnStruct, historicTableName string, keyColumns []columnStruct) {
    columnsList := listColumns(columns)
    s := fmt.Sprintf("INSERT INTO %s (%s, first_historic_moment_id)\nSELECT %s, %d\nFROM %s",
            historicTableName,
            columnsList,
            columnsList,
            historicMomentId,
            tableName)

    if len(keyColumns) > 0 {
        s += "\nORDER BY " + listColumns(keyColumns)
    }

log.Println(s)

    _, err := db.Exec(s)
    if err != nil {
        log.Fatal(err)
    }
}


func createTable(db *sql.DB, tableName string, columns []columnStruct, primaryKeyColumns []columnStruct) {
    s := "CREATE TABLE " + tableName + " (\n"

    first := true
    for _, column := range columns {
        if !first {
            s += ", "
        }

        if column.dataType == "character varying" {
            column.dataType = "varchar"
        }

        s += `  "` + column.columnName + `" ` + column.dataType

        if column.dataType == "numeric" && column.numericPrecision > 0 {
            if column.numericScale > 0 {
                s += fmt.Sprintf("(%d,%d)", column.numericPrecision, column.numericScale)
            } else {
                s += fmt.Sprintf("(%d)", column.numericPrecision)
            }
        }

        if column.dataType == "varchar" && column.characterMaximumLength > 0 {
            s += fmt.Sprintf("(%d)", column.characterMaximumLength)
        }

        first = false
    }

    if len(primaryKeyColumns) > 0 {
        s += ",  PRIMARY KEY ("
        first = true
        for _, primaryKeyColumn := range primaryKeyColumns {
            if !first {
                s += ", "
            }
            s += "    " + primaryKeyColumn.columnName
            first = false
        }
        s += ")"
    }

    s += "\n);"

    log.Println(s)

    _, err := db.Exec(s)
    if err != nil {
        log.Fatal(err)
    }
}


func createForeignKey(db *sql.DB, childTableName string, columns []columnStruct, parentTableName string) {
    if len(columns) == 0 {
        return
    }

    s := "ALTER TABLE " + childTableName + " ADD CONSTRAINT " + parentTableName + "_fk FOREIGN KEY ("
    s += listColumns(columns)
    s += ") REFERENCES " + parentTableName + " ("
    s += listColumns(columns)
    s += ")"

log.Println(s)

    _, err := db.Exec(s)
    if err != nil {
        log.Fatal(err)
    }
}


func listColumns(columns []columnStruct) string {
    s := ""
    first := true
    for _, column := range columns {
        if !first {
            s += ", "
        }
        s += `"` + column.columnName + `"`
        first = false
    }
    return s
}


func tableExists(db *sql.DB, tableName string) bool {
    if sliceContains(tableNames, tableName) {
        return true
    }

    s := fmt.Sprintf(`SELECT COUNT(*) count
        FROM information_schema.tables
        WHERE table_schema='public'
        AND table_name='%s'`,
        tableName)
    rows, err := db.Query(s)
    if err != nil {
        log.Fatal(err)
    }

    defer rows.Close()
    rows.Next()

    var count int
    rows.Scan(&count)
    if count == 1 {
        return true
    }
    return false
}


func sliceContains(slice []string, str string) bool {
    for _, s := range slice {
        if s == str {
            return true
        }
    }
    return false
}

