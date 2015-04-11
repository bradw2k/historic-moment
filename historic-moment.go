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

type statisticsStruct struct {
    newCount int
    updatedCount int
    deletedCount int
    errorCount int
    workLog string
}

var verbose bool
var historicMomentId int
var tableNames []string
var statistics statisticsStruct


func main() {
    verbose = true
    statistics = statisticsStruct{}
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
        s = `CREATE TABLE historic_moments (
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

    verboseLog(fmt.Sprintf("historicMomentId = %d", historicMomentId))

    for _, tableName := range tableNames {
        processTable(db, tableName)
    }

    s = fmt.Sprintf(`UPDATE historic_moments
        SET new_count=%d, updated_count=%d, deleted_count=%d, error_count=%d, work_log='%s', completed_at=CURRENT_TIMESTAMP
        WHERE id=%d`,
            statistics.newCount,
            statistics.updatedCount,
            statistics.deletedCount,
            statistics.errorCount,
            statistics.workLog,
            historicMomentId);

    verboseLog(s)

    _, err = db.Exec(s)
    if err != nil {
        log.Fatal(err)
    }
}


func processTable(db *sql.DB, tableName string) {
    verboseLog(tableName)

    columns, primaryKeyColumns := getTableInfo(db, tableName)

    historicTableName := tableName + "_historic"
    historicColumns := make([]columnStruct, len(columns), len(columns) + 2)
    copy(historicColumns, columns)
    historicColumns = append([]columnStruct{columnStruct{"last_historic_moment_id", "integer", 0, 0, 0, ""}}, historicColumns...)
    historicColumns = append([]columnStruct{columnStruct{"first_historic_moment_id", "integer", 0, 0, 0, ""}}, historicColumns...)

    historicPrimaryKeyColumns := make([]columnStruct, 0, 3)
    if len(primaryKeyColumns) == 0 {
        primaryKeyColumns = columns
    }

    historicPrimaryKeyColumns = append(primaryKeyColumns, columnStruct{"first_historic_moment_id", "int", 0, 0, 0, ""})

    if tableExists(db, historicTableName) {
        addMissingColumns(db, historicTableName, historicColumns)
        statistics.updatedCount += addHistoricRecordsForNewAndChangedRecords(db, tableName, columns, primaryKeyColumns, historicTableName, historicColumns)
        setLastHistoricMomentIdOnPreviousHistoricRecords(db, tableName, columns, primaryKeyColumns, historicTableName, historicColumns)
        statistics.deletedCount += setLastHistoricMomentIdForDeletedRecords(db, tableName, columns, primaryKeyColumns, historicTableName, historicColumns)
    } else {
        statistics.newCount += createHistoricTable(db, tableName, columns, primaryKeyColumns, historicTableName, historicColumns, historicPrimaryKeyColumns)
    }
}


func addHistoricRecordsForNewAndChangedRecords(db *sql.DB, tableName string, columns []columnStruct, primaryKeyColumns []columnStruct, historicTableName string, historicColumns []columnStruct) int {
    first := true
    onClause := "\n"
    for _, column := range columns {
        if !first {
            onClause += " AND\n"
        }

        template := `         %s."%s" IS NOT DISTINCT FROM %s."%s"`
        if containsColumn(primaryKeyColumns, column) {
            template = `         %s."%s" = %s."%s"`
        }

        onClause += fmt.Sprintf(template,
            tableName,
            column.columnName,
            historicTableName,
            column.columnName)
        first = false
    }

    s := fmt.Sprintf(`INSERT INTO %s(%s)
        SELECT %d, NULL, %s
        FROM %s
        LEFT JOIN %s ON (%s)
        WHERE %s."%s" IS NULL`,
        historicTableName,
        listColumns(historicColumns),
        historicMomentId,
        listColumnsWithTableName(tableName, columns),
        tableName,
        historicTableName,
        onClause,
        historicTableName,
        primaryKeyColumns[0].columnName)

    verboseLog(s)

    _, err := db.Exec(s)
    if err != nil {
        log.Fatal(err)
    }

    return 0
}


func setLastHistoricMomentIdOnPreviousHistoricRecords(db *sql.DB, tableName string, columns []columnStruct, primaryKeyColumns []columnStruct, historicTableName string, historicColumns []columnStruct) int {
    whereClause := "\n"
    for _, column := range columns {
        whereClause += fmt.Sprintf(`            AND %s."%s" = innie."%s"` + "\n",
            historicTableName,
            column.columnName,
            column.columnName)
    }

    s := fmt.Sprintf(`UPDATE %s
        SET "last_historic_moment_id" = %d
        WHERE "first_historic_moment_id" != %d
        AND "last_historic_moment_id" IS NULL
        AND EXISTS (
            SELECT 1
            FROM %s innie
            WHERE "first_historic_moment_id" = %d%s)`,
        historicTableName,
        historicMomentId,
        historicMomentId,
        historicTableName,
        historicMomentId,
        whereClause)

    verboseLog(s)

    _, err := db.Exec(s)
    if err != nil {
        log.Fatal(err)
    }

    return 0
}


func setLastHistoricMomentIdForDeletedRecords(db *sql.DB, tableName string, columns []columnStruct, primaryKeyColumns []columnStruct, historicTableName string, historicColumns []columnStruct) int {
    first := true
    whereClause := ""
    for _, column := range primaryKeyColumns {
        if !first {
            whereClause += "\n            AND "
        }
        whereClause += fmt.Sprintf(`%s."%s" = %s."%s"`,
            tableName,
            column.columnName,
            historicTableName,
            column.columnName)
        first = false
    }

    s := fmt.Sprintf(`UPDATE %s
        SET "last_historic_moment_id" = %d
        WHERE "last_historic_moment_id" IS NULL
        AND NOT EXISTS(
            SELECT 1
            FROM %s
            WHERE %s
        )`,
        historicTableName,
        historicMomentId,
        tableName,
        whereClause)

    verboseLog(s)

    _, err := db.Exec(s)
    if err != nil {
        log.Fatal(err)
    }

    return 0
}


func addMissingColumns(db *sql.DB, tableName string, requiredColumns []columnStruct) {
    existingColumns, _ := getTableInfo(db, tableName)

    for _, requiredColumn := range requiredColumns {
        if containsColumn(existingColumns, requiredColumn) {
            continue;
        }

        s := fmt.Sprintf(`ALTER TABLE "%s" ADD %s`, tableName, getColumnSpecification(requiredColumn))

        log.Println(s)

        _, err := db.Exec(s)
        if err != nil {
            log.Fatal(err)
        }
    }
}


func createHistoricTable(db *sql.DB, tableName string, columns []columnStruct, primaryKeyColumns []columnStruct, historicTableName string, historicColumns []columnStruct, historicPrimaryKeyColumns []columnStruct) int {
    if len(primaryKeyColumns) > 0 {
        historicPrimaryKeyColumns := append(primaryKeyColumns, columnStruct{"first_historic_moment_id", "int", 0, 0, 0, ""})
        createTable(db, historicTableName, historicColumns, historicPrimaryKeyColumns)
    } else {
        createTable(db, historicTableName, historicColumns, primaryKeyColumns)
    }

    //createForeignKey(db, historicTableName, primaryKeyColumns, tableName)
    return copyAllRecordsToHistoricTable(db, tableName, columns, historicTableName, primaryKeyColumns)
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

    verboseLog(s)

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


func copyAllRecordsToHistoricTable(db *sql.DB, tableName string, columns []columnStruct, historicTableName string, keyColumns []columnStruct) int {
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

    verboseLog(s)

    _, err := db.Exec(s)
    if err != nil {
        log.Fatal(err)
    }

    s = "SELECT COUNT(*) count FROM " + historicTableName
    rows, err := db.Query(s)
    if err != nil {
        log.Fatal(err)
    }

    defer rows.Close()
    rows.Next()

    var count int
    rows.Scan(&count)
    return count
}


func createTable(db *sql.DB, tableName string, columns []columnStruct, primaryKeyColumns []columnStruct) {
    s := "CREATE TABLE " + tableName + " (\n"

    first := true
    for _, column := range columns {
        if !first {
            s += ", "
        }

        s += getColumnSpecification(column)

        first = false
    }

    if len(primaryKeyColumns) > 0 {
        s += fmt.Sprintf(",  PRIMARY KEY (%s)", listColumns(primaryKeyColumns))
    }

    s += "\n);"

    verboseLog(s)

    _, err := db.Exec(s)
    if err != nil {
        log.Fatal(err)
    }
}


func getColumnSpecification(column columnStruct) string {
    if column.dataType == "character varying" {
        column.dataType = "varchar"
    }

    s := `  "` + column.columnName + `" ` + column.dataType

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

    return s
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

    verboseLog(s)

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


func listColumnsWithTableName(tableName string, columns []columnStruct) string {
    s := ""
    first := true
    for _, column := range columns {
        if !first {
            s += ", "
        }
        s += tableName + `."` + column.columnName + `"`
        first = false
    }
    return s
}


func tableExists(db *sql.DB, tableName string) bool {
    if containsString(tableNames, tableName) {
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


func containsString(slice []string, str string) bool {
    for _, s := range slice {
        if s == str {
            return true
        }
    }
    return false
}


func containsColumn(slice []columnStruct, column columnStruct) bool {
    for _, c := range slice {
        if c.columnName == column.columnName {
            return true
        }
    }
    return false
}


func verboseLog(s string) {
    if verbose {
        log.Println(s)
    }
}

