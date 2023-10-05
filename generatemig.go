package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

func main() {
	godotenv.Load()
	// Connect to MySQL and PostgreSQL
	mysqlConnStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		os.Getenv("MYSQL_USER"),
		os.Getenv("MYSQL_PASSWORD"),
		os.Getenv("MYSQL_HOST"),
		os.Getenv("MYSQL_PORT"),
		os.Getenv("MYSQL_DATABASE"),
	)
	mysqlDB, err := sql.Open("mysql", mysqlConnStr)
	if err != nil {
		log.Fatal(err)
	}
	defer mysqlDB.Close()

	pgConnStr := fmt.Sprintf("user=%s password=%s host=%s port=%s dbname=%s search_path=%s sslmode=disable",
		os.Getenv("PG_USER"),
		os.Getenv("PG_PASSWORD"),
		os.Getenv("PG_HOST"),
		os.Getenv("PG_PORT"),
		os.Getenv("PG_DATABASE"),
		os.Getenv("PG_SCHEMA"),
	)
	pgDB, err := sql.Open("postgres", pgConnStr)
	if err != nil {
		log.Fatal(err)
	}
	defer pgDB.Close()

	file, err := os.Open("migrationlist.csv")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer file.Close()

	// Create a CSV reader reading from the opened file
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Print each record
	for i, record := range records {
		if i == 0 {
			continue
		}
		migrationFile := "./src/migrations/" + record[0]
		table := record[1]
		targetTable := record[2]
		pgCols = make(map[string]pgcol)
		// Generate the JS migration code
		jsCode := generateJSCode(mysqlDB, pgDB, table, targetTable)
		// fmt.Println(jsCode)
		err := os.WriteFile(migrationFile, []byte(jsCode), 0777)
		if err != nil {
			fmt.Println(err)
		}
	}

}

func generateJSCode(mysqlDB, pgDB *sql.DB, table, targetTable string) string {
	jsTemplate := `require('dotenv').config();
const mysql = require('mysql2/promise');
const pgp = require('pg-promise')({
    promiseLib: require('bluebird')
});

const sourceConfig = {
    host: process.env.MYSQL_HOST,
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASSWORD,
    database: process.env.MYSQL_DATABASE
}
const destinationConfig = {
    host: process.env.PG_HOST,
    port: process.env.PG_PORT,
    database: process.env.PG_DATABASE,
    user: process.env.PG_USER,
    password: process.env.PG_PASSWORD
}


async function migrate() {
try{
    const sourceDb = await mysql.createConnection(sourceConfig);
    const destDb = pgp(destinationConfig);
`
	jsTemplate += fmt.Sprintf(`
	await Truncate(destDb,"%s.%s")
				`, os.Getenv("PG_SCHEMA"), targetTable)

	mysqlColumns := getColumnsForTable(mysqlDB, table, "mysql")
	pgColumns := getColumnsForTable(pgDB, targetTable, "postgres")

	// Match columns
	matchingColumns := getMatchingColumns(mysqlColumns, pgColumns)
	jsTemplate += generateMigrationForTable(table, targetTable, matchingColumns, mysqlColumns)

	jsTemplate += `
    console.log('Migration completed!');
} catch (error) {
    console.error('Migration failed:', error);
} 
}

function SetMapping(pgConn, Maptable, Field, oldVal, newVal){
    //Insert OldVal and NewValue in to Maptable 
	
}

function GetMapping(pgConn, Maptable, Field, oldVal){
    //Read NewVal for the Old Value from Maptable 
}

async function  RunQuery(dbConn,query){
	const [rows] = await dbConn.query(query);
	return rows
}

async function  Truncate(dbConn,table){
	await dbConn.none("truncate table " + table +";");
}

migrate();`

	return jsTemplate
}

type pgcol struct {
	Field    string
	Nullable bool
	Datatype string
}

var pgCols map[string]pgcol

func getColumnsForTable(db *sql.DB, tableName, dbType string) []string {

	var query string
	if dbType == "mysql" {
		query = fmt.Sprintf("DESCRIBE %s", tableName)
	} else if dbType == "postgres" {
		query = fmt.Sprintf("SELECT column_name, data_type,is_nullable FROM information_schema.columns WHERE table_name = '%s' AND table_schema='%s'",
			tableName, os.Getenv("PG_SCHEMA"))
	}
	rows, err := db.Query(query)
	if err != nil {
		log.Fatal("COLUMNS ERROR::", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		if dbType == "mysql" {
			var columnType, null, key string
			var defaultValue sql.NullString
			var extra string
			err = rows.Scan(&column, &columnType, &null, &key, &defaultValue, &extra)
		} else if dbType == "postgres" {
			var columnType, is_nullable string
			err = rows.Scan(&column, &columnType, &is_nullable)
			nullable := true
			if is_nullable == "NO" {
				nullable = false
			}
			pgCols[column] = pgcol{Field: column, Datatype: columnType, Nullable: nullable}
		}
		if err != nil {
			log.Fatal(err)
		}
		columns = append(columns, column)
	}
	return columns
}

// func removeDuplicates(strings []string) []string {
// 	seen := make(map[string]struct{})
// 	result := []string{}

// 	for _, str := range strings {
// 		if _, ok := seen[str]; !ok {
// 			seen[str] = struct{}{}
// 			result = append(result, str)
// 		}
// 	}
// 	return result
// }

func getMatchingColumns(mysqlColumns, pgColumns []string) []string {
	matchingColumns := make([]string, 0)
	for _, mysqlCol := range mysqlColumns {
		for _, pgCol := range pgColumns {
			if mysqlCol == pgCol {
				matchingColumns = append(matchingColumns, mysqlCol)
				break
			}
		}
	}
	return matchingColumns
}

func generateMigrationForTable(tableName, targetTable string, columns, mysqlCols []string) string {
	rowVals := generateRowAccessors(columns)
	nullHandles := ""
	for _, col := range pgCols {
		if !col.Nullable {
			if findField(columns, col.Field) < 0 {
				columns = append(columns, fmt.Sprintf(`"%s"`, col.Field))
				rowVals += ", " + getDefaultValue(col.Datatype)
				continue
			}
			field := fmt.Sprintf("row.%s", col.Field)
			nullHandles += fmt.Sprintf(`		 	
				%s = %s==null?%s:%s `,
				field, field, getDefaultValue(col.Datatype), field)
		}
	}
	return fmt.Sprintf(`

		console.log("Migrating Table %s...")
		let success = 0 
		let errors = 0 
		try {
			const [rows] = await sourceDb.execute('SELECT %s FROM %s');
			for (let row of rows) {
				// Do any field Modifications or Add additional fields 

				%s

				await destDb.none('INSERT INTO %s (%s) VALUES (%s)', [%s]).then(
					(res)=>{
						success++
					}
				)
				.catch(
					(err)=>{
						console.log("INSERT ERROR::", err)
						errors++
					}
				);
			}
			console.log('Migration of %s table completed!');
			console.log('Success Count::', success);
			console.log('Error Count::', errors);

		} catch (error) {
			console.error('Migration of %s failed:', error);
		} finally {
			await sourceDb.end();
			await destDb.$pool.end();
		}
`,
		// strings.Join(mysqlCols, ", "), tableName,
		targetTable, "*", targetTable,
		nullHandles,
		fmt.Sprintf("%s.%s", os.Getenv("PG_SCHEMA"), targetTable), strings.Join(columns, ", "), strings.Join(generatePlaceHolders(len(columns)), ", "),
		rowVals,
		targetTable, targetTable)
}

func generatePlaceHolders(length int) []string {
	var placeholders []string
	for i := 1; i <= length; i++ {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
	}
	return placeholders
}

func generateRowAccessors(columns []string) string {
	var accessors []string
	for _, col := range columns {
		accessors = append(accessors, fmt.Sprintf("row.%s", col))
	}
	return strings.Join(accessors, ", ")
}
func findField(columns []string, field string) int {
	for index, str := range columns {
		if str == field {
			return index
		}
	}
	return -1
}
func getDefaultValue(dataType string) string {
	switch dataType {
	case "integer", "bigint", "smallint", "serial", "bigserial", "numeric", "real", "double precision", "money":
		return "0"
	case "boolean":
		return "false"
	case "char", "varchar", "text", "uuid", "cidr", "inet", "macaddr", "tsvector", "json", "jsonb", "xml":
		return "''"
	case "date", "timestamp", "timestamptz", "time", "timetz", "interval":
		return `"1970-01-01"`
	case "bytea":
		return "\\x"
	default:
		return "''" // or maybe "NULL" or any other default value for types not listed above
	}
}
