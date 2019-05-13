package dbx

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

type DB struct {
	*sqlx.DB
}

type Query struct {
	*DB
	table    string
	fields   []string
	selector []Selector
	orderBy  []string
	skip     int64
	limit    int64
}

type S []Selector

type Selector struct {
	Field  string
	Symbol string
	Value  interface{}
}

type D []DocElem

type DocElem struct {
	Field string
	Value interface{}
}

func Open(driverName, dataSourceName string) (*DB, error) {
	db, err := sqlx.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	LogInit()
	return &DB{DB: db}, err
}

func (db *DB) Table(name string) *Query {
	return &Query{DB: db, table: name}
}

func (q *Query) Fields(fields []string) *Query {
	q.fields = fields
	return q
}

func (q *Query) Find(selector S) *Query {
	q.selector = selector
	return q
}

func (q *Query) Sort(orderBy []string) *Query {
	q.orderBy = orderBy
	return q
}

func (q *Query) Limit(limit int64) *Query {
	q.limit = limit
	return q
}

func (q *Query) Skip(skip int64) *Query {
	q.skip = skip
	return q
}

func (q *Query) InsertS(st interface{}) (id int64, err error) {
	id, err = q.Insert(S2D(st))
	return
}

func (q *Query) Insert(data D) (id int64, err error) {
	kStr, vStr, args := GetSqlInsert(data)
	s := "INSERT INTO `" + q.table + "`(" + kStr + ") VALUES (" + vStr + ")"
	LogWrite(s, args...)

	var stmt *sql.Stmt
	stmt, err = q.Prepare(s)
	if err != nil {
		ErrorLogWrite(err, s, args...)
		return
	}
	defer stmt.Close()

	var res sql.Result
	res, err = stmt.Exec(args...)
	if err != nil {
		ErrorLogWrite(err, s, args...)
		return
	}

	id, err = res.LastInsertId()
	if err != nil {
		ErrorLogWrite(err, s, args...)
		return
	}
	return
}

func (q *Query) InsertIgnore(data D) (id int64, err error) {
	kStr, vStr, args := GetSqlInsert(data)
	s := "INSERT IGNORE INTO `" + q.table + "`(" + kStr + ") VALUES (" + vStr + ")"
	LogWrite(s, args...)

	res, err := q.Exec(s, args...)
	if err != nil {
		ErrorLogWrite(err, s, args...)
		return
	}

	id, err = res.LastInsertId()
	if err != nil {
		ErrorLogWrite(err, s, args...)
		return
	}
	return
}

func (q *Query) Replace(data D) (id int64, err error) {
	kStr, vStr, args := GetSqlInsert(data)
	s := "REPLACE INTO `" + q.table + "`(" + kStr + ") VALUES (" + vStr + ")"
	LogWrite(s, args...)

	res, err := q.Exec(s, args...)
	if err != nil {
		ErrorLogWrite(err, s, args...)
		return
	}

	id, err = res.LastInsertId()
	if err != nil {
		ErrorLogWrite(err, s, args...)
		return
	}
	return
}

func (q *Query) Update(data D, where S) (n int64, err error) {
	setStr, args := GetSqlUpdate(data)
	whereStr, args2 := GetSqlWhere(where)
	args = append(args, args2...)

	s := "UPDATE `" + q.table + "` SET " + setStr + whereStr
	LogWrite(s, args...)

	var stmt *sql.Stmt
	stmt, err = q.Prepare(s)
	if err != nil {
		ErrorLogWrite(err, s, args...)
		return
	}
	defer stmt.Close()

	var res sql.Result
	res, err = stmt.Exec(args...)
	if err != nil {
		ErrorLogWrite(err, s, args...)
		return
	}

	n, err = res.RowsAffected()
	return
}

func (q *Query) Delete(where S) (n int64, err error) {
	whereStr, args := GetSqlWhere(where)
	s := "DELETE FROM `" + q.table + "`" + whereStr
	LogWrite(s, args...)

	var stmt *sql.Stmt
	stmt, err = q.Prepare(s)
	if err != nil {
		ErrorLogWrite(err, s, args...)
		return
	}
	defer stmt.Close()

	var res sql.Result
	res, err = stmt.Exec(args...)
	if err != nil {
		ErrorLogWrite(err, s, args...)
		return
	}

	n, err = res.RowsAffected()
	if err != nil {
		ErrorLogWrite(err, s, args...)
		return
	}
	return
}

func (q *Query) Count(where S) (n int64, err error) {
	whereStr, args := GetSqlWhere(where)
	s := "SELECT COUNT(*) FROM `" + q.table + "`" + whereStr
	LogWrite(s, args...)

	var stmt *sql.Stmt
	stmt, err = q.Prepare(s)
	if err != nil {
		ErrorLogWrite(err, s, args...)
		return
	}
	defer stmt.Close()

	err = stmt.QueryRow(args...).Scan(&n)
	if err != nil {
		ErrorLogWrite(err, s, args...)
		return
	}
	return
}

func (q *Query) One(dest interface{}) (err error) {
	fields := GetSqlFields(q.fields)
	whereStr, args := GetSqlWhere(q.selector)
	s := "SELECT " + fields + " FROM `" + q.table + "`" + whereStr + " LIMIT 1"
	LogWrite(s, args...)

	udb := q.Unsafe()
	err = udb.Get(dest, s, args...)
	if err != nil {
		if err != sql.ErrNoRows {
			ErrorLogWrite(err, s, args...)
		}
	}
	return
}

func (q *Query) All(dest interface{}) (err error) {
	fields := GetSqlFields(q.fields)
	whereStr, args := GetSqlWhere(q.selector)
	orderStr := GetSqlOrderBy(q.orderBy)
	limitStr := GetSqlLimit(q.skip, q.limit)
	s := "SELECT " + fields + " FROM `" + q.table + "`" + whereStr + orderStr + limitStr
	LogWrite(s, args...)

	udb := q.Unsafe()
	err = udb.Select(dest, s, args...)
	if err != nil {
		ErrorLogWrite(err, s, args...)
		return
	}
	return
}

func (q *Query) OneMap() (row map[string]interface{}, columns []string, err error) {
	whereStr, args := GetSqlWhere(q.selector)
	fields := GetSqlFields(q.fields)
	s := "SELECT " + fields + " FROM `" + q.table + "`" + whereStr
	LogWrite(s, args...)

	var rows *sql.Rows
	rows, err = q.Query(s, args...)
	if err != nil {
		ErrorLogWrite(err, s, args...)
		return
	}
	defer rows.Close()

	if rows.Next() {
		m := map[string]interface{}{}
		columns, err = MapScan(rows, m)
		if err != nil {
			ErrorLogWrite(err, s, args...)
			return
		}
		row = m
	}
	return
}

func (q *Query) AllMap() (list []map[string]interface{}, columns []string, err error) {
	fields := GetSqlFields(q.fields)
	whereStr, args := GetSqlWhere(q.selector)
	orderStr := GetSqlOrderBy(q.orderBy)
	limitStr := GetSqlLimit(q.skip, q.limit)
	s := "SELECT " + fields + " FROM `" + q.table + "`" + whereStr + orderStr + limitStr
	LogWrite(s, args...)

	var rows *sql.Rows
	rows, err = q.Query(s, args...)
	if err != nil {
		ErrorLogWrite(err, s, args...)
		return
	}
	defer rows.Close()

	for rows.Next() {
		m := map[string]interface{}{}
		columns, err = MapScan(rows, m)
		if err != nil {
			ErrorLogWrite(err, s, args...)
			return
		}
		list = append(list, m)
	}
	return
}

// 绑定到 Map
func MapScan(r *sql.Rows, dest map[string]interface{}) (columns []string, err error) {
	columns, err = r.Columns()
	if err != nil {
		return
	}

	values := make([]interface{}, len(columns))
	for i := range values {
		values[i] = new(interface{})
	}

	err = r.Scan(values...)
	if err != nil {
		return
	}

	for i, column := range columns {
		dest[column] = *(values[i].(*interface{}))
	}

	err = r.Err()
	return
}

func GetSqlFields(fields []string) (s string) {
	if len(fields) > 0 {
		for _, field := range fields {
			s += "`" + field + "`,"
		}
		s = strings.TrimRight(s, ",")
	} else {
		s = "*"
	}
	return
}

func GetSqlOrderBy(arr []string) (s string) {
	if len(arr) > 0 {
		s = " ORDER BY "
		for _, v := range arr {
			v = strings.Replace(v, " ", "", -1)
			symbol := v[0:1]
			if symbol == "-" {
				s += "`" + v[1:] + "` DESC,"
			} else {
				s += "`" + v + "` ASC,"
			}
		}
		s = strings.TrimRight(s, ",")
	}
	return
}

func GetSqlLimit(skip, limit int64) (s string) {
	if limit > 0 {
		s = " LIMIT " + strconv.FormatInt(skip, 10) + "," + strconv.FormatInt(limit, 10)
	}
	return
}

func GetSqlInsert(data D) (kStr, vStr string, args []interface{}) {
	for _, v := range data {
		kStr += "`" + v.Field + "`, "
		vStr += "?, "
		args = append(args, v.Value)
	}
	kStr = strings.TrimSuffix(kStr, ", ")
	vStr = strings.TrimSuffix(vStr, ", ")
	return
}

func GetSqlUpdate(data D) (setStr string, args []interface{}) {
	for _, v := range data {
		symbol := v.Field[0:1]
		if symbol == "+" || symbol == "-" {
			field := v.Field[1:]
			setStr += "`" + field + "`=`" + field + "`" + symbol + "?, "
		} else {
			setStr += "`" + v.Field + "`=?, "
		}
		args = append(args, v.Value)
	}
	setStr = strings.TrimSuffix(setStr, ", ")
	return
}

func GetSqlWhere(selector S) (whereStr string, args []interface{}) {
	if len(selector) == 0 {
		return
	}
	whereStr = " WHERE "
	for _, v := range selector {
		if v.Symbol == "IN" {
			s2 := ""
			switch t := v.Value.(type) {
			case []int:
				arr := v.Value.([]int)
				for _, v2 := range arr {
					s2 += "?,"
					args = append(args, v2)
				}
			case []int64:
				arr := v.Value.([]int64)
				for _, v2 := range arr {
					s2 += "?,"
					args = append(args, v2)
				}
			case []string:
				arr := v.Value.([]string)
				for _, v2 := range arr {
					s2 += "?,"
					args = append(args, v2)
				}
			default:
				fmt.Println("Unsupported types:", t)
			}
			if s2 != "" {
				s2 = strings.Trim(s2, ",")
				whereStr += "`" + v.Field + "` IN (" + s2 + ") AND "
			}
		} else {
			whereStr += "`" + v.Field + "` " + v.Symbol + " ? AND "
			args = append(args, v.Value)
		}
	}
	whereStr = strings.TrimSuffix(whereStr, " AND ")
	return
}

func S2D(t interface{}) (d []DocElem) {
	st := reflect.TypeOf(t)
	sv := reflect.ValueOf(t)
	for i := 0; i < st.NumField(); i++ {
		f := st.Field(i)
		if field := f.Tag.Get("db"); field != "" {
			if strings.Contains(field, ",") {
				arr := strings.Split(field, ",")
				if arr[1] != "auto_increment" {
					d = append(d, DocElem{arr[0], sv.Field(i).Interface()})
				}
			} else {
				d = append(d, DocElem{field, sv.Field(i).Interface()})
			}
		}
	}
	return
}

var LogFile string
var ErrorLogFile string

var LogIoWriter io.Writer = os.Stdout

func LogInit() {
	if LogFile != "" {
		var err error
		LogIoWriter, err = os.OpenFile(LogFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			panic(err)
		}
	}
}

func LogWrite(s string, args ...interface{}) {
	if LogFile == "" {
		return
	}
	_, err := fmt.Fprintf(LogIoWriter, "%v | %s\n",
		time.Now().Format("2006-01-02 15:04:05"),
		fmt.Sprintf(strings.Replace(s, "?", "'%v'", -1), ReplaceSlash(args...)...),
	)
	if err != nil {
		fmt.Println(err)
	}
}

func ErrorLogWrite(e error, s string, args ...interface{}) {
	if ErrorLogFile == "" {
		return
	}

	f, err := os.OpenFile(ErrorLogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
	}

	str := fmt.Sprintf("%v | ERROR: %v | SQL: %s\n",
		time.Now().Format("2006-01-02 15:04:05"),
		e.Error(),
		fmt.Sprintf(strings.Replace(s, "?", "'%v'", -1), ReplaceSlash(args...)...),
	)
	if _, err := f.Write([]byte(str)); err != nil {
		fmt.Println(err)
	}

	if err := f.Close(); err != nil {
		fmt.Println(err)
	}
}

func ReplaceSlash(args ...interface{}) []interface{} {
	for k := range args {
		if s, ok := args[k].(string); ok {
			s = strings.Replace(s, "'", "\\'", -1)
			s = strings.Replace(s, "\\", "\\\\", -1)
			args[k] = s
		}
	}
	return args
}
