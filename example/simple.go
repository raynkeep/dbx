package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/ryankeep/dbx"
)

type User struct {
	Uid        int64     `db:"uid,auto_increment"`
	Gid        int64     `db:"gid"`
	Name       string    `db:"name"`
	CreateDate time.Time `db:"createDate"`
}

var err error
var db *dbx.DB

func main() {
	os.Remove("./test.db")

	// 开启日志
	dbx.LogFile = "./db.log"
	dbx.ErrorLogFile = "./db.error.log"

	// 打开数据库
	db, err = dbx.Open("sqlite3", "./test.db")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// 参数设置
	db.SetMaxIdleConns(50)
	db.SetMaxOpenConns(2000)
	db.SetConnMaxLifetime(time.Second * 5)

	// 创建表
	_, err = db.Exec(`DROP TABLE IF EXISTS user;
CREATE TABLE user
(
  uid        INTEGER PRIMARY KEY AUTOINCREMENT,
  gid        INTEGER NOT NULL DEFAULT '0',
  name       TEXT             DEFAULT '',
  name2      TEXT             DEFAULT '',
  name3      TEXT             DEFAULT '',
  createDate DATETIME         DEFAULT CURRENT_TIMESTAMP
);`)
	if err != nil {
		panic(err)
	}

	// 插入
	for i := 1; i <= 5; i++ {
		uid, err := db.Table("user").Insert(dbx.D{
			{"gid", 1},
			{"name", "admin" + strconv.Itoa(i)},
			{"createDate", time.Now().Format("2006-01-02 15:04:05")},
		})
		if err != nil {
			panic(err)
		}
		fmt.Println("Insert:", uid)
	}

	for i := 1; i <= 5; i++ {
		st := User{}
		st.Gid = 3
		st.Name = "twoTest" + strconv.Itoa(i)
		st.CreateDate = time.Now()
		uid, err := db.Table("user").Insert(&st)
		if err != nil {
			panic(err)
		}
		fmt.Println("Insert:", uid)
	}

	// 更新
	n, err := db.Table("user").Update(dbx.D{
		{"gid", 2},
		{"name", "test"},
	}, dbx.S{
		{"uid", "=", 1},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println()
	fmt.Println("Update:", n)

	// 统计数量
	n, err = db.Table("user").Count(dbx.S{})
	if err != nil {
		panic(err)
	}
	fmt.Println()
	fmt.Println("Count:", n)

	// 读取一条 到 结构体
	row := User{}
	err = db.Table("user").Find(dbx.S{
		{"uid", "=", 1},
	}).One(&row)
	if err != nil {
		panic(err)
	}
	fmt.Println()
	fmt.Printf("Read: %+v\n", row)

	// 更新2
	row.Gid = 4
	row.Name = "test123"
	n, err = db.Table("user").Update(row, dbx.S{
		{"uid", "=", 1},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println()
	fmt.Println("Update2:", n)

	// 读取多条 到 结构体
	var list []User
	err = db.Table("user").Fields([]string{
		"uid",
		"gid",
		"name",
		"createDate",
	}).Sort([]string{"-gid", "-uid"}).Skip(0).Limit(10).Find(dbx.S{
		{"uid", "<=", 5},
	}).All(&list)
	if err != nil {
		panic(err)
	}
	fmt.Println()
	fmt.Println("List:", list)
	listByte, _ := json.Marshal(list)
	fmt.Println("Json:", string(listByte))

	// 读取一条 到 Map
	rowMap, columns, err := db.Table("user").Find(dbx.S{
		{"uid", "=", 1},
	}).OneMap()
	if err != nil {
		panic(err)
	}
	fmt.Println()
	fmt.Println("Map Columns:", columns)
	fmt.Println("Map Row:", rowMap)
	rowMapByte, _ := json.Marshal(rowMap)
	fmt.Println("Map Json:", string(rowMapByte))

	// 读取多条 到 Map
	listMap, columns, err := db.Table("user").Find(dbx.S{
		{"uid", "<", 5},
	}).AllMap()
	if err != nil {
		panic(err)
	}
	fmt.Println()
	fmt.Println("Map Columns:", columns)
	fmt.Println("Map List:", listMap)
	listMapByte, _ := json.Marshal(listMap)
	fmt.Println("Map Json:", string(listMapByte))

	// 删除
	n, err = db.Table("user").Delete(dbx.S{
		{"uid", "=", 4},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println()
	fmt.Println("Delete:", n)
}
