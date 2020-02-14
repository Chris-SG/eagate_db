package eagate_db

import (
	"fmt"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
)

var (
	db *gorm.DB
	idleConnectionLimit int

)

func OpenDb(user string, password string, dbname string, host string, maxIdleConnections int) (*gorm.DB, error) {
	connStr := fmt.Sprintf( "user=%s password=%s dbname=%s host=%s sslmode=disable", user, password, dbname, host)

	newDb, err := gorm.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	db = newDb
	db.DB().SetMaxIdleConns(maxIdleConnections)
	idleConnectionLimit = maxIdleConnections

	return db, nil
}

func GetDb() (*gorm.DB, error) {
	if db == nil {
		return nil, fmt.Errorf("db connection has not been created, please use OpenDb()")
	}
	return db, nil
}

func GetIdleConnectionLimit() int {
	return idleConnectionLimit
}