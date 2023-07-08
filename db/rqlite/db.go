package dqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/rqlite/gorqlite"
)

const (
	rqliteAddr        = "rqlte.addr"
	rqliteAddrDefault = "http://127.0.0.1:4001"
	tableName         = "usertable"
	primaryKey        = "YCSB_KEY"
	columnName        = "YCSB_VALUE"
)

type rqliteCreator struct {
}

type rqliteDB struct {
	p *properties.Properties
	c *gorqlite.Connection
}

func (c rqliteCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	db := new(rqliteDB)
	db.p = p

	serverAddr := p.GetString(rqliteAddr, rqliteAddrDefault)
	conn, err := gorqlite.Open(serverAddr)
	if err != nil {
		return nil, err
	}

	conn.SetConsistencyLevel(gorqlite.ConsistencyLevelStrong)

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ( %s TEXT NOT NULL PRIMARY KEY, %s BLOB NOT NULL);", tableName, primaryKey, columnName)
	_, err = conn.WriteOne(query)
	if err != nil {
		return nil, err
	}

	db.c = conn
	return db, err
}

func (db *rqliteDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *rqliteDB) CleanupThread(_ context.Context) {
}

func (db *rqliteDB) Close() error {
	db.c.Close()
	return nil
}

func (db *rqliteDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?;", columnName, tableName, primaryKey)
	args := make([]interface{}, 0, 1)
	args = append(args, key)
	paramQuery := gorqlite.ParameterizedStatement{
		Query:     query,
		Arguments: args,
	}
	rows, err := db.c.QueryOneParameterized(paramQuery)
	if err != nil {
		return nil, err
	}
	if rows.NumRows() != 1 {
		return nil, fmt.Errorf("[Read] Key %s not found, nRows %d", key, rows.NumRows())
	}
	rows.Next()
	result := make(map[string][]byte)
	value, _ := rows.Map()
	if err = json.Unmarshal([]byte(value[columnName].(string)), &result); err != nil {
		return nil, err
	}

	return result, err
}

func (db *rqliteDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, errors.New("not implemented")
}

func (db *rqliteDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	query := fmt.Sprintf("UPDATE %s SET %s = ? WHERE %s = ?;", tableName, columnName, primaryKey)
	args := make([]interface{}, 0, 2)
	jsonVal, err := json.Marshal(values)
	if err != nil {
		return err
	}
	args = append(args, jsonVal)
	args = append(args, key)
	paramQuery := gorqlite.ParameterizedStatement{
		Query:     query,
		Arguments: args,
	}
	_, err = db.c.QueueOneParameterized(paramQuery)
	return err
}

func (db *rqliteDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	query := fmt.Sprintf("INSERT INTO %s (%s,%s) VALUES(?, ?);", tableName, primaryKey, columnName)
	args := make([]interface{}, 0, 2)
	args = append(args, key)
	jsonVal, err := json.Marshal(values)
	if err != nil {
		return err
	}
	args = append(args, string(jsonVal))
	paramQuery := gorqlite.ParameterizedStatement{
		Query:     query,
		Arguments: args,
	}
	fmt.Print(paramQuery)
	_, err = db.c.QueueOneParameterized(paramQuery)
	return err
}

func (db *rqliteDB) Delete(ctx context.Context, table string, key string) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE %s = ?;", tableName, primaryKey)
	args := make([]interface{}, 0, 1)
	args = append(args, key)
	paramQuery := gorqlite.ParameterizedStatement{
		Query:     query,
		Arguments: args,
	}
	_, err := db.c.QueueOneParameterized(paramQuery)
	return err
}

func init() {
	ycsb.RegisterDBCreator("rqlite", rqliteCreator{})
}
