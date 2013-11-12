// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2013 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"database/sql"
	"fmt"
	"testing"
	"time"
)

func (tb *TB) checkResult(res sql.Result, err error) sql.Result {
	tb.check(err)
	return res
}

type column struct {
	receiver interface{}
	sql      string
	sample   func(int) interface{}
}

func benchmarkMultirowScan(b *testing.B, cols ...*column) {
	const tableName = "testNextSpeed"
	const maxRows = 1000000
	b.StopTimer()
	b.ReportAllocs()
	tb := (*TB)(b)
	sqlDrop := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	sqlCreate := fmt.Sprintf("CREATE TABLE %s (", tableName)
	sqlInsert := fmt.Sprintf("INSERT INTO %s SET ", tableName)
	for i, c := range cols {
		if i > 0 {
			sqlCreate += ", "
			sqlInsert += ", "
		}
		sqlCreate += fmt.Sprintf("`%s` %s", c.sql, c.sql)
		sqlInsert += fmt.Sprintf("`%s`=?", c.sql)
	}
	sqlCreate += ")"
	db := tb.checkDB(sql.Open("mysql", dsn))
	defer db.Close()
	/*
		_, _ = db.Exec(sqlDrop)
		_, err := db.Exec(sqlCreate)
		if err != nil {
			b.Fatalf("Error on %q: %v", sqlCreate, err)
		}
		insert := tb.checkStmt(db.Prepare(sqlInsert))
		defer insert.Close()
		insertRows := b.N
		if insertRows > maxRows {
			b.Logf("downgraded table length from %d to %d rows\n", insertRows, maxRows)
			insertRows = maxRows
		}
		colValues := make([]interface{}, len(cols))
		for i := 0; i < b.N; i++ {
			for i, c := range cols {
				colValues[i] = c.sample(i)
			}
			tb.checkResult(insert.Exec(colValues...))
		}
	*/
	var _ = sqlDrop
	query := tb.checkStmt(db.Prepare(fmt.Sprintf("SELECT * FROM %s", tableName)))
	defer query.Close()
	rows, err := query.Query()
	if err != nil {
		b.Fatal(err)
	}
	receivers := make([]interface{}, len(cols))
	for i, c := range cols {
		receivers[i] = &c.receiver
	}
	defer rows.Close()
	b.StartTimer()
	for i := b.N; i > 0 && rows.Next(); i-- {
		if err := rows.Scan(receivers...); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMultirowSelect(b *testing.B) {
	var receivers []interface{}
	gencol := func(sql string, f func(int) interface{}) *column {
		r := f(0)
		receivers = append(receivers, r)
		return &column{
			receiver: r,
			sql:      sql,
			sample:   f,
		}
	}
	benchmarkMultirowScan(b,
		gencol("TINYINT UNSIGNED", func(i int) interface{} {
			return uint8(i)
		}),
		gencol("TINYINT", func(i int) interface{} {
			return int8(-i)
		}),
		gencol("SMALLINT UNSIGNED", func(i int) interface{} {
			return uint16(i)
		}),
		gencol("SMALLINT", func(i int) interface{} {
			return int16(-i)
		}),
		gencol("INT UNSIGNED", func(i int) interface{} {
			return uint32(i)
		}),
		gencol("INT", func(i int) interface{} {
			return int32(-i)
		}),
		gencol("BIGINT UNSIGNED", func(i int) interface{} {
			return uint64(i)
		}),
		gencol("BIGINT", func(i int) interface{} {
			return int64(-i)
		}),
		gencol("FLOAT", func(i int) interface{} {
			return float32(i)
		}),
		gencol("DOUBLE", func(i int) interface{} {
			return float32(i)
		}),
		gencol("DECIMAL", func(i int) interface{} {
			return float64(i)
		}),
		gencol("YEAR", func(i int) interface{} {
			return int16(i)
		}),
		gencol("DATE", func(i int) interface{} {
			return time.Unix(int64(i)*24*60*60, 0)
		}),
		/*gencol("TIME", func(i int) interface{} {
			return time.Unix(int64(i), 0)
		}),*/
		/*gencol("TIMESTAMP", func(i int) interface{} {
			return time.Unix(int64(i), 0)
		}),*/
		gencol("DATETIME", func(i int) interface{} {
			return time.Unix(int64(i), 0)
		}),
		/*
			gencol("BIT", func(i int) interface{} {
				return float32(i)
			}),
		*/
		gencol("VARCHAR(255)", func(i int) interface{} {
			return "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_+=/|"[:i%68]
		}),
		gencol("CHAR(255)", func(i int) interface{} {
			return "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_+=/|"[:i%68]
		}),
		/*
			gencol("TINY BLOB", func(i int) interface{} {
				return float32(i)
			}),
			gencol("MEDIUM BLOB", func(i int) interface{} {
				return float32(i)
			}),
			gencol("BLOB", func(i int) interface{} {
				return float32(i)
			}),
			gencol("LONG BLOB", func(i int) interface{} {
				return float32(i)
			}),
		*/
	)
}
