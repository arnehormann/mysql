// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2015 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"database/sql"
	"database/sql/driver"
	"testing"
	"time"
)

// test for issue #314:
// Busy buffer. Commands out of sync. Did you run multiple statements at once?

func test314Text(t *testing.T) {
	dsts := []driver.Value{sql.NullInt64{}}
	db, err := MySQLDriver{}.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, _ := db.(*mysqlConn)
	rows, err := conn.Query("SELECT 1", nil)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	rows.Next(dsts) // busy buffer
	rows.Close()
}

func test314Binary(t *testing.T) {
	dsts := []driver.Value{sql.NullInt64{}}
	conn, err := MySQLDriver{}.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	stmt, err := conn.Prepare("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := stmt.Query(nil)
	if err != nil {
		t.Fatal(err)
	}
	stmt.Close() // busy buffer
	rows.Next(dsts)
	rows.Close() // freeze
	conn.Close()
}

func Test314Text(t *testing.T) {
	if !available {
		t.Skipf("MySQL-Server not running on %s", netAddr)
	}
	defer useLogger(t.Error)()
	testTimebound(t, 1*time.Second, test314Text)
}

func Test314Binary(t *testing.T) {
	if !available {
		t.Skipf("MySQL-Server not running on %s", netAddr)
	}
	defer useLogger(t.Error)()
	testTimebound(t, 1*time.Second, test314Binary)
}
