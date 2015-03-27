// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2015 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Printer is a function that can be used as a Logger.
type Printer func(v ...interface{})

func (p Printer) Print(v ...interface{}) {
	p(v...)
}

// useLogger sets the logger and returns a function resetting it to the old one.
// Use it as
//     defer useLogger(LOGGER)()
func useLogger(p Printer) func() {
	var logger = errLog
	SetLogger(p)
	return func() {
		SetLogger(logger)
	}
}

// testTimebound runs a test and fails if it takes longer than allowed.
func testTimebound(t *testing.T, timeout time.Duration, f func(*testing.T)) {
	timer := time.After(timeout)
	done := make(chan struct{})
	defer close(done)
	go func() {
		f(t)
		done <- struct{}{}
	}()
	select {
	case <-timer:
		t.Fatal("TIMEOUT")
	case <-done:
	}
}

// tracer is useable as a Logger, prints full stacktrace of all package internal calls
type tracer struct{}

func (t tracer) Print(v ...interface{}) {
	trace := []string{}
	var (
		dstPkg string
		file   string
		line   int
	)
	ok := true
	for i := 0; ok; i++ {
		_, file, line, ok = runtime.Caller(i)
		if !ok {
			break
		}
		pkg := ""
		pEnd := 0
		for j := len(file) - 2; j > 0; j-- {
			if file[j] == '/' {
				if pEnd != 0 {
					pkg = file[j+1 : pEnd]
					if i == 0 {
						dstPkg = pkg
					}
					file = file[j+1:]
					break
				}
				pEnd = j
			}
		}
		if pkg == dstPkg && i > 0 {
			trace = append(trace, fmt.Sprintf("%s:%d", file, line))
		}
	}
	fmt.Fprintln(
		os.Stderr,
		append([]interface{}{"[" + strings.Join(trace, ", ") + "]: "}, v...),
	)
}
