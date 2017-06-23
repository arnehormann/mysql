// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2017 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

// +build mysqldebug

package mysql

import (
	"fmt"
	"runtime"
	"strings"
)

type frames []uintptr

const (
	traceSkip = 3
	traceLen  = 4
)

func trace(skip, max int) frames {
	if max <= 0 {
		max = 5
	}
	pcs := make([]uintptr, max)
	n := runtime.Callers(skip, pcs)
	if n == 0 {
		return nil
	}
	return frames(pcs)
}

func (fs frames) String() string {
	if len(fs) == 0 {
		return "[unknown]"
	}
	s := "["
	frames := runtime.CallersFrames([]uintptr(fs))
	for {
		f, more := frames.Next()
		s += fmt.Sprintf("%s:%d", f.File[strings.LastIndexByte(f.File, '/')+1:], f.Line)
		if !more {
			break
		}
		s += ";"
	}
	s += "]"
	return s
}

func (b *buffer) aquire() {
	b.frames = trace(traceSkip, traceLen)
}

func (b *buffer) collision() {
	errLog.Print("buffer owned by ", b.frames, " wanted by ", trace(traceSkip, traceLen))
}
