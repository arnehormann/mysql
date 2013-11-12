// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

func (rows *binaryRows) init() error {
	storers := make([]storeCol, len(rows.columns))
	for i, col := range rows.columns {
		unsigned := col.flags&flagUnsigned != 0
		switch col.fieldType {
		case fieldTypeNULL:
			storers[i] = storeNull
		case fieldTypeTiny:
			if unsigned {
				storers[i] = storeUint8
			} else {
				storers[i] = storeInt8
			}
		case fieldTypeShort, fieldTypeYear:
			if unsigned {
				storers[i] = storeUint16
			} else {
				storers[i] = storeInt16
			}
		case fieldTypeInt24, fieldTypeLong:
			if unsigned {
				storers[i] = storeUint32
			} else {
				storers[i] = storeInt32
			}
		case fieldTypeLongLong:
			if unsigned {
				storers[i] = storeUint64
			} else {
				storers[i] = storeInt64
			}
		case fieldTypeFloat:
			storers[i] = storeFloat32
		case fieldTypeDouble:
			storers[i] = storeFloat64
		case fieldTypeDecimal, fieldTypeNewDecimal, fieldTypeVarChar,
			fieldTypeBit, fieldTypeEnum, fieldTypeSet, fieldTypeTinyBLOB,
			fieldTypeMediumBLOB, fieldTypeLongBLOB, fieldTypeBLOB,
			fieldTypeVarString, fieldTypeString, fieldTypeGeometry:
			storers[i] = storeLengthCodedBinary
		case fieldTypeDate, fieldTypeNewDate:
			storers[i] = rows.mc.dateStorer(false)
		case fieldTypeTime:
			storers[i] = storeTime
		case fieldTypeTimestamp, fieldTypeDateTime:
			storers[i] = rows.mc.dateStorer(true)
		default:
			return fmt.Errorf("Unknown FieldType %d in column %d", col.fieldType, i)
		}
	}
	rows.storer = storers
	return nil
}

type storeCol func(dest *driver.Value, data []byte, pos int) (int, error)

// fieldTypeNULL
func storeNull(dest *driver.Value, data []byte, pos int) (int, error) {
	*dest = nil
	return 0, nil
}

// fieldTypeTiny + flagUnsigned
func storeUint8(dest *driver.Value, data []byte, pos int) (int, error) {
	*dest = int64(uint8(data[pos]))
	return 1, nil
}

// fieldTypeTiny
func storeInt8(dest *driver.Value, data []byte, pos int) (int, error) {
	*dest = int64(int8(data[pos]))
	return 1, nil
}

// fieldTypeShort, fieldTypeYear + flagUnsigned
func storeUint16(dest *driver.Value, data []byte, pos int) (int, error) {
	*dest = int64(binary.LittleEndian.Uint16(data[pos : pos+2]))
	return 2, nil
}

// fieldTypeShort, fieldTypeYear
func storeInt16(dest *driver.Value, data []byte, pos int) (int, error) {
	*dest = int64(int16(binary.LittleEndian.Uint16(data[pos : pos+2])))
	return 2, nil
}

// fieldTypeInt24, fieldTypeLong + flagUnsigned
func storeUint32(dest *driver.Value, data []byte, pos int) (int, error) {
	*dest = int64(binary.LittleEndian.Uint32(data[pos : pos+4]))
	return 4, nil
}

// fieldTypeInt24, fieldTypeLong
func storeInt32(dest *driver.Value, data []byte, pos int) (int, error) {
	*dest = int64(int16(binary.LittleEndian.Uint32(data[pos : pos+4])))
	return 4, nil
}

// fieldTypeLongLong + flagUnsigned
func storeUint64(dest *driver.Value, data []byte, pos int) (int, error) {
	*dest = binary.LittleEndian.Uint64(data[pos : pos+8])
	return 8, nil
}

// fieldTypeLongLong
func storeInt64(dest *driver.Value, data []byte, pos int) (int, error) {
	*dest = int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
	return 8, nil
}

// fieldTypeFloat
func storeFloat32(dest *driver.Value, data []byte, pos int) (int, error) {
	*dest = float64(math.Float32frombits(binary.LittleEndian.Uint32(data[pos : pos+4])))
	return 4, nil
}

// fieldTypeDouble
func storeFloat64(dest *driver.Value, data []byte, pos int) (int, error) {
	*dest = math.Float64frombits(binary.LittleEndian.Uint64(data[pos : pos+8]))
	return 8, nil
}

// fieldTypeDecimal, fieldTypeNewDecimal, fieldTypeVarChar,
// fieldTypeBit, fieldTypeEnum, fieldTypeSet, fieldTypeTinyBLOB,
// fieldTypeMediumBLOB, fieldTypeLongBLOB, fieldTypeBLOB,
// fieldTypeVarString, fieldTypeString, fieldTypeGeometry
func storeLengthCodedBinary(dest *driver.Value, data []byte, pos int) (int, error) {
	var isNull bool
	var n int
	var err error
	*dest, isNull, n, err = readLengthEncodedString(data[pos:])
	if isNull {
		*dest = nil
	}
	return n, err
}

// fieldTypeDate, fieldTypeNewDate
func storeBinaryDate(dest *driver.Value, data []byte, pos int) (int, error) {
	// Date YYYY-MM-DD
	num, isNull, n := readLengthEncodedInteger(data[pos:])
	if isNull {
		*dest = nil
		return n, nil
	}
	var err error
	*dest, err = formatBinaryDate(num, data[pos+n:])
	return n + int(num), err
}

// // fieldTypeTimestamp, fieldTypeDateTime
func storeBinaryDateTime(dest *driver.Value, data []byte, pos int) (int, error) {
	// Date YYYY-MM-DD
	num, isNull, n := readLengthEncodedInteger(data[pos:])
	if isNull {
		*dest = nil
		return n, nil
	}
	var err error
	*dest, err = formatBinaryDateTime(num, data[pos+n:])
	return n + int(num), err
}

type timeLocation struct {
	loc *time.Location
}

// fieldTypeDate, fieldTypeNewDate
func (t timeLocation) storeParsedBinaryDate(dest *driver.Value, data []byte, pos int) (int, error) {
	// Date YYYY-MM-DD
	num, isNull, n := readLengthEncodedInteger(data[pos:])
	if isNull {
		*dest = nil
		return n, nil
	}
	var err error
	*dest, err = parseBinaryDateTime(num, data[pos+n:], t.loc)
	return n + int(num), err
}

func (mc *mysqlConn) dateStorer(dateTime bool) storeCol {
	if mc.parseTime {
		return timeLocation{mc.cfg.loc}.storeParsedBinaryDate
	}
	if dateTime {
		return storeBinaryDateTime
	}
	return storeBinaryDate
}

// fieldTypeTime
func storeTime(dest *driver.Value, data []byte, pos int) (int, error) {
	// Time [-][H]HH:MM:SS[.fractal]
	// maximum for HOUR is a lot lower than the protocol supports,
	// we only need one byte for days because values get clamped:
	// SELECT cast('839:00:00' as TIME), cast('-839:00:00' as TIME)
	// => '838:59:59', '-838:59:59'
	num, isNull, n := readLengthEncodedInteger(data[pos:])
	if num == 0 {
		if isNull {
			*dest = nil
			return n, nil
		}
		*dest = []byte("00:00:00")
		return n, nil
	}
	var sign string
	if data[pos+n] == 1 {
		sign = "-"
	}
	pos += n + 1
	if num == 8 {
		*dest = []byte(fmt.Sprintf(
			sign+"%02d:%02d:%02d",
			// value clamping: ignore data[pos+1:pos+4]
			uint16(data[pos])*24+uint16(data[pos+4]),
			data[pos+5],
			data[pos+6],
		))
		return n + 8, nil
	}
	if num == 12 {
		*dest = []byte(fmt.Sprintf(
			sign+"%02d:%02d:%02d.%06d",
			// value clamping: ignore data[pos+1:pos+4]
			uint16(data[pos])*24+uint16(data[pos+4]),
			data[pos+5],
			data[pos+6],
			binary.LittleEndian.Uint32(data[pos+7:pos+11]),
		))
		return n + 12, nil
	}
	return n, fmt.Errorf("Invalid TIME-packet length %d", num)
}
