// Go support for LLSN - Allyst's data interchange format.
// LLSN specification http://allyst.org/opensource/llsn/
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Library General Public License for more details.
//
// Full license: https://github.com/allyst/go-llsn/blob/master/LICENSE
//
// copyright (C) 2014 Allyst Inc. http://allyst.com
// author Taras Halturin <halturin@allyst.com>

package llsn

import (
	"io"
	"os"
	"reflect"
	"time"
	"unicode/utf8"
)

func encode_ext(value reflect.Value, channel chan []byte, threshold uint16) {

	var stack *stackElement // = &stackElement{}
	var tail, tail_first *tailElement
	var tt *typesTree = &typesTree{}
	var nullflags []byte
	var mdf bool = false // multidimensional array flag
	var index func(int) reflect.Value

	tail = &tailElement{}
	tail_first = tail

	defer close(channel)

	i := uint64(0)
	n := uint64(value.NumField())

	index = value.Field

	// encode version and threshold
	channel <- []byte{byte(((threshold >> 8) & 0xf) | (VERSION << 4)), byte(threshold)}
	channel <- EncodeUNumber(uint64(n))

	// because of Go has no tail recoursion we use "for" loop to emulate it
	for {

		if i >= n {

			if stack == nil {
				break
			}

			// 'pop' from 'stack'
			i = stack.i
			n = stack.n
			value = stack.value
			index = stack.index
			nullflags = stack.nullflags

			stack = stack.parent
			tt = tt.parent.next

			if stack == nil {
				mdf = false
			}

			continue
		}

		if nullflags != nil {

			// every 8 items should leads by nullflag byte
			if i%8 == 0 {
				channel <- []byte{nullflags[i/8]}
			}

			// skip value if its nil.
			// see 'encodeNullFlags' how the flags being encoded
			if (nullflags[i/8])&(1<<(7-(uint(i)%8))) > 0 {
				tt = tt.append(tt.ttype)
				i++
				continue
			}
		}

		field := index(int(i))

		// some items are pointers to the values. dereference it and start
		// process over here
	dereference:

		switch field.Kind() {
		case reflect.Array, reflect.Slice:

			switch a := field.Interface().(type) {
			case Blob:
				// because of Blob type is an array of bytes, so we catch it here.
				if field.Len() > 0 {
					var blen uint64
					var blob Blob

					if tt.ttype == type_undefined {
						channel <- []byte{type_blob}
						tt = tt.append(type_blob)
					} else {
						tt = tt.next
					}

					blen, blob, tail = encodeBlob(a, tail)
					channel <- EncodeUNumber(blen)

					// is exceed the threshold limit?
					if blob != nil {
						channel <- blob
					}

				} else {
					// blob value is nil
					if tt.ttype == type_undefined {
						channel <- []byte{type_blob_null}
						tt = tt.append(type_blob)
					} else {
						tt = tt.next
					}
				}

			// Array, Slice
			default:
				if field.Len() > 0 {
					var ta int = type_array // set to 'type_arrayn' if array have null values

					// FIXME: tail optimization -> dont increase 'stack' if
					// the i'th element is the last one in array
					stack = &stackElement{stack, i + 1, n, value, index, nullflags}

					nullflags = encodeNullFlags(field, mdf)
					if nullflags != nil {
						ta = type_arrayn
					}

					// set up multidimensional array flag
					mdf = true

					i = uint64(0)
					n = uint64(field.Len())
					index = field.Index
					value = field

					if tt.ttype == type_undefined {
						tt = tt.addchild(ta)
						tt.next = tt
						channel <- []byte{byte(ta)}
					} else {
						tt = tt.child
					}

					channel <- EncodeUNumber(uint64(n))
					continue

				} else {
					// array is nil
					var ta, tan int

					switch field.Type().Elem().Kind() {
					case reflect.Slice, reflect.Ptr:
						ta = type_arrayn
						tan = type_arrayn_null
					default:
						ta = type_array
						tan = type_array_null
					}

					if tt.ttype == type_undefined {
						tt.ttype = ta
						channel <- []byte{byte(tan)}
					}

					if tt.child == nil {
						tt1 := tt.addchild(ta)
						tt1.next = tt1
					}

					tt = tt.next
				}
			}

		case reflect.Struct:

			// there are lot of custom types based on a 'struct' , so we need
			// to recognize some of them to encode it into 'type_date' and 'type_file'
			// but any other types should be processed like a regular struct

			switch ct := field.Interface().(type) {
			case time.Time:
				if tt.ttype == type_undefined {
					channel <- []byte{type_date}
					tt = tt.append(type_date)
				} else {
					tt = tt.next
				}

				channel <- EncodeDate(&ct)

			case File:
				var tailed bool = false
				var bin []byte

				if tt.ttype == type_undefined {
					channel <- []byte{type_file}
					tt = tt.append(type_file)
				} else {
					tt = tt.next
				}

				tailed, bin, tail = encodeFile(ct, tail)

				// write file name and size
				channel <- bin
				// write body of file if itsnt tailed
				if !tailed {
					file_to_channel(ct, channel)
				}

			default:
				stack = &stackElement{stack, i + 1, n, value, index, nullflags}

				i = uint64(0)
				n = uint64(field.NumField())
				index = field.Field
				value = field
				nullflags = nil

				if tt.ttype == type_undefined {
					channel <- []byte{type_struct}
					channel <- EncodeUNumber(uint64(n))
					tt.n = n
					tt = tt.addchild(type_struct)

				} else {

					if tt.n == 0 {
						channel <- EncodeUNumber(uint64(n))
						tt.n = n
					} else {
						// field types of struct seems to be already encoded
						nullflags = encodeNullFlags(field, false)
					}
					tt = tt.child
				}

				continue
			}

		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			// encode signed number
			if tt.ttype == type_undefined {
				channel <- []byte{type_number}
				tt = tt.append(type_number)
			} else {
				tt = tt.next
			}

			channel <- EncodeNumber(int64(field.Int()))

		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			// encode unsigned number
			if tt.ttype == type_undefined {
				channel <- []byte{type_unumber}
				tt = tt.append(type_unumber)
			} else {
				tt = tt.next
			}
			channel <- EncodeUNumber(uint64(field.Uint()))

		case reflect.Float32, reflect.Float64:
			// encode float number
			if tt.ttype == type_undefined {
				channel <- []byte{type_float}
				tt = tt.append(type_float)
			} else {
				tt = tt.next
			}

			channel <- EncodeFloat(float64(field.Float()))

		case reflect.Bool:
			// encode boolean
			if tt.ttype == type_undefined {
				channel <- []byte{type_bool}
				tt = tt.append(type_bool)
			} else {
				tt = tt.next
			}

			if field.Bool() {
				channel <- []byte{1}
			} else {
				channel <- []byte{0}
			}

		case reflect.String:
			// encode string
			var binlen, bin []byte

			if tt.ttype == type_undefined {
				channel <- []byte{type_string}
				tt = tt.append(type_string)
			} else {
				tt = tt.next
			}

			binlen, bin, tail = encodeString(field.String(), tail)
			channel <- binlen // length of string in octet(bytes)
			if bin != nil {
				// string is not tailed. encode it
				channel <- bin
			}

		case reflect.Ptr:
			if field.IsNil() {
				switch field.Type().Elem().Kind() {
				case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64:
					// nil value for number
					if tt.ttype == type_undefined {
						channel <- []byte{type_number_null}
						tt = tt.append(type_number)
					} else {
						tt = tt.next
					}

				case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					//nil value for unsigned number
					if tt.ttype == type_undefined {
						channel <- []byte{type_unumber_null}
						tt = tt.append(type_unumber)
					} else {
						tt = tt.next
					}

				case reflect.Float32, reflect.Float64:
					// nil value for float
					if tt.ttype == type_undefined {
						channel <- []byte{type_float_null}
						tt = tt.append(type_float)
					} else {
						tt = tt.next
					}

				case reflect.String:
					// nil value for string
					if tt.ttype == type_undefined {
						channel <- []byte{type_string_null}
						tt = tt.append(type_string)
					} else {
						tt = tt.next
					}

				case reflect.Bool:
					// nil value for bool
					if tt.ttype == type_undefined {
						channel <- []byte{type_bool_null}
						tt = tt.append(type_bool)
					} else {
						tt = tt.next
					}

				case reflect.Struct:
					switch field.Interface().(type) {
					case *time.Time:
						// nil value for date
						if tt.ttype == type_undefined {
							channel <- []byte{type_date_null}
							tt = tt.append(type_date)
						} else {
							tt = tt.next
						}

					case *File:
						// nil value for file
						if tt.ttype == type_undefined {
							channel <- []byte{type_file_null}
							tt = tt.append(type_file)
						} else {
							tt = tt.next
						}

					default:
						// nil value for struct
						if tt.ttype == type_undefined {
							tt.ttype = type_struct
							channel <- []byte{type_struct_null}
						}

						if tt.child == nil {
							tt.addchild(type_struct)
						}

						tt = tt.next
					}

				// Arrays and Blobs should be processed like a regular value

				default:
					panic("unsupported type for LLSN encoding (nil value): " + field.Type().Elem().String())

				}

			} else {
				// pointer to value. dereference it...
				field = field.Elem()
				goto dereference
			}

		default:
			panic("unsupported type for LLSN encoding: " + field.Type().String())
		}

		i++
	}

	// Tail processing (> threshold).
	if tail_first.next != nil {

		for tail = tail_first.next; tail != nil; tail = tail.next {
			switch tv := tail.value.Interface().(type) {
			case File:
				file_to_channel(tv, channel)
			case Blob:
				channel <- []byte(tv)
			case string:
				channel <- []byte(tv)
			default:
				panic("wrong tail type")

			}
		}
	}

	return
}

// encode tab:
//
// 1111 1111   [....          8 bytes ....]  - 64 bits number (9 bytes)
// 1111 1110   [....          7 bytes ....]  - 56 bits number (8 bytes)
// 1111 110 .  [1 bit  + .... 6 bytes ....]  - 49 bits number (7 bytes)
// 1111 10 ..  [2 bits + .... 5 bytes ....]  - 42 bits number (6 bytes)
// 1111 0 ...  [3 bits + .... 4 bytes ....]  - 35 bits number (5 bytes)
// 1110  ....  [4 bits + .... 3 bytes ....]  - 28 bits number (4 bytes)
// 110.  ....  [5 bits + .... 2 bytes ....]  - 21 bits number (3 bytes)
// 10..  ....  [6 bits + .... 1 byte  ....]  - 14 bits number (2 bytes)
// 0...  ....  [7 bits]                      - 7 bits number  (1 byte )

func EncodeNumber(number int64) []byte {
	var num uint64

	if 0 > number {
		num = uint64(-number)
	} else {
		num = uint64(number)
	}

	switch {
	case (num & 0x3f) == num: // 1 byte
		return pack_number(number&0x7f, 1)

	case (num & 0x1fff) == num: // 2 bytes
		return pack_number((0x2<<14)|(number&0x3fff), 2)

	case (num & 0xfffff) == num: // 3 bytes
		return pack_number((0x6<<21)|(number&0x1fffff), 3)

	case (num & 0x7ffffff) == num: // 4 bytes
		return pack_number((0xe<<28)|(number&0xfffffff), 4)

	case (num & 0x3ffffffff) == num: // 5 bytes
		return pack_number((0x1e<<35)|(number&0x7ffffffff), 5)

	case (num & 0x1ffffffffff) == num: // 6 bytes
		return pack_number((0x3e<<42)|(number&0x3ffffffffff), 6)

	case (num & 0xffffffffffff) == num: // 7 bytes
		return pack_number((0x7e<<49)|(number&0x1ffffffffffff), 7)

	case (num & 0x7fffffffffffff) == num: // 8 bytes
		return pack_number(number, 8)

	default: // 9 bytes
		return pack_number(number, 9)

	}

}

func EncodeUNumber(number uint64) []byte {

	switch {
	case (number & 0x7f) == number: // 1 byte
		return pack_number(number, 1)

	case (number & 0x3fff) == number: // 2 bytes
		return pack_number((0x2<<14)|number, 2)

	case (number & 0x1fffff) == number: // 3 bytes
		return pack_number((0x6<<21)|number, 3)

	case (number & 0xfffffff) == number: // 4 bytes
		return pack_number((0xe<<28)|number, 4)

	case (number & 0x7ffffffff) == number: // 5 bytes
		return pack_number((0x1e<<35)|number, 5)

	case (number & 0x3ffffffffff) == number: // 6 bytes
		return pack_number((0x3e<<42)|number, 6)

	case (number & 0x1ffffffffffff) == number: // 7 bytes
		return pack_number((0x7e<<49)|number, 7)

	case (number & 0xffffffffffffff) == number: // 8 bytes
		return pack_number(number, 8)

	default: // 9 bytes
		return pack_number(number, 9)

	}

}

func EncodeFloat(f float64) []byte {
	var i uint8
	var p float64 = 10.0

	// calculate power of 10 for 'f' float number
	// example: 3.141596 -> 3141596*10(-6)
	// encoded: 1B:power of 10 and 1..9B:signed number for the value

	for i = 1; ; i++ {
		if f*p > float64(int(f*p)) {
			p *= 10.0
			continue
		}

		break
	}
	return append([]byte{i}, EncodeNumber(int64(f*p))...)
}

func encodeString(s string, tail *tailElement) ([]byte, []byte, *tailElement) {

	length := uint64(len(s))

	if length > STRING_MAXBYTES {
		panic("The limit of string length is exceeded")
	}

	if !utf8.ValidString(s) {
		panic("String is not valid UTF8")
	}

	bl := EncodeUNumber(length)
	if (threshold > 0) && (length > uint64(threshold)) && (tail != nil) {
		// len of value > threshold. push it to the tail
		tail = tail.append(reflect.ValueOf(s), length)
		return bl, nil, tail
	} else {
		return bl, []byte(s), tail
	}

}

// 2B: year. (-32767..32768)
//   :4b month (1..12)
//   :5b day of month (1..31)
//   :5b hour (0..23)
//   :6b min (0..59)
//   :6b sec (0..59)
//   :10 msec (0..999)
//   :6b hours offset (signed)
//   :6b min offset (unsigned)
//   -- :48bit
// --
// 8B total
//

func EncodeDate(t *time.Time) []byte {

	var year int
	var month time.Month
	var day int
	var hour, min, sec, nsec int
	var date int64
	var zone int

	bin := make([]byte, 8)

	year, month, day = t.Date()
	hour, min, sec = t.Clock()
	_, zone = t.Zone()

	date |= int64(year) << 48
	date |= int64(month) << 44
	date |= int64(day) << 39
	date |= int64(hour) << 34
	date |= int64(min) << 28
	date |= int64(sec) << 22

	// range of nanoseconds: 0..999999999. [https://golang.org/src/time/time.go]
	// it can be stored in 30bits.
	// max range of LLSN Data.nsec: 0..4095 (12 bits)
	// truncate it to fit the value in 12 bits.
	nsec = t.Nanosecond() / 1000000
	if nsec > 4095 {
		date |= int64(nsec/10) << 12
	} else {
		date |= int64(nsec) << 12
	}

	// timezone. hours, mins
	date |= int64(zone/3600) << 6
	// only hours is signed
	if zone < 0 {
		zone *= -1
	}
	date |= int64(zone % 3600 / 60)

	for i := uint8(0); i < 8; i++ {
		bin[i] = byte(date >> ((7 - i) * 8))
	}
	return bin
}

func encodeBlob(b Blob, tail *tailElement) (uint64, Blob, *tailElement) {
	length := uint64(len(b))

	if length > BLOB_MAXBYTES {
		panic("The limit of blob length is exceeded")
	}

	if (threshold > 0) && (length > uint64(threshold)) && (tail != nil) {
		// len of value > threshold
		tail = tail.append(reflect.ValueOf(b), length)
		return length, nil, tail
	} else {
		return length, b, tail
	}
}

func encodeFile(f File, tail *tailElement) (bool, []byte, *tailElement) {
	var fi os.FileInfo
	var err error
	var binfilelen, binnamelen, binname []byte

	if err != nil {
		panic(err)
	}

	fi, err = os.Stat(f.Name)
	if fi == nil {
		panic("Can not open file (" + f.Name + ")")
	}

	length := uint64(fi.Size())

	//[filesize:NUM,namelen:NUM,name]
	binfilelen = EncodeUNumber(length)
	binnamelen, binname, _ = encodeString(fi.Name(), nil)
	bin := append(binnamelen, binname...)
	bin = append(binfilelen, bin...)

	if (threshold > 0) && (length > uint64(threshold)) && (tail != nil) {
		tail = tail.append(reflect.ValueOf(f), length)
		return true, bin, tail
	}

	return false, bin, tail
}

// Encode helpers //////////////////////////////////////////////////////////////

func pack_number(value interface{}, n uint8) []byte {
	buffer := make([]byte, n)

	var v uint64

	switch value.(type) {
	case int64:
		v = uint64(value.(int64))
	case uint64:
		v = value.(uint64)
	}

	for i := uint8(0); i < n; i++ {
		if n < 8 || i > 0 {
			buffer[i] = byte(v >> ((n - i - 1) * 8))
		} else {
			if n == 8 {
				buffer[i] = 0xfe
			} else {
				buffer[i] = 0xff
			}

		}
	}

	return buffer
}

// encodeNullFlags.
// nullbit sets like bigendian. for example... array of 8th elements with nil values
// at the first and last places:
// a[0] == nil set the first bit: 0b10000000
// a[7] == nil set the  last one: 0b00000001
// so, byteflag = 0b10000001
func encodeNullFlags(v reflect.Value, force bool) []byte {
	var method func(int) reflect.Value
	var nelements func() int
	var hasnil bool = force

	switch v.Kind() {
	case reflect.Struct:
		method = v.Field
		nelements = v.NumField

	case reflect.Array, reflect.Slice:
		method = v.Index
		nelements = v.Len

	default:
		panic("internal error")

	}
	// allocate slice of bytes for every element of array/struct. 1 value = 1 bit.
	// 0 - value,  1 - nil
	n := nelements()
	flags := make([]byte, n/8+1)

	for i := 0; i < n; i++ {
		val := method(i)
		if (val.Kind() == reflect.Ptr || val.Kind() == reflect.Slice) && val.IsNil() {
			// set 'nil' flag
			flags[i/8] |= 1 << (7 - (uint(i) % 8))
			hasnil = true
		}
	}

	if hasnil || (v.Kind() == reflect.Struct) {
		return flags
	}

	return nil
}

func file_to_channel(f File, channel chan []byte) {
	var readbytes int
	var err error
	var buffer []byte

	buffer = make([]byte, 65536) // 64K

	of, err := os.Open(f.Name)
	if err != nil {
		panic(err)
	}
	defer of.Close()

	for {
		readbytes, err = of.Read(buffer)
		if readbytes > 0 {
			channel <- buffer[:readbytes]
		}

		switch err {
		case nil:
			continue
		case io.EOF:
			return
		default:
			panic(err)
		}
	}
}
