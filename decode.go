// Golang support for LLSN - Allyst's data interchange format.
// LLSN specification http://allyst.org/opensource/llsn/

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// copyright (C) 2015 Allyst Inc. http://allyst.com
// author Taras Halturin <halturin@allyst.com>

package llsn

import (
	"io/ioutil"
	"math"
	"reflect"
	"time"
)

func decode_ext(buffer *decodeBuffer, value *reflect.Value) {
	var value_type int

	var stack *stackElement = &stackElement{}
	var tt *typesTree = &typesTree{}
	var tail *tailElement = &tailElement{}
	var tail_first *tailElement = tail
	var version uint8

	head := buffer.read(2)
	// 4 bits - version, 12 bits - threshold
	version = uint8(head[0]) >> 4

	if version != 1 {
		panic("Unsupported version")
	}

	head[0] &= 0xf
	threshold = (uint16(head[0]&0xf) << 8) | uint16(head[1])

	stack.n = decodeUNumber(buffer)
	stack.index = value.Field

	for {

		if stack.i >= stack.n {
			stack = stack.parent
			if stack == nil {
				break
			}
			tt = tt.parent.next
			continue
		}

		if stack.nullflags != nil {
			if stack.i > 0 && stack.i%8 == 0 {
				//read null flag
				stack.nullflags = buffer.read(1)
			}

			flags := stack.nullflags[0]

			// have to skip if the NULL flag is set
			if flags&(1<<(7-(uint(stack.i)%8))) > 0 {
				// NULL value. skip it
				if tt.next == nil {
					tt = tt.append(tt.ttype)
				} else {
					tt = tt.next
				}
				stack.i++
				continue
			}
		}

		if tt.ttype == type_undefined {
			value_type = int(buffer.read(1)[0])
		} else {
			value_type = tt.ttype
		}

		field := stack.index(int(stack.i))

		switch value_type {

		// STRUCT
		case type_struct:
			var n uint64
			var nullflags []byte

			if tt.ttype == type_undefined {
				n = decodeUNumber(buffer)
				tt.n = n
				nullflags = []byte{0}
			} else {
				if tt.n == 0 {
					n = decodeUNumber(buffer)
					nullflags = nil
					tt.n = n
				} else {
					n = tt.n
					nullflags = buffer.read(1)
				}
			}

			if field.Kind() == reflect.Ptr {
				pstruct := reflect.New(field.Type().Elem())
				field.Set(pstruct)
				field = pstruct.Elem()
			}

			stack.i += 1
			stack = &stackElement{stack, 0, n, field, field.Field, nullflags}

			if tt.child == nil {
				tt = tt.addchild(value_type)
			} else {
				tt = tt.child
			}

			continue

		case type_struct_null:
			if tt.child == nil {
				tt.addchild(type_struct)
			}
			value_type = type_struct

		// ARRAY, ARRAYN
		case type_array, type_arrayn:
			var n uint64
			var nullflags []byte

			n = decodeUNumber(buffer)

			if value_type == type_arrayn || tt.ttype != type_undefined {
				nullflags = buffer.read(1)
			}

			if field.Kind() == reflect.Slice {
				field.Set(reflect.MakeSlice(field.Type(), int(n), int(n)))
			}

			stack.i += 1
			stack = &stackElement{stack, 0, n, field, field.Index, nullflags}

			if tt.child == nil {
				tt = tt.addchild(value_type)
				tt.next = tt
			} else {
				tt = tt.child
			}

			continue

		case type_array_null, type_arrayn_null:
			var tt1 *typesTree

			if tt.child == nil {

				if value_type == type_array_null {
					value_type = type_array
				} else {
					value_type = type_arrayn
				}

				tt1 = tt.addchild(value_type)
				tt1.next = tt1
			}

		// NUMBER
		case type_number:
			num := decodeNumber(buffer)

			if field.Kind() == reflect.Ptr {
				ifield := reflect.New(field.Type().Elem())
				ifield.Elem().SetInt(num)
				field.Set(ifield)
			} else {
				field.SetInt(num)
			}

		case type_number_null:
			ifield := reflect.NewAt(field.Type().Elem(), nil)
			field.Set(ifield)
			value_type = type_number

		// UNUMBER
		case type_unumber:
			num := decodeUNumber(buffer)

			if field.Kind() == reflect.Ptr {
				ifield := reflect.New(field.Type().Elem())
				ifield.Elem().SetUint(num)
				field.Set(ifield)
			} else {
				field.SetUint(num)
			}

		case type_unumber_null:
			ifield := reflect.NewAt(field.Type().Elem(), nil)
			field.Set(ifield)
			value_type = type_unumber

		// FLOAT
		case type_float:
			f := decodeFloat(buffer)

			if field.Kind() == reflect.Ptr {
				ifield := reflect.New(field.Type().Elem())
				ifield.Elem().SetFloat(f)
				field.Set(reflect.ValueOf((*float64)(&f)))
			} else {
				field.SetFloat(f)
			}
		case type_float_null:
			ifield := reflect.NewAt(field.Type().Elem(), nil)
			field.Set(ifield)
			value_type = type_float

		// BOOL
		case type_bool:
			var b bool

			if buffer.read(1)[0] == 1 {
				b = true
			}

			if field.Kind() == reflect.Ptr {
				field.Set(reflect.ValueOf((*bool)(&b)))
			} else {
				field.SetBool(b)
			}

		case type_bool_null:
			ifield := reflect.NewAt(field.Type().Elem(), nil)
			field.Set(ifield)
			value_type = type_bool

		// STRING
		case type_string:
			string_len := decodeUNumber(buffer)

			if (threshold > 0) && (string_len > uint64(threshold)) && (tail != nil) {
				// len of value > threshold. push it to the tail
				tail = tail.append(field, string_len)

			} else {

				s := string(buffer.read(string_len))

				if field.Kind() == reflect.Ptr {
					field.Set(reflect.ValueOf((*string)(&s)))
				} else {
					field.SetString(s)
				}
			}

		case type_string_null:
			field.Set(reflect.ValueOf((*string)(nil)))
			value_type = type_string

		// DATE
		case type_date:
			dt := decodeDate(buffer)

			if field.Kind() == reflect.Ptr {
				field.Set(reflect.ValueOf((*time.Time)(dt)))
			} else {
				field.Set(reflect.ValueOf((time.Time)(*dt)))
			}

		case type_date_null:
			field.Set(reflect.ValueOf((*time.Time)(nil)))
			value_type = type_date

		// BLOB
		case type_blob:
			blob_len := decodeUNumber(buffer)

			if (threshold > 0) && (blob_len > uint64(threshold)) && (tail != nil) {
				// len of value > threshold. push it to the tail
				tail = tail.append(field, blob_len)

			} else {
				field.Set(reflect.ValueOf((Blob)(buffer.read(blob_len))))
			}

		case type_blob_null:
			field.Set(reflect.ValueOf((Blob)(nil)))
			value_type = type_blob

		// FILE
		case type_file:
			var file *File
			var filevalue reflect.Value
			var file_len, filename_len uint64

			if field.Kind() == reflect.Ptr {
				if field.IsNil() {
					file = new(File)
					filevalue = reflect.ValueOf(&file)
					field.Set(filevalue)
				} else {
					file = field.Interface().(*File)
				}

			} else {
				file = field.Addr().Interface().(*File)
			}

			file_len = decodeUNumber(buffer)
			filename_len = decodeUNumber(buffer)
			file.Name = string(buffer.read(filename_len))
			file.length = file_len

			if (threshold > 0) && (file_len > uint64(threshold)) && (tail != nil) {
				// len of value > threshold. push it to the tail
				tail = tail.append(field, file_len)
			} else {
				decodeFile(buffer, file)
			}

		case type_file_null:
			field.Set(reflect.ValueOf((*File)(nil)))
			value_type = type_file

		default:
			panic("FIXME. bug!")
		}

		stack.i += 1
		if tt.next == nil {
			tt = tt.append(value_type)
		} else {
			if tt.ttype == type_undefined {
				tt.ttype = value_type
			}
			tt = tt.next
		}
	} // end of main loop

	// tail data processing
	if tail_first.next != nil {

		for tail = tail_first.next; tail != nil; tail = tail.next {
			switch v := tail.value.Interface().(type) {
			case File, *File:
				var file *File

				if tail.value.Kind() == reflect.Ptr {
					file = tail.value.Interface().(*File)
				} else {
					file = tail.value.Addr().Interface().(*File)
				}

				decodeFile(buffer, file)

			case string, Blob:
				val := buffer.read(tail.length)

				switch v.(type) {
				case Blob:
					tail.value.Set(reflect.ValueOf((Blob)(val)))

				case string:
					if tail.value.Kind() == reflect.Ptr {
						str := string(val)
						tail.value.Set(reflect.ValueOf((*string)(&str)))
					} else {
						str := string(val)
						tail.value.SetString(str)
					}

				}

			default:
				panic("Wrong tail type")
			}

			// in case of valueParted we have to start at the last processed tail element
			tail_first = tail.next
		}

	}

}

func DecodeNumber(buffer []byte) int64 {
	var b decodeBuffer

	b.init_buffer(buffer)

	return decodeNumber(&b)
}

func decodeNumber(buffer *decodeBuffer) int64 {
	var l uint8
	var val, mask uint64

	b := buffer.read(1)

	switch {
	case (b[0] >> 7) == 0: // 0... ....
		l = 1

		if b[0]&0x40 > 0 { // 0100 0000
			b[0] |= 0x80
			mask = 0xFFFFFFFFFFFFFF00 //7

		} else {
			b[0] &= 0x3f
		}

		return int64(uint64(b[0]) | mask)

	case (b[0] >> 6) == 0x2: // 10.. ....
		l = 2
		if b[0]&0x20 > 0 { // 0010 0000
			b[0] |= 0x40
			mask = 0xFFFFFFFFFFFF8000 //14

		} else {
			b[0] &= 0x1f
		}

	case (b[0] >> 5) == 0x6: // 110. ....
		l = 3
		if b[0]&0x10 > 0 { // 0001 0000
			b[0] |= 0x20
			mask = 0xFFFFFFFFFFC00000 //21

		} else {
			b[0] &= 0xf
		}

	case (b[0] >> 4) == 0xe: // 1110 ....
		l = 4
		if b[0]&0x8 > 0 { // 0000 1000
			b[0] |= 0x10
			mask = 0xFFFFFFFFE0000000 //28

		} else {
			b[0] &= 0x7
		}

	case (b[0] >> 3) == 0x1e: // 1111 0...
		l = 5
		if b[0]&0x4 > 0 { // 0000 0100
			b[0] |= 0x8
			mask = 0xFFFFFFF000000000 //35

		} else {
			b[0] &= 0x3
		}

	case (b[0] >> 2) == 0x3e: // 1111 10..
		l = 6
		if b[0]&0x2 > 0 { // 0000 0010
			mask = 0xFFFFF80000000000 //42
			b[0] |= 0x4
		} else {
			b[0] &= 0x1
		}

	case (b[0] >> 1) == 0x7e: // 1111 110.
		l = 7
		if b[0]&0x1 > 0 { // 0000 0001
			mask = 0xFFFC000000000000 //49
			b[0] |= 0x2
		} else {
			b[0] &= 0
		}

	case b[0] == 0xfe: // 1111 1110
		l = 8

		bb := buffer.look(1)
		if bb[0]&0x80 > 0 {
			mask = 0xFF00000000000000
		}
		// b[0] &= 0xff

	case b[0] == 0xff: // 1111 1111
		l = 9
	}

	bb := buffer.read(uint64(l - 1))

	if l < 8 {
		val = unpack_number(append(b, bb...), l)
	} else {
		val = unpack_number(bb, l-1)
	}

	return int64(val | mask)
}

func DecodeUNumber(buffer []byte) uint64 {
	var b decodeBuffer

	b.init_buffer(buffer)

	return decodeUNumber(&b)
}

func decodeUNumber(buffer *decodeBuffer) uint64 {
	var l uint8
	var value uint64

	b := buffer.read(1)

	switch {
	case (b[0] >> 7) == 0: // 0... ....
		return uint64(b[0])

	case (b[0] >> 6) == 0x2: // 10.. ....
		l = 2
		b[0] &= 0x3f
	case (b[0] >> 5) == 0x6: // 110. ....
		l = 3
		b[0] &= 0x1f
	case (b[0] >> 4) == 0xe: // 1110 ....
		l = 4
		b[0] &= 0xf
	case (b[0] >> 3) == 0x1e: // 1111 0...
		l = 5
		b[0] &= 0x7
	case (b[0] >> 2) == 0x3e: // 1111 10..
		l = 6
		b[0] &= 0x3
	case (b[0] >> 1) == 0x7e: // 1111 110.
		l = 7
		b[0] &= 0x1
	case b[0] == 0xfe: // 1111 1110
		l = 8
	case b[0] == 0xff: // 1111 1111
		l = 9
	}

	bb := buffer.read(uint64(l - 1))

	if l < 8 {
		value = unpack_number(append(b, bb...), l)
	} else {
		value = unpack_number(bb, l-1)
	}

	return value
}

func DecodeFloat(buffer []byte) float64 {
	var b decodeBuffer

	b.init_buffer(buffer)
	return decodeFloat(&b)
}

func decodeFloat(buffer *decodeBuffer) float64 {
	var float_value float64
	var powerbyte []byte

	powerbyte = buffer.read(1)
	int_value := decodeNumber(buffer)

	float_value = float64(int_value) / math.Pow(10, float64(powerbyte[0]))
	return float_value
}

// 2B:   year. (-32767..32768)
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
func DecodeDate(buffer []byte) *time.Time {
	var b decodeBuffer

	b.init_buffer(buffer)
	return decodeDate(&b)
}

func decodeDate(buffer *decodeBuffer) *time.Time {
	var date time.Time
	var year int
	var month time.Month
	var day int
	var hour, min, sec, nsec, offh, offm int
	var loc *time.Location

	datebin := buffer.read(8)

	year = int(uint(datebin[0])<<8 | uint(datebin[1]))
	month = time.Month(uint(datebin[2]) >> 4)
	day = int(((uint(datebin[2]) & 0xf) << 1) | (uint(datebin[3]) >> 7))
	hour = int((uint(datebin[3]) & 0x7f) >> 2)
	min = int(((uint(datebin[3]) & 0x3) << 4) | (uint(datebin[4]) >> 4))
	sec = int(((uint(datebin[4]) & 0xf) << 2) | (uint(datebin[5]) >> 6))
	nsec = int(((uint(datebin[5])&0x3f)<<4)|(uint(datebin[6])>>4)) * 1000000
	offh = int(((uint(datebin[6]) & 0xf) << 2) | (uint(datebin[7]) >> 6))
	offm = int(uint(datebin[7]) & 0x3f)

	loc = time.FixedZone(" ", offh*3600+offm*60)
	date = time.Date(year, month, day, hour, min, sec, nsec, loc)

	return &date
}

func decodeFile(buffer *decodeBuffer, file *File) {
	var bin []byte

	file.f, _ = ioutil.TempFile(dir, "llsndecode_")
	file.tmp = file.f.Name()

	defer file.f.Close()
	n := uint64(65535) // 64K

	for {
		if n < file.length {
			bin = buffer.read(n)
			file.f.Write(bin)
			file.length -= n
		} else {
			bin = buffer.read(file.length)
			file.f.Write(bin)
			break
		}
	}
}

// Decode helpers //////////////////////////////////////////////////////////////

func unpack_number(buffer []byte, n uint8) uint64 {
	var v uint64

	for i := uint8(0); i < n; i++ {
		v |= uint64(buffer[i]) << ((n - (i + 1)) * 8)
	}
	return v
}

type decodeBuffer struct {
	buffer  []byte
	channel chan []byte
	read    func(uint64) []byte
	look    func(uint64) []byte
}

func (b *decodeBuffer) init_chan(channel chan []byte) {
	b.channel = channel
	b.read = b.read_chan
	b.look = b.look_chan
}

func (b *decodeBuffer) init_buffer(buffer []byte) {
	b.buffer = buffer
	b.read = b.read_buffer
	b.look = b.look_buffer
}

func (b *decodeBuffer) waitdata() {
	select {
	case buffer, ok := <-b.channel:
		if ok {
			b.buffer = append(b.buffer, buffer...)
		} else {
			panic("Channel was closed")
		}

	case <-time.After(1 * time.Minute):
		panic("Read channel timeout")
	}
}

func (b *decodeBuffer) read_chan(n uint64) []byte {

	for {
		if len(b.buffer) >= int(n) {
			return b.read_buffer(n)
		}

		b.waitdata()
	}
}

func (b *decodeBuffer) look_chan(n uint64) []byte {

	for {
		if len(b.buffer) >= int(n) {
			return b.look_buffer(n)
		}

		b.waitdata()
	}
}

func (b *decodeBuffer) read_buffer(n uint64) []byte {
	buff := b.buffer[:n]
	b.buffer = b.buffer[n:]
	return buff
}

func (b *decodeBuffer) look_buffer(n uint64) []byte {
	return b.buffer[:n]
}
