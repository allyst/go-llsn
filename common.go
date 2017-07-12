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
	"io"
	"os"
	"reflect"
)

const (
	type_undefined = 0
	type_number    = 1
	type_float     = 2
	type_string    = 3
	type_blob      = 4
	type_file      = 5
	type_date      = 6
	type_bool      = 7
	type_struct    = 8
	type_array     = 9

	type_arrayn  = 10
	type_pointer = 11
	type_unumber = 12

	type_undefined_null = 255
	type_number_null    = 254
	type_float_null     = 253
	type_string_null    = 252
	type_blob_null      = 251
	type_file_null      = 250
	type_date_null      = 249
	type_bool_null      = 248
	type_struct_null    = 247

	type_array_null   = 246
	type_arrayn_null  = 245
	type_pointer_null = 244
	type_unumber_null = 243

	// huge data threshold (STRING, BLOB, FILE)
	// if set to 0 - tail encoding is disable
	// if set > 0 - data exeeds this value are placed to the end of binary packet
	DEFAULT_THRESHOLD = 0

	// max length for the types STRING/BLOB
	STRING_MAXBYTES = 10485760
	BLOB_MAXBYTES   = 4294967296

	// temporary folder for decoding files
	DECODE_FOLDER = "/tmp/"

	// version of encoder
	VERSION = 1
)

type File struct {
	Name   string
	tmp    string // temporary file name
	f      *os.File
	length uint64
}

func (f *File) SaveTo(path string) error {
	if err := os.Rename(f.tmp, path+f.Name); err != nil {
		// invalid cross-device link?

		src, err := os.Open(f.tmp)
		if err != nil {
			return err
		}
		defer src.Close()

		dst, err := os.Create(path + f.Name)
		if err != nil {
			return err
		}
		defer dst.Close()

		_, err = io.Copy(dst, src)
		if err != nil {
			return err
		}

		os.Remove(f.tmp)
		return nil
	}

	return nil
}

// we have declared custom type Blob because of 'reflect'
// reflects '[]byte' as a '[]uint8' but we need the strict naming
type Blob []byte

type typesTree struct {
	ttype int    // encode type
	n     uint64 // number of fields of struct

	parent *typesTree
	child  *typesTree
	prev   *typesTree
	next   *typesTree
}

type stackElement struct {
	parent    *stackElement
	i         uint64
	n         uint64
	value     reflect.Value
	index     func(int) reflect.Value
	nullflags []byte
}

func (t *typesTree) append(previous_type int) *typesTree {
	t.ttype = previous_type
	if t.next == nil {
		t.next = &typesTree{type_undefined, 0, t.parent, nil, t, nil}
	}
	return t.next
}

func (t *typesTree) addchild(parent_type int) *typesTree {
	t.ttype = parent_type
	t.child = &typesTree{type_undefined, 0, t, nil, nil, nil}
	t.append(t.ttype) // just add 'next' item
	return t.child
}

type tailElement struct {
	next   *tailElement
	value  reflect.Value // destination
	length uint64        // len of tailed data
}

func (t_current *tailElement) append(v reflect.Value, l uint64) *tailElement {
	n := &tailElement{nil, v, l}
	t_current.next = n
	return n
}

const (
	valuePARTED  = true
	valueDECODED = false
)

type decodeOpts struct {
	threshold  uint16
	stack      *stackElement
	tt         *typesTree
	tail       *tailElement
	tail_first *tailElement
}

var errorLLSNlist = map[int]string{
	100: "blabla blablabla",
}

type ErrorLLSN struct {
	code int
}

func (e *ErrorLLSN) Error() string {
	return errorLLSNlist[e.code]
}

func (e *ErrorLLSN) Code() int {
	return e.code
}

func oops(code int) {
	panic(&ErrorLLSN{code})
}
