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
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"sync"
)

// global variables
var threshold uint16
var dir string
var version uint8

////////////////////////////////////////////////////////////////////////////////
// ENCODE routines
////////////////////////////////////////////////////////////////////////////////

// Encode method allows calling
//
// Encode(value)
// Encode(value, threshold)
//
// return nil in this case. all encoded data writes to the channel
// Encode(value, channel)
// Encode(value, channel, threshold)
func Encode(v interface{}, a ...interface{}) *bytes.Buffer {
	var value reflect.Value = reflect.ValueOf(v)
	var args []interface{} = a
	var channel chan []byte
	var buffer *bytes.Buffer
	var wg sync.WaitGroup

	switch len(args) {
	case 0:
		channel = make(chan []byte)
		buffer = new(bytes.Buffer)

	case 1:
		switch reflect.ValueOf(args[0]).Kind() {
		case reflect.Int:
			threshold = uint16(args[0].(int))
			channel = make(chan []byte)
			buffer = new(bytes.Buffer)

		case reflect.Chan:
			channel = args[0].(chan []byte)

		default:
			panic("wrong second argument")
		}

	case 2:
		channel = args[0].(chan []byte)
		threshold = uint16(args[1].(int))

	default:
		panic("wrong arguments")
	}

	if value.Kind() != reflect.Ptr || value.Elem().Kind() != reflect.Struct {
		panic("Incorrect type of the source (expect '*struct')")
	}
	value = value.Elem()

	// create internal channel handler
	if buffer != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				b, ok := <-channel
				if ok {
					buffer.Write(b)
				} else {
					break
				}
			}
		}()

	}

	// encode it
	encode_ext(value, channel, threshold)

	// we should wait channel's routines in case of using internal handler
	if buffer != nil {
		wg.Wait()
	}

	return buffer
}

func Decode(source interface{}, destination interface{}) (err error) {
	var value reflect.Value = reflect.ValueOf(destination)
	var buffer decodeBuffer

	defer func() {
		if r := recover(); r != nil {
			err = errors.New(fmt.Sprintf("Malformed data. (%s)", r))
		}
	}()

	if value.Kind() != reflect.Ptr || value.Elem().Kind() != reflect.Struct {
		return errors.New("Incorrect type of the destination (expect '*struct')")
	}

	value = value.Elem()

	switch v := source.(type) {
	case []byte:
		buffer.init_buffer(v)

	case chan []byte:
		buffer.init_chan(v)

	default:
		return errors.New("Incorrect type of the source (expect 'chan []byte' or '[]byte'")
	}

	decode_ext(&buffer, &value)
	return err
}

func init() {
	threshold = DEFAULT_THRESHOLD
	dir = DECODE_FOLDER
}

func SetOption(name string, v interface{}) {
	switch name {
	case "threshold":
		threshold = uint16(v.(int))
	case "dir":
		dir = v.(string)

	default:
		panic("unknown option")
	}
}
