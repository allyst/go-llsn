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
	var mutex = &sync.Mutex{}
	var done = sync.NewCond(mutex)

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

	if value.Kind() != reflect.Ptr && value.Elem().Kind() != reflect.Struct {
		panic("expect '*struct' type, but yours '" + value.Type().String() + "'")
	}
	value = value.Elem()

	// create internal channel handler
	if buffer != nil {
		go func() {

			mutex.Lock()
			for {
				b, ok := <-channel
				if ok {
					buffer.Write(b)
				} else {
					break
				}
			}
			done.Signal()
		}()

	}

	// encode it
	encode_ext(value, channel, threshold)

	// we should wait channel's routines in case of using internal handler
	if buffer != nil {
		done.Wait()
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

		if buffer.channel != nil {
			close(buffer.channel)
		}
	}()

	if value.Kind() != reflect.Ptr && value.Elem().Kind() != reflect.Struct {
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
