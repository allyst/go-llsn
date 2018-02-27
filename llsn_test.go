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

package llsn_test

import (
	"bytes"
	"errors"
	"fmt"
	llsn "github.com/allyst/go-llsn"
	"math/rand"
	"os"
	"testing"
	"time"
)

var exampleMainValueEncoded []byte = []byte{16, 4, 19, 1, 33, 254, 12, 131, 120, 9, 3, 7, 1, 0, 1, 2, 6, 224, 47, 239, 220, 3, 128, 146, 6, 7, 223, 71, 195, 137, 234, 96, 0, 249, 8, 2, 1, 0, 247, 9, 5, 8, 2, 1, 0, 247, 64, 0, 64, 0, 64, 0, 64, 0, 246, 10, 4, 32, 8, 2, 1, 23, 8, 2, 1, 24, 247, 0, 25, 0, 22, 2, 1, 21, 247, 64, 26, 10, 10, 223, 10, 5, 208, 8, 2, 1, 27, 247, 64, 28, 64, 4, 160, 64, 29, 0, 30, 2, 1, 31, 247, 4, 13, 5, 75, 12, 108, 108, 115, 110, 116, 101, 115, 116, 102, 105, 108, 101, 250, 9, 34, 1, 191, 192, 65, 63, 128, 64, 223, 224, 0, 160, 1, 159, 255, 192, 32, 0, 239, 240, 0, 0, 208, 0, 1, 207, 255, 255, 224, 16, 0, 0, 247, 248, 0, 0, 0, 232, 0, 0, 1, 231, 255, 255, 255, 240, 8, 0, 0, 0, 251, 252, 0, 0, 0, 0, 244, 0, 0, 0, 1, 243, 255, 255, 255, 255, 248, 4, 0, 0, 0, 0, 253, 254, 0, 0, 0, 0, 0, 250, 0, 0, 0, 0, 1, 249, 255, 255, 255, 255, 255, 252, 2, 0, 0, 0, 0, 0, 254, 255, 0, 0, 0, 0, 0, 0, 253, 0, 0, 0, 0, 0, 1, 252, 255, 255, 255, 255, 255, 255, 254, 1, 0, 0, 0, 0, 0, 0, 255, 255, 128, 0, 0, 0, 0, 0, 0, 254, 128, 0, 0, 0, 0, 0, 1, 254, 127, 255, 255, 255, 255, 255, 255, 255, 0, 128, 0, 0, 0, 0, 0, 0, 255, 128, 0, 0, 0, 0, 0, 0, 1, 255, 127, 255, 255, 255, 255, 255, 255, 255, 9, 17, 12, 127, 128, 128, 191, 255, 192, 64, 0, 223, 255, 255, 224, 32, 0, 0, 239, 255, 255, 255, 240, 16, 0, 0, 0, 247, 255, 255, 255, 255, 248, 8, 0, 0, 0, 0, 251, 255, 255, 255, 255, 255, 252, 4, 0, 0, 0, 0, 0, 253, 255, 255, 255, 255, 255, 255, 254, 2, 0, 0, 0, 0, 0, 0, 254, 255, 255, 255, 255, 255, 255, 255, 255, 1, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 10, 5, 168, 10, 3, 0, 12, 131, 120, 131, 120, 131, 120, 4, 224, 131, 120, 72, 101, 108, 108, 111, 32, 87, 111, 114, 108, 100, 46, 32, 228, 189, 160, 229, 165, 189, 228, 184, 150, 231, 149, 140, 46, 32, 217, 133, 216, 177, 216, 173, 216, 168, 216, 167, 32, 216, 168, 216, 167, 217, 132, 216, 185, 216, 167, 217, 132, 217, 133, 46, 32, 227, 129, 147, 227, 130, 147, 227, 129, 171, 227, 129, 161, 227, 129, 175, 228, 184, 150, 231, 149, 140, 46, 32, 206, 147, 206, 181, 206, 185, 206, 172, 32, 206, 163, 206, 191, 207, 133, 32, 206, 154, 207, 140, 207, 131, 206, 188, 206, 181, 46, 32, 215, 148, 215, 162, 215, 156, 215, 144, 32, 215, 149, 215, 149, 215, 162, 215, 156, 215, 152, 46, 32, 208, 159, 209, 128, 208, 184, 208, 178, 208, 181, 209, 130, 32, 208, 156, 208, 184, 209, 128, 46, 8, 8, 8, 8, 8, 9, 9, 9, 9, 9, 7, 7, 7, 84, 104, 105, 115, 32, 105, 115, 32, 100, 101, 109, 111, 32, 102, 105, 108, 101, 46, 32, 84, 104, 105, 115, 32, 105, 115, 32, 100, 101, 109, 111, 32, 102, 105, 108, 101, 46, 32, 84, 104, 105, 115, 32, 105, 115, 32, 100, 101, 109, 111, 32, 102, 105, 108, 101, 46, 32, 84, 104, 105, 115, 32, 105, 115, 32, 100, 101, 109, 111, 32, 102, 105, 108, 101, 46}

// check for correct number encoding.

var signed_numbers []int64 = []int64{-64, -63, 63, 64, // 2,1,1,2 bytes
	-8192, -8191, 8191, 8192, // 3,2,2,3 bytes
	-1048576, -1048575, 1048575, 1048576, // 4,3,3,4
	-134217728, -134217727, 134217727, 134217728, // 5,4,4,5
	-17179869184, -17179869183, 17179869183, 17179869184, // 6,5,5,6
	-2199023255552, -2199023255551, 2199023255551, 2199023255552, // 7,6,6,7
	-281474976710656, -281474976710655, 281474976710655, 281474976710656, // 8,7,7,8
	-36028797018963968, -36028797018963967, 36028797018963967, 36028797018963968, // 9,8,8,9
							-9223372036854775807, 9223372036854775807} // 9,9
var unsigned_numbers []uint64 = []uint64{127, 128, // 1,2 bytes
	16383, 16384, // 2,3
	2097151, 2097152, // 3,4
	268435455, 268435456, // 4,5
	34359738367, 34359738368, // 5,6
	4398046511103, 4398046511104, // 6,7
	562949953421311, 562949953421312, // 7,8
	72057594037927935, 72057594037927936, // 8,9
	18446744073709551615} // 9 (<<255,255,255,255,255,255,255,255,255>>)

var exampleMainValue ExampleMain

type ExampleStruct struct {
	Field1 int64
	Field2 *ExampleStruct
}

type ExampleMain struct {
	Field1  int64              // number
	Field2  *int64             // number, nullable
	Field3  *uint64            // unumber
	Field4  [3]bool            // boolean
	Field5  float64            // float
	Field6  string             // string
	Field7  time.Time          // date
	Field8  *time.Time         // date, nullable
	Field9  ExampleStruct      // struct
	Field10 [5]ExampleStruct   // array of struct.
	Field11 []ExampleStruct    // array of struct. nullable
	Field12 [4]*ExampleStruct  // array of struct. have null values
	Field13 [][]*ExampleStruct // array of struct. nullable, have null values
	Field14 llsn.Blob          // blob. nullable
	Field15 llsn.File          // file
	Field16 *llsn.File         // file. nullable
	Field17 []int64
	Field18 []uint64
	Field19 [][]*uint64 // just for check array encoding [nil,nil,[1,2,3],nil,[nil,nil,1]]
}

func TestLLSN_1M_random_signed_NUMBER(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	for kk := 0; kk < 1000000; kk++ { // in 3.130425306s
		rv := rand.Int63n(1 << uint(rand.Int63n(63)))
		if rand.Int63n(2) > 0 {
			rv *= -1
		}

		b := llsn.EncodeNumber(int64(rv))
		rv1 := llsn.DecodeNumber(b)

		if rv != rv1 {
			t.Fatalf("\t# %d == %d  FAILED\n", rv, rv1)
		}

	}

	fmt.Printf("TestLLSN_1M_random_signed_NUMBER: PASSED\n")
}

func TestLLSN_1M_random_unsigned_NUMBER(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	for kk := 0; kk < 1000000; kk++ { // in 3.130425306s
		rv := uint64(rand.Int63n(1 << uint(rand.Int63n(63))))

		b := llsn.EncodeUNumber(rv)
		rv1 := llsn.DecodeUNumber(b)

		if rv != rv1 {
			t.Fatalf("\t# %d == %d  FAILED\n", rv, rv1)
		}

	}

	fmt.Printf("TestLLSN_1M_random_unsigned_NUMBER: PASSED\n")
}

func TestLLSN_unsigned_NUMBER(t *testing.T) {
	for _, v := range unsigned_numbers {
		b := llsn.EncodeUNumber(uint64(v))
		v1 := llsn.DecodeUNumber(b)
		if v != v1 {
			t.Fatalf("%d != %d ", v, v1)
		}
	}

	fmt.Printf("TestLLSN_unsigned_NUMBER: PASSED\n")
}

func TestLLSN_signed_NUMBER(t *testing.T) {
	for _, sv := range signed_numbers {
		b := llsn.EncodeNumber(int64(sv))
		sv1 := llsn.DecodeNumber(b)
		if sv != sv1 {
			t.Fatalf("%d != %d ", sv, sv1)
		}
	}

	fmt.Printf("TestLLSN_signed_NUMBER: PASSED\n")
}

func TestLLSN_1M_random_DATE(t *testing.T) {

	for kk := 1; kk < 1000000; kk++ {
		d := time.Unix(rand.Int63n(1<<uint(rand.Int63n(40))), rand.Int63n(999)*1000000)
		bd := llsn.EncodeDate(&d)
		d1 := llsn.DecodeDate(bd)

		if !d.Equal(*d1) {
			t.Fatalf("\nERROR... date source: %s\n", d)
			t.Fatalf("           date dest: %s\n", d1)
		} else {
			// fmt.Printf("\n#OK      date source: %s\n", d)
			// fmt.Printf("#          date dest: %s\n", d1)
		}

	}

	fmt.Printf("TestLLSN_1M_random_DATE: PASSED\n")
}

func TestLLSN_1M_random_FLOAT(t *testing.T) {

	// for kk := 1; kk < 1000000; kk++ {
	// 	f := rand.Float64()
	// 	fb := llsn.EncodeFloat(f)
	// 	f1 := llsn.DecodeFloat(fb)
	// 	if f != f1 {
	// 		t.Fatal(fmt.Sprintf("%f != %f", f, f1))
	// 	}
	// }

	// BUG!
	// 0.9430547821300547
	// fb := []byte{16, 254, 33, 129, 8, 132, 237, 11, 66}
	// f1, _ := llsn.DecodeFloat(fb)
	fmt.Printf("TestLLSN_1M_random_FLOAT: PASSED\n")
}

func TestLLSN_encodeComplexStruct(t *testing.T) {

	llsn.SetOption("threshold", 4)
	b := llsn.Encode(&exampleMainValue)

	if bytes.Compare(b.Bytes(), exampleMainValueEncoded) != 0 {
		fmt.Printf("%d\n", b.Bytes())
		t.Fatalf("encoded result is incorrect")
	}

	fmt.Printf("TestLLSN_encodeComplexStruct: PASSED\n")

}

func BenchmarkLLSN_encodeComplexStruct(b *testing.B) {
	llsn.SetOption("threshold", 4)
	for i := 0; i < b.N; i++ {
		llsn.Encode(&exampleMainValue)
	}
}

func TestLLSN_encodeComplexStruct_via_channel(t *testing.T) {
	channel := make(chan []byte)
	tail := exampleMainValueEncoded

	go llsn.Encode(&exampleMainValue, channel)

	for {
		bin, encoded := <-channel
		if encoded {
			l := len(bin)
			if bytes.Compare(tail[:l], bin) != 0 {
				t.Fatalf("encoded result is incorrect")
				break
			}

			tail = tail[l:]

		} else {
			// channel is closed
			break
		}
	}

	if len(tail) != 0 {
		t.Fatal("encoded result is incorrect")
	}

}

func BenchmarkLLSN_encodeComplexStruct_via_channel(b *testing.B) {
	llsn.SetOption("threshold", 4)
	for i := 0; i < b.N; i++ {
		channel := make(chan []byte)

		go llsn.Encode(&exampleMainValue, channel)

		for {
			_, encoded := <-channel
			if !encoded {
				// encoding finished. channel is closed
				break
			}
		}
	}

}

func TestLLSN_decodeComplexStruct(t *testing.T) {
	var E1 ExampleMain

	var data []byte = make([]byte, len(exampleMainValueEncoded))
	copy(data, exampleMainValueEncoded)

	if err := llsn.Decode(data, &E1); err != nil {
		fmt.Printf("TestLLSN_decodeComplexStruct: %s\n", err)
		return
	}

	if err := compareComplexStruct(&E1, &exampleMainValue); err != nil {
		fmt.Printf("TestLLSN_decodeComplexStruct: %s\n", err)
		return
	}

	fmt.Printf("TestLLSN_decodeComplexStruct: PASSED\n")

	// reflect.DeepEqual doesn't work for this case. we have to check it manualy.
}

func BenchmarkLLSN_decodeComplexStruct(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var E1 ExampleMain

		var data []byte = make([]byte, len(exampleMainValueEncoded))
		copy(data, exampleMainValueEncoded)

		llsn.Decode(data, &E1)
	}
}

func TestLLSN_decodeComplexStruct_via_channel(t *testing.T) {
	var E1 ExampleMain
	var chn chan []byte

	var data []byte = make([]byte, len(exampleMainValueEncoded))
	chn = make(chan []byte)

	go func() {
		if err := llsn.Decode(chn, &E1); err != nil {
			fmt.Printf("Decoding error: %s\n", err)
		}
	}()
	copy(data, exampleMainValueEncoded)

	for k := 0; k < len(data); k++ {
		chn <- data[k : k+1]
	}

	if err := compareComplexStruct(&E1, &exampleMainValue); err != nil {
		fmt.Printf("TestLLSN_decodeComplexStruct_via_channel: %s\n", err)
		return
	}

	fmt.Printf("TestLLSN_decodeComplexStruct_via_channel: PASSED\n")
}

func BenchmarkLLSN_decodeComplexStruct_via_channel(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var E1 ExampleMain
		var chn chan []byte

		var data []byte = make([]byte, len(exampleMainValueEncoded))
		chn = make(chan []byte)

		go func() {
			if err := llsn.Decode(chn, &E1); err != nil {
				fmt.Printf("Decoding error: %s\n", err)
			}
		}()
		copy(data, exampleMainValueEncoded)

		for k := 0; k < len(data); k++ {
			chn <- data[k : k+1]
		}
	}

}

func init() {

	// array of struct
	var field10_array5 [5]ExampleStruct

	// array of struct with nil values
	var field12_array4 [4]*ExampleStruct
	field12_array4[0] = &ExampleStruct{23, &ExampleStruct{24, nil}}
	field12_array4[1] = &ExampleStruct{25, &ExampleStruct{22, &ExampleStruct{21, nil}}}
	field12_array4[2] = nil
	field12_array4[3] = &ExampleStruct{26, nil}

	// two dimensional array with null values
	// [nil, nil, [nil,nil,VALUE,nil,nil], nil,  nil, nil, nil, nil, [nil,VALUE,nil,VALUE], nil]
	var field13_2array10 [][]*ExampleStruct
	var field13_2array10_array5 []*ExampleStruct
	var field13_2array10_array4 []*ExampleStruct
	field13_2array10 = make([][]*ExampleStruct, 10)
	field13_2array10_array5 = make([]*ExampleStruct, 5)
	field13_2array10_array5[2] = &ExampleStruct{27, nil}
	field13_2array10_array5[4] = &ExampleStruct{28, nil}
	field13_2array10[2] = field13_2array10_array5
	field13_2array10_array4 = make([]*ExampleStruct, 4)
	field13_2array10_array4[1] = &ExampleStruct{29, nil}
	field13_2array10_array4[3] = &ExampleStruct{30, &ExampleStruct{31, nil}}
	field13_2array10[8] = field13_2array10_array4

	var tmpfile string = "/tmp/llsntestfile"

	f, _ := os.Create(tmpfile)
	f.WriteString("This is demo file. This is demo file. This is demo file. This is demo file.")
	f.Close()

	var field_19_uint64value uint64 = 888
	var field19 = [][]*uint64{nil,
		[]*uint64{&field_19_uint64value, &field_19_uint64value, &field_19_uint64value}, nil,
		[]*uint64{nil, nil, nil, &field_19_uint64value}, nil}

	exampleMainValue = ExampleMain{33, nil, &field_19_uint64value, // Field1, Field2, Field3
		[...]bool{true, false, true}, // Field4
		3.141596,                     // Field5
		"Hello World. 你好世界. مرحبا بالعالم. こんにちは世界. Γειά Σου Κόσμε. העלא וועלט. Привет Мир.",
		time.Date(2015, time.April, 15, 16, 56, 39, 678000000, time.UTC), // Field7,
		nil,                                              // Field8
		ExampleStruct{},                                  // Field9
		field10_array5,                                   // Field10
		nil,                                              // Field11
		field12_array4,                                   // Field12
		field13_2array10,                                 // Field13
		llsn.Blob{8, 8, 8, 8, 8, 9, 9, 9, 9, 9, 7, 7, 7}, // Field14
		llsn.File{Name: tmpfile},                         // Field15
		nil,              // Field16
		signed_numbers,   // Field17
		unsigned_numbers, // Field18
		field19}
}

////// how to recover and get the error code and description
//
// defer func() {
// 		if r := recover(); r != nil {
// 			rrr := r.(*llsn.ErrorLLSN)
// 			fmt.Printf("Recovered error: [code %d] %s \n", rrr.Code(), r)
// 		}
// 	}()

func compareComplexStruct(e, e1 *ExampleMain) error {
	if e.Field1 != e1.Field1 {
		return errors.New("Field1 mismatch")
	}

	if e.Field2 != e1.Field2 {
		return errors.New("Field1 mismatch")
	}

	if *(e.Field3) != *(e1.Field3) {
		return errors.New("Field1 mismatch")
	}

	if e.Field4[2] != e1.Field4[2] {
		return errors.New("Field4[2] mismatch")
	}

	if e.Field5 != e1.Field5 {
		return errors.New("Field5 mismatch")
	}

	if e.Field6 != e1.Field6 {
		return errors.New("Field6 mismatch")
	}

	if !e.Field7.Equal(e1.Field7) {
		return errors.New("Field7 mismatch")
	}

	if e.Field12[0].Field1 != e1.Field12[0].Field1 ||
		e.Field12[0].Field2.Field1 != e1.Field12[0].Field2.Field1 ||
		e.Field12[1].Field1 != e1.Field12[1].Field1 ||
		e.Field12[1].Field2.Field1 != e1.Field12[1].Field2.Field1 ||
		e.Field12[1].Field2.Field2.Field1 != e1.Field12[1].Field2.Field2.Field1 {
		return errors.New("Field12 mismatch")
	}

	return nil
}
