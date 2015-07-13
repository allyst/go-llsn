# LLSN #

Go support for LLSN. Allyst's data interchange binary format

Format specification is available at http://allyst.org/opensource/llsn/


    int64              // number
    *int64             // number, nullable
    *uint64            // unumber, nullable
    [3]bool            // boolean
    float64            // float
    string             // string
    time.Time          // date
    *time.Time         // date, nullable
    ExampleStruct      // struct
    [5]ExampleStruct   // array of struct.
    []ExampleStruct    // array of struct. nullable
    [4]*ExampleStruct  // array of struct. have null values
    [][]*ExampleStruct // array of struct. nullable, have null values
    llsn.Blob          // blob. nullable. its a regular slice of bytes ([]byte), 
                       // but you have to use this type for correct encode/decode 
    llsn.File          // file
    *llsn.File         // file. nullable

Encode(value *struct) []byte
Encode(value *struct, threshold uint16) []byte

    example...

You can get encoded data via channel. Returns nil
Encode(value *struct, channel chan []byte) []byte
Encode(value *struct, channel chan []byte, threshold uint16) []byte
    example...



Decode(source []byte, destination *struct) error
Notice: source data will modify by decoder. you have to copy the original to reuse it elsewhere
    example
Decode(source chan []byte, destination *struct) error
    example


EncodeNumber(number int64) []byte // returns 1..9 bytes
EncodeUNumber(number uint64) []byte // returns 1..9 bytes
EncodeFloat(f float64) []byte // returns 4 or 8 bytes
EncodeDate(t *time.Time) []byte // return 8 bytes

DecodeFloat(buffer []byte) float64
DecodeNumber(buffer []byte) int64
DecodeUNumber(buffer []byte) uint64
DecodeDate(buffer []byte) *time.Time


llsn.SetOption(name string, v interface{})
    tail encoding threshold
    "threshold" int (0 - disabled, max - 4096). default: 0
    cache directory. uses for decoding files.
    "dir" string. default: "/tmp/"
