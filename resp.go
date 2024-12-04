/// Package to serialize and deserialize RESP
package main

import (
	"fmt"
	"bufio"
	"strconv"
	"io"
)

/// Constant types to its sign
const (
	STRING 	= '+'
	ERROR 	= '-'
	INTEGER = ':'
	BULK 	= '$'
	ARRAY 	= '*'
)

/// Struct type for processing 
type Value struct {
	typ		string 
	str 	string 
	num		int 	
	bulk 	string 
	array   []Value 
}

type Resp struct {
	reader *bufio.Reader 
}

/// Use later to pass in reader from connection in main
func NewResp(rd io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(rd)}
}


/// Implement functions bounded to a Resp instance. Similar to method in a class Resp

func (r *Resp) readLine() (line []byte, n int, err error) {
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err 
		}

		line = append(line, b)
		n += 1
		if len(line) >= 2 && line[len(line) - 2] == '\r' {
			break 
		}
	}

	return line[: len(line) - 2], n, nil
}

func (r *Resp) readInteger() (x int, n int, err error) {
	line, n, err := r.readLine()
	if err != nil {
		return 0, 0, err 
	}

	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, n, err 
	}

	/// why need int here?
	return int(i64), n, nil
}

func (r *Resp) Read() (Value, error) {
	_type, err := r.reader.ReadByte()

	if err != nil {
		return Value{}, err 
	}

	switch _type {
		case ARRAY:
			return r.readArray()
		case BULK:
			return r.readBulk()
		default:
			fmt.Println("Unknown type of %v", string(_type))
			return Value{}, nil
	}
}

func (r *Resp) readArray() (Value, error) {
	v := Value{}
	v.typ = "array"

	/// get length of array 
	length, _, err := r.readInteger()
	if err != nil {
		return v, err 
	}

	/// loop through each line (element) of array and append to v.array
	v.array = make([]Value, length)

	for i := 0; i < length; i += 1 {
		val, err := r.Read()

		if err != nil {
			return v, err
		}

		v.array[i] = val 
	}

	return v, nil
}

func (r *Resp) readBulk() (Value, error) {
	v := Value{}
	v.typ = "bulk"

	bulkSize, _,  err := r.readInteger()
	if err != nil {
		return v, err 
	}

	bulk := make([]byte, bulkSize)
	r.reader.Read(bulk)
	v.bulk = string(bulk)
	
	/// Read the trailing CRLF
	r.readLine()

	return v, nil
}

/// WRITER 
/// Marshal Value object to byte
/// Marshalling: formatting and transfroming from one type to another for transmitting data
func (v Value) Marshal() []byte {
	switch v.typ {
	case "array":
		return v.marshalArray()
	case "bulk":
		return v.marshalBulk()
	case "string":
		return v.marshalString()
	case "null":
		return v.marshalNull()
	case "error":	
		return v.marshalError()
	default:
		return []byte{}
	}
}

func (v Value) marshalArray() []byte {
	var bytes []byte

	bytes = append(bytes, ARRAY)
	bytes = append(bytes, strconv.Itoa(len(v.array))...)
	bytes = append(bytes, '\r', '\n')

	for i := 0; i < len(v.array); i++ {
		bytes = append(bytes, v.array[i].Marshal()...)
	}

	return bytes
}

func (v Value) marshalBulk() []byte {
	var bytes []byte

	bytes = append(bytes, BULK)
	bytes = append(bytes, strconv.Itoa(len(v.bulk))...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, v.bulk...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (v Value) marshalString() []byte {
	var bytes []byte 
	bytes = append(bytes, STRING)
	bytes = append(bytes, v.str...) /// like spreading factor in JS
	bytes = append(bytes, '\r', '\n') /// a must else client RESP cannot understand

	return bytes
}

func (v Value) marshalNull() []byte {
	return []byte("$-1\r\n")
}

func (v Value) marshalError() []byte {
	var bytes []byte 
	bytes = append(bytes, ERROR)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

func (w *Writer) Write(v Value) error {
	var bytes = v.Marshal()

	_, err := w.writer.Write(bytes)
	if err != nil {
		return err 
	}

	return nil 
}


