package main

import (
	"fmt"
	"net"
	"strings"
)

func main() {
	/// Start a TCP server so clients can connect
	l, err := net.Listen("tcp", ":6379")

	if err != nil {
		fmt.Println("Error while setting up tcp server: ", err)
		return 
	}

	aof, err := NewAof("db.aof")
	if err != nil {
		fmt.Println("Error while creating aof ", err)
		return 
	}
	defer aof.Close()

	aof.Read(func(value Value) {
		/// Extract command and args from file
		command := strings.ToUpper(value.array[0].bulk) /// why always bulk?
		args := value.array[1:]
		
		handler, ok := Handlers[command]

		if !ok {
			fmt.Println("Invalid command ", command)
			return 
		}

		handler(args)
	})

	fmt.Println("Listening on port 6379...")
	/// Start receiving requests
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error while accepting client requests: ", err)
		return 
	}
	/// Close connection when finished everything or error happens
	defer conn.Close()

	/// Loop for receiving and responding
	/// Inf loop
	for {
		resp := NewResp(conn)
		value, err := resp.Read()
		if err != nil {
			fmt.Println(err)
			return 
		}

		/// We should receive an array of RESP
		if value.typ != "array" {
			fmt.Println("Invalid request, expected array of RESP")
		}

		if len(value.array) == 0 {
			fmt.Println("Invalid request. Expected array length > 0")
		}


		/// Extract command and args from file
		command := strings.ToUpper(value.array[0].bulk) /// why always bulk?
		args := value.array[1:]

		/// Write back to client using Writer
		writer := NewWriter(conn)
		/// Handler object
		handler, ok := Handlers[command]

		/// Command not found
		if !ok {
			fmt.Println("Invalid command: ", command)
			writer.Write(Value{typ: "string", str: ""})
			continue
		}
		
		/// Only write if command changes data to save space
		if command == "SET" || command == "HSET" {
			aof.Write(value)
		}

		result := handler(args)
		writer.Write(result)
	}
}