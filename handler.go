package main 

import (
	"sync"
	// "fmt"
)

var Handlers = map[string]func([]Value) Value{
	"PING": ping,
	"GET": get,
	"SET": set,
	/// map of map of strings
	"HGET": hget, 
	"HSET": hset,
	"HGETALL": hgetall,
}

/// PING command
func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}

	return Value{typ: "string", str: args[0].bulk}
}

/// Set commands
var sets = map[string]string{}
/// we are handling requests concurrently, this avoid multiple threads editing at the same time
var setsMutex = sync.RWMutex{} 

func set(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "Invalid number of arguments for set command"}
	}

	key := args[0].bulk
	value := args[1].bulk 

	// fmt.Println(key, value)

	/// Lock before write and Unlock after
	setsMutex.Lock()
	sets[key] = value 
	setsMutex.Unlock()

	return Value{typ: "string", str: "OK"}
}

func get(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "Invalid number of arguments for get command"}
	}

	key := args[0].bulk 

	setsMutex.RLock()
	value, ok := sets[key]
	setsMutex.RUnlock()

	if !ok {
		return Value{typ: "error", str: "Key not found"}
	}

	return Value{typ: "string", str: value}
}

/// The H- family
var hsets = map[string]map[string]string{}
var hsetsMutex = sync.RWMutex{}

func hset(args []Value) Value {
	if len(args) != 3 {
		return Value{typ: "error", str: "Invalid number of arguments for hset command. Expected 3"}
	}

	hash := args[0].bulk 
	key := args[1].bulk 
	value := args[2].bulk 


	hsetsMutex.Lock()
	/// Check for existence of hash
	_, ok := hsets[hash]
	if !ok {
		hsets[hash] = map[string]string{}
	}

	hsets[hash][key] = value 
	hsetsMutex.Unlock()

	return Value{typ: "string", str: "OK"}
}

func hget(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "Invalid number of arguments for hget command"}
	}

	hash := args[0].bulk 
	key := args[1].bulk 

	hsetsMutex.RLock()
	value, ok := hsets[hash][key]
	hsetsMutex.RUnlock()
	
	if !ok {
		return Value{typ: "error", str: "Key not found"}
	}

	return Value{typ: "string", str: value}
}

func hgetall(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "HGETALL command takes 1 argument"}
	}

	hash := args[0].bulk
	hsetsMutex.RLock()
	_, ok := hsets[hash]
	hsetsMutex.RUnlock()

	if !ok {
		return Value{typ: "error", str: "Hash not found"}
	}
	var fields []Value 

	hsetsMutex.RLock()
	for k, v := range hsets[hash] {
		fields = append(fields, Value{typ: "bulk", bulk: k})
		fields = append(fields, Value{typ: "bulk", bulk: v})
	}

	hsetsMutex.RUnlock()

	return Value{typ: "array", array: fields}
}
