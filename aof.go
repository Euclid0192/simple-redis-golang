/// AOF for data persistence
/// Basically create log after each command
/// If server crashes, power down, etc. -> execute all commands in this file in-memory to reconstruct data
package main 

import (
	"os"
	"bufio"
	"time"
	"sync"
	"io"
	// "fmt"
)

type Aof struct {
	file 	*os.File 
	rd 		*bufio.Reader 
	mutex 	sync.Mutex
}

func NewAof(path string) (*Aof, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err 
	}

	aof := &Aof{
		file: f,
		rd: bufio.NewReader(f),
	}

	/// Goroutine to sync file to disk every 1 seconds
	go func() {
		for {
			aof.mutex.Lock()

			aof.file.Sync()

			aof.mutex.Unlock()

			time.Sleep(time.Second)
		}
	}()

	return aof, nil 
}

func (aof *Aof) Close() error {
	aof.mutex.Lock()

	defer aof.mutex.Unlock()

	return aof.file.Close()
}

func (aof *Aof) Write(value Value) error {
	aof.mutex.Lock()
	/// Wait until everything is done before release lock
	defer aof.mutex.Unlock()

	/// Write to file, Marshal to format data into RESP
	_, err := aof.file.Write(value.Marshal())
	if err != nil {
		return err 
	}

	return nil
}

func (aof *Aof) Read(callback func(value Value)) error {
	aof.mutex.Lock()
	defer aof.mutex.Unlock()

	aof.file.Seek(0, io.SeekStart)

	resp := NewResp(aof.file)
	for {
		value, err := resp.Read()
		// fmt.Println(value)
		/// NO error, use callback to do something
		if err == nil {
			callback(value)
			continue
		}
		
		if err == io.EOF {
			break
		}

		return err
	}

	return nil
}