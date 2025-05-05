package core

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"log"
	"os"
)

type Command struct {
	Op    string
	Key   string
	Value string
}

type LogEntry struct {
	Term    int64
	Command Command
}

func NewCommand(op string, key string, value string) *Command {
	return &Command{Op: op, Key: key, Value: value}
}

func NewLogEntry(term int64, command *Command) *LogEntry {
	return &LogEntry{Term: term, Command: *command}
}

type Logger struct {
	file    *os.File
	offsets []int64
}

func newLogger(id string) *Logger {
	os.MkdirAll("logs", 0755)
	path := "logs/" + id + ".log"
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		log.Fatal(err)
	}

	return &Logger{file: f}
}

func encoreLogEntry(entry *LogEntry) []byte {
	data, err := json.Marshal(entry)

	if err != nil {
		log.Fatal(err)
	}

	data = append(data, '\n')

	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, uint32(len(data)))
	buf.Write(data)
	final := buf.Bytes()

	return final
}

func (l *Logger) append(entry *LogEntry) error {

	data := encoreLogEntry(entry)

	l.file.Seek(0, io.SeekEnd)
	if _, err := l.file.Write(data); err != nil {
		return err
	}

	return l.file.Sync()
}
