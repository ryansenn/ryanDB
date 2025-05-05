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
	file   *os.File
	offset []int64
}

func newLogger(id string) *Logger {
	os.MkdirAll("logs", 0755)
	path := "logs/" + id + ".log"
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		log.Fatal(err)
	}

	// Temporary clear file
	err = f.Truncate(0)
	if err != nil {
		log.Fatal(err)
	}
	f.Seek(0, io.SeekStart)

	return &Logger{file: f}
}

func encodeLogEntry(entry *LogEntry) []byte {
	data, err := json.Marshal(entry)

	if err != nil {
		log.Fatal(err)
	}

	data = append(data, '\n')

	var buf bytes.Buffer
	size := uint32(len(data))
	binary.Write(&buf, binary.LittleEndian, size)
	buf.Write(data)
	final := buf.Bytes()

	return final
}

func (l *Logger) AppendLog(entry *LogEntry) {
	data := encodeLogEntry(entry)

	pos, err := l.file.Seek(0, io.SeekEnd)

	if err != nil {
		log.Fatal(err)
	}

	if _, err := l.file.Write(data); err != nil {
		log.Fatal(err)
	}

	l.offset = append(l.offset, pos)
}

func (l *Logger) AppendLogs(entries []*LogEntry, start int64) {
	if start < int64(len(l.offset)) {
		err := l.file.Truncate(l.offset[start])
		if err != nil {
			log.Fatal(err)
		}
	}

	for _, entry := range entries {
		l.AppendLog(entry)
	}
}
