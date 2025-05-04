package core

import (
	"encoding/json"
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
	file *os.File
}

func newLogger(id string) *Logger {
	os.MkdirAll("logs", 0755)
	path := "logs/" + id + ".log"
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		log.Fatal(err)
	}

	return &Logger{file: f}
}

func (l *Logger) append(entry *LogEntry) error {
	data, err := json.Marshal(entry)

	if err != nil {
		log.Fatal(err)
	}

	data = append(data, '\n')

	if _, err := l.file.Write(data); err != nil {
		return err
	}

	return l.file.Sync()
}
