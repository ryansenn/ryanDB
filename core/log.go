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

func newCommand(op string, key string, value string) *Command {
	return &Command{Op: op, Key: key, Value: value}
}

type Logger struct {
	file *os.File
}

func newLogger(id string) *Logger {
	path := id + ".log"
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		log.Fatal(err)
	}

	return &Logger{file: f}
}

func (l *Logger) append(command *Command) error {
	data, err := json.Marshal(command)

	if err != nil {
		log.Fatal(err)
	}

	data = append(data, '\n')

	if _, err := l.file.Write(data); err != nil {
		return err
	}

	return l.file.Sync()
}
