package core

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"log"
	"os"
	"sync"
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

type MetaData struct {
	Term     int64
	VotedFor string
}

func NewCommand(op string, key string, value string) *Command {
	return &Command{Op: op, Key: key, Value: value}
}

func NewLogEntry(term int64, command *Command) *LogEntry {
	return &LogEntry{Term: term, Command: *command}
}

type Logger struct {
	Id       string
	logFile  *os.File
	metaFile *os.File
	offset   []int64
	metaMu   sync.Mutex
}

func newLogger(id string) *Logger {
	os.MkdirAll("logs", 0755)
	path := "logs/" + id + ".rlog"
	logs, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		log.Fatalf("wow1 %s %s", id, err)
	}

	path = "logs/" + id + ".meta"
	meta, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		log.Fatal(err)
	}

	return &Logger{Id: id, logFile: logs, metaFile: meta}
}

func (l *Logger) ClearData() {
	err := l.logFile.Truncate(0)
	if err != nil {
		log.Fatal(err)
	}
	l.logFile.Seek(0, io.SeekStart)

	l.metaMu.Lock()
	defer l.metaMu.Unlock()

	err = l.metaFile.Truncate(0)
	if err != nil {
		log.Fatalf("%s %s", l.Id, err)
	}
	l.metaFile.Seek(0, io.SeekStart)
}

func (l *Logger) WriteTerm(term int64) {
	l.metaMu.Lock()
	defer l.metaMu.Unlock()

	var metaData MetaData
	decoder := json.NewDecoder(l.metaFile)
	err := decoder.Decode(&metaData)

	if err != nil {
		if err != io.EOF {
			log.Fatalf("%s %s", l.Id, err)
		}
		metaData = MetaData{}
	}

	metaData.Term = term

	l.metaFile.Truncate(0)
	l.metaFile.Seek(0, io.SeekStart)
	json.NewEncoder(l.metaFile).Encode(metaData)
}

func (l *Logger) WriteVotedFor(votedFor string) {
	l.metaMu.Lock()
	defer l.metaMu.Unlock()
	l.metaFile.Seek(0, io.SeekStart)

	var metaData MetaData
	decoder := json.NewDecoder(l.metaFile)
	err := decoder.Decode(&metaData)

	if err != nil {
		if err != io.EOF {
			log.Fatalf("%s %s", l.Id, err)
		}
		metaData = MetaData{}
	}

	metaData.VotedFor = votedFor

	l.metaFile.Truncate(0)
	l.metaFile.Seek(0, io.SeekStart)
	json.NewEncoder(l.metaFile).Encode(metaData)
}

func (l *Logger) AppendLog(entry *LogEntry) {
	data := encodeLogEntry(entry)

	pos, err := l.logFile.Seek(0, io.SeekEnd)

	if err != nil {
		log.Fatalf("%s %s", l.Id, err)
	}

	if _, err := l.logFile.Write(data); err != nil {
		log.Fatalf("%s %s", l.Id, err)
	}

	l.offset = append(l.offset, pos)
	l.logFile.Sync()
}

func (l *Logger) AppendLogs(entries []*LogEntry, start int64) {
	if start < int64(len(l.offset)) {
		err := l.logFile.Truncate(l.offset[start])
		if err != nil {
			log.Fatalf("%s %s", l.Id, err)
		}
	}

	for _, entry := range entries {
		l.AppendLog(entry)
	}
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

func (l *Logger) LoadMeta() (int64, string) {
	l.metaMu.Lock()
	defer l.metaMu.Unlock()

	var metaData MetaData
	decoder := json.NewDecoder(l.metaFile)
	err := decoder.Decode(&metaData)

	if err != nil {
		if err != io.EOF {
			log.Fatalf("%s %s", l.Id, err)
		}
		metaData = MetaData{}
	}

	return metaData.Term, metaData.VotedFor
}

func (l *Logger) BuildOffsetTable() {
	l.logFile.Seek(0, io.SeekStart)

	i := 0
	offset := int64(0)
	var buf [4]byte
	n, err := l.logFile.Read(buf[:])

	if n != 4 {
		return
	}

	for err == nil {
		l.offset[i] = offset
		i += 1
		offset += int64(binary.LittleEndian.Uint32(buf[:]))
		_, err = l.logFile.Read(buf[:])
	}

	l.offset[i] = offset
}

func (l *Logger) LoadLogs() []*LogEntry {

}
