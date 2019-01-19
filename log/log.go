package log

import (
	"os"
	"fmt"
)

var gVerbosity = 0
var gDebugLevel = 0

var gLogFp *os.File

func SetVerbosity(v int) {
	gVerbosity = v
}

func SetDebugLevel(d int) {
	gDebugLevel = d
}

func OpenLogFile(logFileName string) bool {
	var err error
	gLogFp, err = os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		gLogFp = nil
		Error("Cannot open log file: %v", err)
		return false
	}

	return true
}

func CloseLogFile() {
	gLogFp.Close()
	gLogFp = nil
}

func logMsg(f string, args ... interface{}) {
	fmt.Printf(f, args...)

	if gLogFp != nil {
		fmt.Fprintf(gLogFp, f, args...)
	}
}

func appendNewLine(s string) string {
	return s + "\n"
}

func Info(level int, f string, args ... interface{}) {
	if level <= gVerbosity {
		f = appendNewLine(f)
		logMsg(f, args...)
	}
}

func Debug(level int, f string, args ... interface{}) {
	if level <= gDebugLevel {
		f = appendNewLine("DEBUG: " + f)
		logMsg(f, args...)
	}
}

func Error(f string, args ... interface{}) {
	f = appendNewLine("ERROR: " + f)
	logMsg(f, args...)
}

func Fatal(f string, args ... interface{}) {
	f = appendNewLine("FATAL: " + f)
	logMsg(f, args...)
	panic(fmt.Sprintf(f, args...))
}

