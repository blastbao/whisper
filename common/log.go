package common

import (
	"fmt"
)

// log
type Logger struct {
}

var Log = &Logger{}

func (l *Logger) Debug(tag string, args ...interface{}) {
	c := GetConf()
	if c.Debug {
		fmt.Println(tag, args)
	}
}

func (l *Logger) Info(tag string, args ...interface{}) {
	fmt.Println(tag, args)
}

func (l *Logger) Warning(tag string, args ...interface{}) {
	fmt.Println(tag, args)
}

func (l *Logger) Error(tag string, args ...interface{}) {
	fmt.Println(tag, args)
}
