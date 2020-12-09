package common

import (
	"fmt"
)

// log
type Logger struct {
}

var Log = &Logger{}

func (this *Logger) Debug(tag string, args ...interface{}) {
	c := GetConf()
	if c.Debug {
		fmt.Println(tag, args)
	}
}

func (this *Logger) Info(tag string, args ...interface{}) {
	fmt.Println(tag, args)
}

func (this *Logger) Warning(tag string, args ...interface{}) {
	fmt.Println(tag, args)
}

func (this *Logger) Error(tag string, args ...interface{}) {
	fmt.Println(tag, args)
}
