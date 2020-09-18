package common

import (
	"fmt"
	"testing"
)

func TestGetProperties(t *testing.T) {
	if r, e := GetProperties("D:/tmp/a.txt"); e != nil {
		t.Fatal(e)
	} else {
		fmt.Println(r)
	}
}

func TestGetConf(t *testing.T) {
	c := GetConf()
	fmt.Println(c)
}
