package main

import (
	"fmt"
	"os"
)

type logger bool

func (l logger) Logf(format string, a ...interface{}) {
	if !l {
		return
	}
	fmt.Fprintf(os.Stderr, format, a...)
	fmt.Fprintln(os.Stderr, "")
}
