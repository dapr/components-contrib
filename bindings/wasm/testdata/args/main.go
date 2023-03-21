package main

import (
	"os"
	"strings"
)

func main() {
	os.Stdout.WriteString(strings.Join(os.Args, "\n"))
}
