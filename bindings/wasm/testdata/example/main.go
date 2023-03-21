package main

import (
	"os"
)

func main() {
	os.Stdout.WriteString("hello ")
	os.Stdout.WriteString(os.Args[1])
}
