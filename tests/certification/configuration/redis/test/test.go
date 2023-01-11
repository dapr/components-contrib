package main

import (
	"fmt"

	"github.com/stretchr/testify/assert"
)

type A struct {
	a int
}

type B struct {
	a int
}

func main() {
	var t assert.TestingT
	map1 := make([]interface{}, 0, 10)
	map2 := make([]interface{}, 0, 10)
	a1 := A{
		a: 1,
	}
	b1 := A{
		a: 1,
	}
	// subsubmap1 := &map[string]string{
	// 	"key1": "val1",
	// }
	// subsubmap2 := &map[string]string{
	// 	"key1": "val1",
	// }
	submap1 := map[string]interface{}{
		"key1": a1,
	}
	submap2 := map[string]interface{}{
		"key1": b1,
	}
	map1 = append(map1, submap1)
	map2 = append(map2, submap2)
	assert.Equal(t, map1, map2)

	atest := A{}
	fmt.Println(atest.a)
}
