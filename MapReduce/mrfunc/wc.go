package mrfunc

//
// 实现了Map, Reduce函数, 可以在Linux下编译成Plugin
//
// go build -buildmode=plugin wc.go
//
// Output: wc.so
//

import (
	"mapreduce/mr"
	"strconv"
	"strings"
	"unicode"
)

func Map(filename string, contents string) []mr.KeyValue {

	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	words := strings.FieldsFunc(contents, ff)

	kva := []mr.KeyValue{}
	for _, w := range words {
		kv := mr.KeyValue{Key: w, Value: "1"}
		kva = append(kva, kv)
	}
	return kva
}


func Reduce(key string, values []string) string {
	return strconv.Itoa(len(values))
}
