package main

import (
	"nosql-db/pkg/util"
	"testing"
)

//BenchmarkInnerJoin takes an array of arrays of strings, inner-joins them,
//and returns the resulting array of strings
func BenchmarkInnerJoin(b *testing.B) {
	s := make([][]string, 3)
	s[0] = []string{"hey", "hi", "yo", "yep", "yop", "Yo"}
	s[1] = []string{"hey", "hi", "salut", "yep", "ciao"}
	s[2] = []string{"hey", "hi", "bonjour", "bonsoir", "au revoir"}
	//log.Println(util.InnerJoin(s))
	for n := 0; n < b.N; n++ {
		util.InnerJoin(s)
	}
}
