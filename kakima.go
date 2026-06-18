package main

import (
	"fmt"
	"hash/fnv"
)

var (
	DefaultServerPrefix string = "SERVER"
)

// hashFNV1a uses the fnv-1a algorithm to
// deterministically hash a string
func hashFNV1a(s string) uint64 {
	hasher := fnv.New64a()
	hasher.Write([]byte(s))
	return hasher.Sum64()
}

// TODO: bugfix stops working after 27 #jugaad
func excelStyleAlphaFromInt(i int) string {
	var s string
	for i > 0 {
		i--
		rem := i % 26
		s = fmt.Sprintf("%c%s", rune('A'+rem), s)
		i /= 26
	}
	return s
}

func getServerString(serverId int, virtualCount int, collisionCount int) string {
	var s string = DefaultServerPrefix
	s = fmt.Sprintf("%s#%03d-%02d", s, serverId, virtualCount)
	if collisionCount == 0 {
		return s
	}
	return fmt.Sprintf(
		"%s-%s",
		s, excelStyleAlphaFromInt(collisionCount),
	)
}

func main() {
	fmt.Println(getServerString(4, 23, 52))
}
