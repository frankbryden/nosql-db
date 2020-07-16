package util

import (
	"os"
)

//FileExists returns true if file exists, false if it doesn't or path is a directory.
func FileExists(filename string) bool {
	stat, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !stat.IsDir()
}
