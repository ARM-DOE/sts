// util contains code that is needed by both the Sender and Receiver.
package util

import (
    "crypto/md5"
    "fmt"
    "os"
)

// Both the sender and receiver use the same multipart boundary and bin size.
const MULTIPART_BOUNDARY = "83c64d3ae066414b27bfbff29a53ff1a500d5fac0a0799990c9c6464a1fb"

// generateMD5 generates and returns an md5 string from an array of bytes.
func GenerateMD5(data []byte) string {
    new_hash := md5.New()
    new_hash.Write(data)
    return fmt.Sprintf("%x", new_hash.Sum(nil))
}

// FileMD5 calculates the MD5 of a file given a path.
// Unline GenerateMD5, FileMD5 doesn't load the whole file into memory.
func FileMD5(path string) string {
    block_size := 4096
    fi, err := os.Open(path)
    if err != nil {
        panic("File " + path + " does not exist, can't generate MD5")
    }
    next_bytes := make([]byte, block_size)
    new_hash := md5.New()
    for {
        _, err := fi.Read(next_bytes)
        new_hash.Write(next_bytes)
        if err != nil {
            // file is done
            return fmt.Sprintf("%x", new_hash.Sum(nil))
        }
    }
}

// IsStringInArray takes an array of strings and a string value, and returns a boolean.
// true if the value is equal to a value in the array, else false.
func IsStringInArray(array []string, value string) bool {
    for _, element := range array {
        if element == value {
            return true
        }
    }
    return false
}

// IsIntInArray takes an array of ints and an int value, and returns a boolean.
// true if the value is equal to a value in the array, else false.
func IsIntInArray(array []int, value int) bool {
    for _, element := range array {
        if element == value {
            return true
        }
    }
    return false
}
