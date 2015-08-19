/* Util contains utility functions that are needed by both the Sender and Receiver. */
package util

import (
    "time"
)

// Default block size for the sender and receiver to use when reading/writing files.
const DEFAULT_BLOCK_SIZE = 8192

// GetTimestamp returns an int64 containing the current time since the epoch in seconds.
func GetTimestamp() int64 {
    now_time := time.Now()
    return now_time.Unix()
}

// IsStringInArray takes an array of strings and a string value and returns a boolean.
// true if the value is equal to a value in the array, else false.
func IsStringInArray(array []string, value string) bool {
    for _, element := range array {
        if element == value {
            return true
        }
    }
    return false
}

// IsIntInArray takes an array of ints and an int value and returns a boolean.
// true if the value is equal to a value in the array, else false.
func IsIntInArray(array []int, value int) bool {
    for _, element := range array {
        if element == value {
            return true
        }
    }
    return false
}
