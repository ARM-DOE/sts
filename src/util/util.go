/* Util contains utility functions that are needed by both the Sender and Receiver. */
package util

import (
    "net"
    "os"
    "strings"
    "time"
)

var host_names map[string]string

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

// PathJoin takes a list of params and joins them with os.PathSeparator
// to create a valid path on any OS.
func JoinPath(params ...string) string {
    return strings.Join(params, string(os.PathSeparator))
}

// GetHostname does a lookup on an IP address, if the IP has already been successfully looked up,
// a cached copy is returned. If the resulting hostname has a trailing dot, it is removed.
func GetHostname(ip string) string {
    if host_names == nil {
        // Host names map hasn't been initialized yet.
        host_names = make(map[string]string)
    }
    memoized_name, ip_is_in := host_names[ip]
    if ip_is_in {
        return memoized_name
    }
    found_hosts, err := net.LookupAddr(ip)
    host_name := strings.TrimSuffix(found_hosts[0], ".")
    if err != nil {
        // Could not complete lookup
        return ip
    } else {
        host_names[ip] = host_name
    }
    return host_name
}
