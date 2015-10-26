/* Util contains utility functions that are needed by both the Sender and Receiver. */
package util

import (
    "fmt"
    "net"
    "os"
    "strings"
    "syscall"
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

// GetTimestampNS returns an int64 containing the current time since the epoch in nanoseconds.
func GetTimestampNS() int64 {
    now_time := time.Now()
    return now_time.UnixNano()
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

// PrintDebug is a wrapper over fmt.Println that can be used to write debug messages which can be
// easily found and removed later.
func PrintDebug(str string) {
    fmt.Println(str)
}

// Restart is called to restart the sts binary. No closing of resources is performed, so data could
// be lost if Restart() is called without proper preparation.
func Restart() {
    // Replace the currently running process with a new instance of the sts.
    syscall.Exec(os.Args[0], os.Args, os.Environ())
}

// GetHostname does a lookup on an IP address. If the IP has already been successfully looked up,
// a cached copy is returned. If the resulting hostname has a trailing dot, it is removed.
func GetHostname(ip string) string {
    if host_names == nil {
        // Host names map hasn't been initialized yet.
        host_names = make(map[string]string)
    }
    memoized_name, ip_is_in := host_names[ip]
    if ip_is_in {
        // Lookup has already been completed for this IP once, return the name we found before.
        return memoized_name
    }
    found_hosts, err := net.LookupAddr(ip)
    if err != nil || len(found_hosts) < 1 {
        // Could not complete lookup
        return ip
    }
    host_name := strings.TrimSuffix(found_hosts[0], ".")
    host_names[ip] = host_name
    return host_name
}
