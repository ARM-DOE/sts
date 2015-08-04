// util contains code that is needed by both the Sender and Receiver.
package util

import (
    "crypto/md5"
    "fmt"
)

// generateMD5 generates and returns an md5 string from an array of bytes.
func GenerateMD5(data []byte) string {
    new_hash := md5.New()
    new_hash.Write(data)
    return fmt.Sprintf("%x", new_hash.Sum(nil))
}
