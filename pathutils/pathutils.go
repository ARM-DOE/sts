package pathutils

import (
	"os"
	"path/filepath"
	"strings"
)

// Sep is the path separator.
const Sep = string(os.PathSeparator)

var gRoot string

// GetRoot returns the STS root.  It will use $STS_DATA and fall back to the directory
// of the executable plus "/sts".
func GetRoot() string {
	if gRoot == "" {
		gRoot = os.Getenv("STS_DATA")
		if gRoot == "" {
			var err error
			gRoot, err = filepath.Abs(filepath.Dir(os.Args[0]))
			if err != nil {
				gRoot = Sep + "sts"
			}
		}
	}
	return gRoot
}

// Join combines N path elements via the Sep string.
func Join(params ...string) string {
	return strings.Join(params, Sep)
}
