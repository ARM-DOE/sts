package sts

// InitPath will turn a relative path into absolute (based on root) and make sure it exists.
import (
	"os"
	"path/filepath"

	"code.arm.gov/dataflow/sts/logging"
)

func InitPath(root string, path string, isdir bool) string {
	var err error
	if !filepath.IsAbs(path) {
		path, err = filepath.Abs(filepath.Join(root, path))
		if err != nil {
			logging.Error("Failed to initialize: ", path, err.Error())
		}
	}
	pdir := path
	if !isdir {
		pdir = filepath.Dir(pdir)
	}
	_, err = os.Stat(pdir)
	if os.IsNotExist(err) {
		logging.Debug("MAIN Make Path:", pdir)
		os.MkdirAll(pdir, os.ModePerm)
	}
	return path
}
