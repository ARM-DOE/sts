package sts

import (
	"os"
	"path/filepath"
	"reflect"

	"code.arm.gov/dataflow/sts/logging"
)

// InitPath will turn a relative path into absolute (based on root) and make sure it exists.
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
		logging.Debug("UTIL Make Path:", pdir)
		os.MkdirAll(pdir, os.ModePerm)
	}
	return path
}

// CopyStruct copies the properties of one struct to another.
func CopyStruct(tgt interface{}, src interface{}) {
	v := reflect.ValueOf(tgt)
	z := reflect.ValueOf(src)
	if v.Kind() == reflect.Ptr {
		v = reflect.Indirect(v)
		z = reflect.Indirect(z)
	}
	if v.Kind() != reflect.Struct || v.Type() != z.Type() {
		return
	}
	for i := 0; i < v.NumField(); i++ {
		if IsZero(v.Field(i)) {
			v.Field(i).Set(reflect.ValueOf(z.Field(i).Interface()))
		}
	}
}

// IsZero identifies whether a variable is the empty value for its type.
func IsZero(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Func, reflect.Map, reflect.Slice:
		return v.IsNil()
	case reflect.Array:
		z := true
		for i := 0; i < v.Len(); i++ {
			z = z && IsZero(v.Index(i))
		}
		return z
	case reflect.Struct:
		z := true
		n := 0
		for i := 0; i < v.NumField(); i++ {
			if v.Field(i).CanSet() {
				z = z && IsZero(v.Field(i))
				n++
			}
		}
		if n > 0 {
			return z
		}
		return false
	case reflect.Ptr:
		if v.IsNil() {
			return true
		}
		return IsZero(reflect.Indirect(v))
	}
	// Compare other types directly:
	z := reflect.Zero(v.Type())
	return v.Interface() == z.Interface()
}
