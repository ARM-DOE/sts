package fileutil

import (
	"bytes"
	"fmt"
	"html/template"
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/arm-doe/sts"
)

// PathMap houses a single translation from a pattern to a string template used
// to generate the mapping from an input path to an output name
type PathMap struct {
	Pattern   *regexp.Regexp
	Template  string
	Funcs     template.FuncMap
	ExtraVars map[string]string
	Stat      func(string) time.Time
}

// Translate takes an input path and generates an output name if it matches the
// configured pattern
func (pm *PathMap) Translate(
	inputPath string,
) (outputName string, err error) {
	matches := pm.Pattern.FindStringSubmatch(inputPath)
	if matches == nil {
		err = fmt.Errorf(
			"path \"%s\" does not match: %s",
			inputPath,
			pm.Pattern.String(),
		)
		return
	}
	vars := make(map[string]interface{})
	if strings.Contains(pm.Template, ".__modtime") {
		if pm.Stat == nil {
			pm.Stat = func(path string) time.Time {
				info, err := os.Stat(path)
				if err != nil {
					return time.Time{}
				}
				return info.ModTime()
			}
		}
		vars["__modtime"] = pm.Stat(inputPath)
	}
	vars["__leaf"] = path.Base(inputPath)
	for key, value := range pm.ExtraVars {
		vars[key] = value
	}
	for i, matchName := range pm.Pattern.SubexpNames()[1:] {
		vars[matchName] = matches[i+1]
	}
	template, err := template.New(inputPath).Funcs(pm.Funcs).Parse(pm.Template)
	if err != nil {
		return
	}
	var b bytes.Buffer
	if err = template.Execute(&b, vars); err != nil {
		return
	}
	outputName = b.String()
	return
}

// PathMapper houses an ordered list of translations for one path to another
type PathMapper struct {
	Maps   []*PathMap
	Logger sts.Logger
}

// Translate takes an input path and returns the first valid output name
// generated by one of the configured mappings
func (pm *PathMapper) Translate(inputPath string) string {
	for _, m := range pm.Maps {
		if outputName, err := m.Translate(inputPath); err == nil {
			return outputName
		}
	}
	return ""
}
