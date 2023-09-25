package fileutil

import (
	"fmt"
	"regexp"
	"testing"
	"time"
)

func TestBasicPathTranslate(t *testing.T) {
	runTest(
		t,
		&PathMap{
			Pattern:  regexp.MustCompile(`^/(?P<prefix>.+)/(?P<Y>\d{4})/(?P<M>\d{2})/(?P<D>\d{2})`),
			Template: "project/{{.source}}.{{.Y}}{{.M}}{{.D}}.000000.{{.prefix}}.{{.__leaf}}",
			ExtraVars: map[string]string{
				"source": "dataset",
			},
		},
		"/data/2019/04/19/status.txt",
		"project/dataset.20190419.000000.data.status.txt",
	)
	now := time.Now()
	runTest(
		t,
		&PathMap{
			Pattern:  regexp.MustCompile(`/just/a/basic/(?P<name>[^\.]+)`),
			Template: `project/{{.name}}.{{.__modtime.Format "20060102.150405"}}.txt`,
			Stat: func(path string) time.Time {
				return now
			},
		},
		"/just/a/basic/path.txt",
		fmt.Sprintf("project/path.%s.txt", now.Format("20060102.150405")),
	)
}

func runTest(t *testing.T, pm *PathMap, path, name string) {
	computedName, err := pm.Translate(path)
	if name == "" && err == nil {
		t.Errorf("Got \"%s\" from \"%s\" but expected no match", computedName, path)
	} else if name != "" && err != nil {
		t.Errorf("Got unexpected error (%s) from \"%s\"", err.Error(), path)
	} else if name != computedName {
		t.Errorf(
			"Got \"%s\" (from \"%s\") but expected \"%s\"",
			computedName,
			path,
			name,
		)
	}
}
