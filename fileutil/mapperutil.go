package fileutil

import (
	"html/template"
	"strconv"
	"strings"

	"github.com/golang-module/carbon/v2"
)

func CreateDateFuncs() template.FuncMap {
	return template.FuncMap{
		"parseDate": func(s string) carbon.Carbon {
			return carbon.Parse(s)
		},
		"parseDayOfYear": func(year, day string) carbon.Carbon {
			d, _ := strconv.Atoi(day)
			return carbon.Parse(year).AddDays(d - 1)
		},
		"parseJulianDate": func(julian string) carbon.Carbon {
			j, _ := strconv.ParseFloat(julian, 64)
			return carbon.CreateFromJulian(j)
		},
		"addDuration": func(duration string, c carbon.Carbon) carbon.Carbon {
			return c.AddDuration(duration)
		},
		"formatDate": func(format string, c carbon.Carbon) string {
			return c.Format(format)
		},
	}
}

func CreateStringFuncs() template.FuncMap {
	return template.FuncMap{
		"toUpper": func(s string) string {
			return strings.ToUpper(s)
		},
		"toLower": func(s string) string {
			return strings.ToLower(s)
		},
	}
}

func CombineFuncs(
	funcs ...template.FuncMap,
) template.FuncMap {
	combined := make(template.FuncMap)
	for _, f := range funcs {
		for k, v := range f {
			combined[k] = v
		}
	}
	return combined
}
