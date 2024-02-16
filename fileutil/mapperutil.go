package fileutil

import (
	"html/template"
	"strconv"

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
