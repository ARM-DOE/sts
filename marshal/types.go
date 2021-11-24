package marshal

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Duration wraps time.Duration for JSON marshaling
type Duration struct {
	time.Duration
}

// MarshalJSON implements Marshaler interface
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

// UnmarshalJSON implements Unmarshaler interface
func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		d.Duration = time.Duration(value)
		return nil
	case string:
		var err error
		d.Duration, err = time.ParseDuration(value)
		if err != nil {
			return err
		}
		return nil
	default:
		return errors.New("invalid duration")
	}
}

// UnmarshalYAML implements Unmarshaler interface
func (d *Duration) UnmarshalYAML(unmarshal func(interface{}) error) (err error) {
	err = unmarshal(&d.Duration)
	return
}

// NanoTime wraps time for marshaling time at the nanosecond level
type NanoTime struct {
	time.Time
}

func (t NanoTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(fmt.Sprintf("%d+%d", t.Unix(), t.Nanosecond()))
}

func (t *NanoTime) UnmarshalJSON(b []byte) error {
	var raw interface{}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	switch value := raw.(type) {
	// JSON numbers (even integers) are float64 by default
	case float64:
		// Support simple unix timestamp for backward compatibility
		t.Time = time.Unix(int64(value), 0)
	case string:
		parts := strings.Split(value, "+")
		if len(parts) != 2 {
			return fmt.Errorf("invalid NanoTime: %s", value)
		}
		var sec int64
		var nano int64
		var err error
		if sec, err = strconv.ParseInt(parts[0], 10, 64); err != nil {
			return err
		}
		if nano, err = strconv.ParseInt(parts[1], 10, 64); err != nil {
			return err
		}
		t.Time = time.Unix(sec, nano)
	default:
		return errors.New("invalid NanoTime")
	}
	return nil
}
