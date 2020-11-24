package scheduler

import "time"

// Duration is yaml に time.Duration.String() の値を出力するための Wrapper
type Duration struct {
	Dur time.Duration
}

func (d *Duration) MarshalYAML() ([]byte, error) {
	if d == nil {
		return nil, nil
	}

	return []byte(d.Dur.String()), nil
}

func (d *Duration) UnmarshalYAML(b []byte) error {
	if b == nil {
		return nil
	}

	dur, err := time.ParseDuration(string(b))
	if err != nil {
		return err
	}
	d.Dur = dur
	return nil
}
