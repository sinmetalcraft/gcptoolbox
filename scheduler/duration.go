package scheduler

import "time"

type Duration struct {
	t time.Duration
}

func (d *Duration) MarshalYAML() ([]byte, error) {
	if d == nil {
		return nil, nil
	}

	return []byte(d.t.String()), nil
}

func (d *Duration) UnmarshalYAML(b []byte) error {
	if b == nil {
		return nil
	}

	dur, err := time.ParseDuration(string(b))
	if err != nil {
		return err
	}
	d.t = dur
	return nil
}
