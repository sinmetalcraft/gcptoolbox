package scheduler

import (
	"strings"
	"testing"
	"time"

	"github.com/goccy/go-yaml"
)

func TestDuration_MarshalYAML(t *testing.T) {
	type Hoge struct {
		Name     string
		Deadline *Duration `yaml:",omitempty"`
	}

	cases := []struct {
		name  string
		value *Hoge
		want  string
	}{
		{"zero value", &Hoge{Name: "zeroValue"}, "name: zeroValue"},
		{"3s", &Hoge{Name: "3sec", Deadline: &Duration{3 * time.Second}}, "name: 3sec\ndeadline: 3s"},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := yaml.Marshal(tt.value)
			if err != nil {
				t.Fatal(err)
			}
			if e, g := tt.want, strings.TrimSpace(string(got)); e != g {
				t.Errorf("want \"%s\" but got \"%s\"", e, g)
			}
		})
	}
}

func TestDuration_UnmarshalYAML(t *testing.T) {
	type Hoge struct {
		Deadline *Duration
	}
	v := &Hoge{}

	err := yaml.Unmarshal([]byte("deadline: 3s\n"), v)
	if err != nil {
		t.Fatal(err)
	}
	if e, g := 3*time.Second, v.Deadline.Dur; e != g {
		t.Errorf("want \"%v\" but got \"%v\"", e, g)
	}
}
