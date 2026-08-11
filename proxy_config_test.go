package main

import (
	"reflect"
	"testing"
)

func TestParseTrustedProxies(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  []string
	}{
		{name: "empty is fail closed", value: "", want: []string{}},
		{name: "trims and drops empty entries", value: " 172.18.0.1, ,10.0.0.0/8 ", want: []string{"172.18.0.1", "10.0.0.0/8"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := parseTrustedProxies(test.value); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("parseTrustedProxies(%q) = %#v, want %#v", test.value, got, test.want)
			}
		})
	}
}
