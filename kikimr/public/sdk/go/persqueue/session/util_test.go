package session

import "testing"

func TestFormatEndpoint(t *testing.T) {
	var testData = []struct {
		testName string
		endpoint string
		port     int
		expected string
	}{
		{
			testName: "empty_endpoint",
			endpoint: "",
			port:     42,
			expected: "",
		},
		{
			testName: "endpoint_without_port",
			endpoint: "hostname",
			port:     42,
			expected: "hostname:42",
		},
		{
			testName: "endpoint_with_port",
			endpoint: "hostname:42",
			port:     0,
			expected: "hostname:42",
		},
	}

	for _, v := range testData {
		v := v
		t.Run(v.testName, func(t *testing.T) {
			got := formatEndpoint(v.endpoint, v.port)
			if got != v.expected {
				t.Errorf("got %q, expected %q", got, v.expected)
			}
		})
	}
}
