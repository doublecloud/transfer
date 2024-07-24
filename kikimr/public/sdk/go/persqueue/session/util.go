package session

import "fmt"

func formatEndpoint(endpoint string, port int) string {
	if len(endpoint) == 0 {
		return ""
	}

	hasPort := false
	for i := len(endpoint) - 1; i >= 0; i-- {
		if endpoint[i] == ':' {
			hasPort = true
			break
		}
	}

	if hasPort {
		return endpoint
	}
	return fmt.Sprintf("%s:%d", endpoint, port)
}
