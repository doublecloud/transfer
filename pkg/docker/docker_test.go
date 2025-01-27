package docker

import (
	"testing"
)

func TestDockerOptsString(t *testing.T) {
	testCases := []struct {
		name     string
		opts     DockerOpts
		expected string
	}{
		{
			name: "AutoRemoveTrue",
			opts: DockerOpts{
				AutoRemove: true,
				Image:      "alpine",
			},
			expected: "docker run --rm alpine",
		},
		{
			name: "WithContainerName",
			opts: DockerOpts{
				ContainerName: "my-container",
				Image:         "nginx",
			},
			expected: "docker run --name my-container nginx",
		},
		{
			name: "WithNetwork",
			opts: DockerOpts{
				Network: "my-network",
				Image:   "ubuntu",
			},
			expected: "docker run --network my-network ubuntu",
		},
		{
			name: "WithVolumes",
			opts: DockerOpts{
				Volumes: map[string]string{
					"/host/data": "/container/data",
				},
				Image: "busybox",
			},
			expected: "docker run -v /host/data:/container/data busybox",
		},
		{
			name: "WithEnvVariables",
			opts: DockerOpts{
				Env:   []string{"VAR1=value1", "VAR2=value2"},
				Image: "redis",
			},
			expected: "docker run -e VAR1=value1 -e VAR2=value2 redis",
		},
		{
			name: "WithLogDriverAndOptions",
			opts: DockerOpts{
				LogDriver:  "json-file",
				LogOptions: map[string]string{"max-size": "10m", "max-file": "3"},
				Image:      "mysql",
			},
			expected: "docker run --log-driver json-file --log-opt max-file=3 --log-opt max-size=10m mysql",
		},
		{
			name: "WithAttachOptions",
			opts: DockerOpts{
				AttachStdout: true,
				AttachStderr: true,
				Image:        "golang",
			},
			expected: "docker run --attach stderr --attach stdout golang",
		},
		{
			name: "WithCommand",
			opts: DockerOpts{
				Image:   "alpine",
				Command: []string{"echo", "Hello, World!"},
			},
			expected: "docker run alpine echo Hello, World!",
		},
		{
			name: "FullOptions",
			opts: DockerOpts{
				AutoRemove:    true,
				ContainerName: "full-container",
				Network:       "full-network",
				Volumes: map[string]string{
					"/host/vol1": "/container/vol1",
					"/host/vol2": "/container/vol2",
				},
				Env:          []string{"ENV1=val1", "ENV2=val2"},
				LogDriver:    "syslog",
				LogOptions:   map[string]string{"syslog-address": "tcp://192.168.0.42:123"},
				AttachStdout: true,
				AttachStderr: false,
				Image:        "full-image",
				Command:      []string{"bash", "-c", "echo Full Test"},
			},
			expected: "docker run --rm --name full-container --network full-network -v /host/vol1:/container/vol1 -v /host/vol2:/container/vol2 -e ENV1=val1 -e ENV2=val2 --log-driver syslog --log-opt syslog-address=tcp://192.168.0.42:123 --attach stdout full-image bash -c echo Full Test",
		},
	}

	for _, tc := range testCases {
		// Use t.Run for better test reporting
		t.Run(tc.name, func(t *testing.T) {
			result := tc.opts.String()
			if result != tc.expected {
				t.Errorf("Test %s failed:\nExpected: %q\nGot:      %q", tc.name, tc.expected, result)
			}
		})
	}
}
