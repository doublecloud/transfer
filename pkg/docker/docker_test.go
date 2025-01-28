package docker

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/doublecloud/transfer/internal/logger"

	"github.com/docker/docker/api/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
		{
			name: "WithMountsOnly",
			opts: DockerOpts{
				Mounts: []mount.Mount{
					{
						Type:     mount.TypeBind,
						Source:   "/host/config",
						Target:   "/container/config",
						ReadOnly: true,
					},
					{
						Type:   mount.TypeTmpfs,
						Source: "",
						Target: "/container/tmp",
					},
				},
				Image: "nginx",
			},
			expected: "docker run --mount type=bind,source=/host/config,target=/container/config,readonly --mount type=tmpfs,source=,target=/container/tmp nginx",
		},
		{
			name: "WithVolumesAndMounts",
			opts: DockerOpts{
				Volumes: map[string]string{
					"/host/data": "/container/data",
				},
				Mounts: []mount.Mount{
					{
						Type:   mount.TypeVolume,
						Source: "myvolume",
						Target: "/container/volume",
					},
				},
				Image: "postgres",
			},
			expected: "docker run -v /host/data:/container/data --mount type=volume,source=myvolume,target=/container/volume postgres",
		},
		{
			name: "WithAttachStdoutOnly",
			opts: DockerOpts{
				AttachStdout: true,
				Image:        "ubuntu",
			},
			expected: "docker run --attach stdout ubuntu",
		},
		{
			name: "WithAttachStderrOnly",
			opts: DockerOpts{
				AttachStderr: true,
				Image:        "ubuntu",
			},
			expected: "docker run --attach stderr ubuntu",
		},
		{
			name:     "NoOptions",
			opts:     DockerOpts{},
			expected: "docker run",
		},
		{
			name: "WithMultipleLogOptions",
			opts: DockerOpts{
				LogDriver:  "fluentd",
				LogOptions: map[string]string{"fluentd-address": "localhost:24224", "tag": "docker.test"},
				Image:      "fluentd",
			},
			expected: "docker run --log-driver fluentd --log-opt fluentd-address=localhost:24224 --log-opt tag=docker.test fluentd",
		},
		{
			name: "WithMultipleEnvVariablesUnordered",
			opts: DockerOpts{
				Env:   []string{"Z_VAR=last", "A_VAR=first", "M_VAR=middle"},
				Image: "env-test",
			},
			expected: "docker run -e A_VAR=first -e M_VAR=middle -e Z_VAR=last env-test",
		},
		{
			name: "WithMultipleVolumesUnordered",
			opts: DockerOpts{
				Volumes: map[string]string{
					"/host/c": "/container/c",
					"/host/a": "/container/a",
					"/host/b": "/container/b",
				},
				Image: "vol-test",
			},
			expected: "docker run -v /host/a:/container/a -v /host/b:/container/b -v /host/c:/container/c vol-test",
		},
		{
			name: "WithMultipleMountsUnordered",
			opts: DockerOpts{
				Mounts: []mount.Mount{
					{
						Type:   mount.TypeTmpfs,
						Source: "",
						Target: "/container/tmp1",
					},
					{
						Type:   mount.TypeVolume,
						Source: "vol1",
						Target: "/container/vol1",
					},
					{
						Type:   mount.TypeBind,
						Source: "/host/data",
						Target: "/container/data",
					},
				},
				Image: "mount-test",
			},
			expected: "docker run --mount type=bind,source=/host/data,target=/container/data --mount type=tmpfs,source=,target=/container/tmp1 --mount type=volume,source=vol1,target=/container/vol1 mount-test",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.opts.String()
			if result != tc.expected {
				t.Errorf("Teste %s falhou:\nEsperado: %q\nObtido:   %q", tc.name, tc.expected, result)
			}
		})
	}
}

type testErrNotFound struct{}

func (t testErrNotFound) Error() string { return "" }
func (t testErrNotFound) NotFound()     {}

func TestDockerWrapper_Pull_Success(t *testing.T) {
	mockClient := new(MockDockerClient)
	logger := logger.Log

	mockClient.On("ImageInspectWithRaw", mock.Anything, "alpine").
		Return(types.ImageInspect{}, []byte{}, testErrNotFound{})

	mockClient.On("ImagePull", mock.Anything, "alpine", types.ImagePullOptions{}).
		Return(io.NopCloser(strings.NewReader("")), nil).Once()

	dw := &DockerWrapper{
		cli:    mockClient,
		logger: logger,
	}

	err := dw.Pull(context.Background(), "alpine", types.ImagePullOptions{})
	assert.NoError(t, err)

	mockClient.AssertExpectations(t)
}

func TestDockerWrapper_Pull_ImageAlreadyExists(t *testing.T) {
	mockClient := new(MockDockerClient)
	logger := logger.Log

	imageInspect := types.ImageInspect{}
	mockClient.On("ImageInspectWithRaw", mock.Anything, "nginx").
		Return(imageInspect, []byte{}, nil)

	mockClient.On("ImagePull", mock.Anything, "nginx", mock.Anything).Return(nil, nil).Maybe()

	dw := &DockerWrapper{
		cli:    mockClient,
		logger: logger,
	}

	err := dw.Pull(context.Background(), "nginx", types.ImagePullOptions{})
	assert.NoError(t, err)

	mockClient.AssertExpectations(t)
	mockClient.AssertNotCalled(t, "ImagePull", mock.Anything, mock.Anything, mock.Anything)
}

func TestDockerWrapper_Run_Success(t *testing.T) {
	mockClient := new(MockDockerClient)
	logger := logger.Log

	mockClient.On("ImageInspectWithRaw", mock.Anything, "alpine").
		Return(types.ImageInspect{}, []byte{}, testErrNotFound{})

	mockClient.On("ImagePull", mock.Anything, "alpine", types.ImagePullOptions{}).
		Return(io.NopCloser(strings.NewReader("")), nil).Once()

	containerCreateResponse := container.CreateResponse{ID: "container-id"}
	mockClient.On("ContainerCreate", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, "test-container").
		Return(containerCreateResponse, nil).Once()

	mockClient.On("ContainerStart", mock.Anything, "container-id", mock.Anything).
		Return(nil).Once()

	stdoutBuffer := bytes.NewBufferString("mock stdout\n")
	stderrBuffer := bytes.NewBufferString("mock stderr\n")

	hijackedResp := types.HijackedResponse{
		Reader: bufio.NewReader(io.MultiReader(stdoutBuffer, stderrBuffer)),
	}
	mockClient.On("ContainerAttach", mock.Anything, "container-id", mock.Anything).
		Return(hijackedResp, nil).Once()

	waitCh := make(chan container.WaitResponse, 1)
	waitCh <- container.WaitResponse{StatusCode: 0}
	close(waitCh)
	errCh := make(chan error)
	close(errCh)

	mockClient.On("ContainerWait", mock.Anything, "container-id", container.WaitConditionNextExit).
		Return((<-chan container.WaitResponse)(waitCh), (<-chan error)(errCh)).Once()

	dw := &DockerWrapper{
		cli:    mockClient,
		logger: logger,
	}

	opts := DockerOpts{
		Image:         "alpine",
		ContainerName: "test-container",
		Command:       []string{"echo", "Hello, World!"},
		AutoRemove:    true,
	}

	_, _, err := dw.Run(context.Background(), opts)

	assert.NoError(t, err)

	mockClient.AssertExpectations(t)
}
