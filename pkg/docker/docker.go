package docker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"go.ytsaurus.tech/library/go/core/log"
)

type DockerOpts struct {
	Volumes       map[string]string
	Mounts        []mount.Mount
	LogDriver     string
	LogOptions    map[string]string
	Image         string
	Network       string
	ContainerName string
	Command       []string
	Env           []string
	Timeout       time.Duration
	AutoRemove    bool
	AttachStdout  bool
	AttachStderr  bool
}

type DockerWrapper struct {
	cli    DockerClient
	logger log.Logger
}

func NewDockerWrapper(logger log.Logger, cli ...DockerClient) (*DockerWrapper, error) {
	dw := &DockerWrapper{
		logger: logger,
	}

	if len(cli) > 0 {
		dw.cli = cli[0]
	}

	if err := dw.ensureDocker(os.Getenv("SUPERVISORD_PATH"), 30*time.Second); err != nil {
		return nil, err
	}

	return dw, nil
}

func (dw *DockerWrapper) isDockerReady() bool {
	if dw.cli == nil {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_, err := dw.cli.Ping(ctx)
	if err != nil {
		dw.logger.Warnf("Docker is not ready: %v", err)
		return false
	}
	dw.logger.Infof("Docker is ready")
	return true
}

func (dw *DockerWrapper) Pull(ctx context.Context, image string, opts types.ImagePullOptions) error {
	_, _, err := dw.cli.ImageInspectWithRaw(ctx, image)
	if client.IsErrNotFound(err) {
		reader, err := dw.cli.ImagePull(ctx, image, types.ImagePullOptions{})
		if err != nil {
			return err
		}
		defer reader.Close()
	} else if err != nil {
		return err
	}

	return nil
}

func (dw *DockerWrapper) Run(ctx context.Context, opts DockerOpts) (stdout io.Reader, stderr io.Reader, err error) {
	if dw.cli == nil {
		return nil, nil, xerrors.Errorf("docker unavailable")
	}

	if err := dw.Pull(ctx, opts.Image, types.ImagePullOptions{}); err != nil {
		return nil, nil, err
	}

	var mountsList []mount.Mount
	for hostPath, containerPath := range opts.Volumes {
		mountsList = append(mountsList, mount.Mount{
			Type:   mount.TypeBind,
			Source: hostPath,
			Target: containerPath,
		})
	}
	mountsList = append(mountsList, opts.Mounts...)

	containerConfig := &container.Config{
		Image:  opts.Image,
		Cmd:    opts.Command,
		Env:    opts.Env,
		Labels: opts.LogOptions,
		Tty:    false,
	}

	hostConfig := &container.HostConfig{
		Mounts:     mountsList,
		AutoRemove: opts.AutoRemove,
		LogConfig:  container.LogConfig{Type: opts.LogDriver, Config: opts.LogOptions},
	}

	networkingConfig := &network.NetworkingConfig{}
	if opts.Network != "" {
		networkingConfig.EndpointsConfig = map[string]*network.EndpointSettings{
			opts.Network: {},
		}
	}

	resp, err := dw.cli.ContainerCreate(ctx, containerConfig, hostConfig, networkingConfig, nil, opts.ContainerName)
	if err != nil {
		return nil, nil, err
	}

	if err := dw.cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return nil, nil, err
	}

	attachOptions := container.AttachOptions{
		Stream: true,
		Stdout: opts.AttachStdout,
		Stderr: opts.AttachStderr,
	}

	attachResp, err := dw.cli.ContainerAttach(ctx, resp.ID, attachOptions)
	if err != nil {
		return nil, nil, err
	}

	// TODO: don't hold stdoutBuf and stderrBuf in memory
	stdoutBuf := new(bytes.Buffer)
	stderrBuf := new(bytes.Buffer)

	copyCh := make(chan struct{})

	go func() {
		defer close(copyCh)
		if attachResp.Conn != nil {
			defer attachResp.Close()
		}
		stdcopy.StdCopy(stdoutBuf, stderrBuf, attachResp.Reader)
	}()

	waitCh, errCh := dw.cli.ContainerWait(ctx, resp.ID, container.WaitConditionNextExit)

	select {
	case err := <-errCh:
		if err != nil {
			return nil, nil, err
		}
	case <-ctx.Done():
		dw.cli.ContainerKill(ctx, resp.ID, "SIGKILL")
		return nil, nil, ctx.Err()
	}

	<-waitCh
	<-copyCh

	return stdoutBuf, stderrBuf, nil
}

func (dw *DockerWrapper) ensureDocker(supervisorConfigPath string, timeout time.Duration) error {
	if supervisorConfigPath == "" {
		// no supervisor, assume docker is already running.
		if !dw.isDockerReady() {
			return xerrors.New("docker is not ready")
		}
		return nil
	}
	// Command to start supervisord
	st := time.Now()
	var stdoutBuf, stderrBuf bytes.Buffer

	// Ensure config path is valid to prevent command injection
	if _, err := os.Stat(supervisorConfigPath); os.IsNotExist(err) {
		return xerrors.Errorf("supervisord config file not found: %s", supervisorConfigPath)
	} else if err != nil {
		return xerrors.Errorf("error checking supervisord config file: %w", err)
	}

	supervisorCmd := exec.Command("supervisord", "-n", "-c", supervisorConfigPath)
	supervisorCmd.Stdout = &stdoutBuf
	supervisorCmd.Stderr = &stderrBuf

	// Start supervisord in a separate goroutine
	errCh := make(chan error, 1)
	go func() {
		errCh <- supervisorCmd.Run()
		dw.logger.Infof("supervisord: output: \n%s", stdoutBuf.String())
		if stderrBuf.Len() > 0 {
			dw.logger.Warnf("supervidord: stderr: \n%s", stderrBuf.String())
		}
	}()

	// Wait for dockerd to be ready
	dockerReady := make(chan bool)
	go func() {
		for {
			if dw.cli == nil {
				cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
				if err != nil {
					continue
				}
				dw.cli = cli
			}

			if dw.isDockerReady() {
				close(dockerReady)
				return
			}
			time.Sleep(1 * time.Second)
		}
	}()

	select {
	case <-dockerReady:
		dw.logger.Infof("Docker is ready in %v!", time.Since(st))
		return nil
	case err := <-errCh:
		return xerrors.Errorf("supervisord exited unexpectedly: %w", err)
	case <-time.After(timeout):
		return xerrors.Errorf("timeout: %v waiting for Docker to be ready", timeout)
	}
}

func (opts *DockerOpts) String() string {
	var args []string

	// AutoRemove
	if opts.AutoRemove {
		args = append(args, "--rm")
	}

	// ContainerName
	if opts.ContainerName != "" {
		args = append(args, "--name", opts.ContainerName)
	}

	// Network
	if opts.Network != "" {
		args = append(args, "--network", opts.Network)
	}

	// Volumes (handled with -v)
	if len(opts.Volumes) > 0 {
		var volumeKeys []string
		for hostPath := range opts.Volumes {
			volumeKeys = append(volumeKeys, hostPath)
		}
		sort.Strings(volumeKeys)
		for _, hostPath := range volumeKeys {
			containerPath := opts.Volumes[hostPath]
			args = append(args, "-v", fmt.Sprintf("%s:%s", hostPath, containerPath))
		}
	}

	// Mounts (handled with --mount)
	if len(opts.Mounts) > 0 {
		sort.Slice(opts.Mounts, func(i, j int) bool {
			ikey := fmt.Sprintf("%s/%s/%s", opts.Mounts[i].Type, opts.Mounts[i].Source, opts.Mounts[i].Target)
			jkey := fmt.Sprintf("%s/%s/%s", opts.Mounts[j].Type, opts.Mounts[j].Source, opts.Mounts[j].Target)
			return ikey < jkey
		})
		for _, m := range opts.Mounts {
			mountOpts := []string{
				fmt.Sprintf("type=%s", m.Type),
				fmt.Sprintf("source=%s", m.Source),
				fmt.Sprintf("target=%s", m.Target),
			}
			if m.ReadOnly {
				mountOpts = append(mountOpts, "readonly")
			}

			args = append(args, "--mount", strings.Join(mountOpts, ","))
		}
	}

	// Environment Variables
	if len(opts.Env) > 0 {
		sort.Strings(opts.Env)
		for _, envVar := range opts.Env {
			args = append(args, "-e", envVar)
		}
	}

	// Log Driver and Options
	if opts.LogDriver != "" {
		args = append(args, "--log-driver", opts.LogDriver)
		if len(opts.LogOptions) > 0 {
			var logOptKeys []string
			for key := range opts.LogOptions {
				logOptKeys = append(logOptKeys, key)
			}
			sort.Strings(logOptKeys)
			for _, key := range logOptKeys {
				value := opts.LogOptions[key]
				args = append(args, "--log-opt", fmt.Sprintf("%s=%s", key, value))
			}
		}
	}

	// Attach options
	var attachOptions []string
	if opts.AttachStderr {
		attachOptions = append(attachOptions, "stderr")
	}
	if opts.AttachStdout {
		attachOptions = append(attachOptions, "stdout")
	}
	if len(attachOptions) > 0 {
		sort.Strings(attachOptions)
		for _, attach := range attachOptions {
			args = append(args, "--attach", attach)
		}
	}

	// Image
	if opts.Image != "" {
		args = append(args, opts.Image)
	}

	// Command
	if len(opts.Command) > 0 {
		args = append(args, opts.Command...)
	}

	cmd := append([]string{"docker", "run"}, args...)

	return strings.Join(cmd, " ")
}
