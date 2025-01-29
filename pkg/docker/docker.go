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

type DockerWrapper struct {
	cli    DockerClient
	logger log.Logger
}

func NewDockerWrapper(logger log.Logger) (*DockerWrapper, error) {
	d := &DockerWrapper{
		logger: logger,
	}

	if err := d.ensureDocker(os.Getenv("SUPERVISORD_PATH"), 30*time.Second); err != nil {
		return nil, err
	}

	return d, nil
}

func (d *DockerWrapper) isDockerReady() bool {
	if d.cli == nil {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_, err := d.cli.Ping(ctx)
	if err != nil {
		d.logger.Warnf("Docker is not ready: %v", err)
		return false
	}
	d.logger.Infof("Docker is ready")
	return true
}

func (d *DockerWrapper) Pull(ctx context.Context, image string, opts types.ImagePullOptions) error {
	_, _, err := d.cli.ImageInspectWithRaw(ctx, image)
	if client.IsErrNotFound(err) {
		reader, pullErr := d.cli.ImagePull(ctx, image, types.ImagePullOptions{})
		if pullErr != nil {
			return xerrors.Errorf("error pulling image %s: %w", image, pullErr)
		}
		defer reader.Close()
	} else if err != nil {
		return xerrors.Errorf("error inspecting image %s: %w", image, err)
	}

	return nil
}

func (d *DockerWrapper) Run(ctx context.Context, opts DockerOpts) (stdout io.Reader, stderr io.Reader, err error) {
	if d.cli == nil {
		return nil, nil, xerrors.Errorf("docker unavailable")
	}

	if err := d.Pull(ctx, opts.Image, types.ImagePullOptions{}); err != nil {
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

	resp, err := d.cli.ContainerCreate(ctx, containerConfig, hostConfig, networkingConfig, nil, opts.ContainerName)
	if err != nil {
		return nil, nil, xerrors.Errorf("error creating container: %w", err)
	}

	waitCh, errCh := d.cli.ContainerWait(ctx, resp.ID, container.WaitConditionNextExit)

	if err := d.cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return nil, nil, xerrors.Errorf("error starting container: %w", err)
	}

	attachOptions := container.AttachOptions{
		Stream: true,
		Stdout: opts.AttachStdout,
		Stderr: opts.AttachStderr,
	}

	attachResp, err := d.cli.ContainerAttach(ctx, resp.ID, attachOptions)
	if err != nil {
		return nil, nil, xerrors.Errorf("error attaching to container: %w", err)
	}

	// TODO: don't hold stdoutBuf and stderrBuf in memory
	stdoutBuf := new(bytes.Buffer)
	stderrBuf := new(bytes.Buffer)

	copyCh := make(chan error)

	go func() {
		defer close(copyCh)
		if attachResp.Conn != nil {
			defer attachResp.Close()
		}
		if _, err := stdcopy.StdCopy(stdoutBuf, stderrBuf, attachResp.Reader); err != nil {
			copyCh <- err
		}
	}()

	select {
	case err := <-errCh:
		if err != nil {
			return nil, nil, xerrors.Errorf("error waiting for container: %w", err)
		}
	case <-waitCh:
		select {
		case err := <-errCh:
			if err != nil {
				return nil, nil, xerrors.Errorf("error waiting for container: %w", err)
			}
		default:
		}
	case <-ctx.Done():
		d.cli.ContainerKill(ctx, resp.ID, "SIGKILL")
		return nil, nil, xerrors.Errorf("error waiting for container: %w", ctx.Err())
	}

	if err := <-copyCh; err != nil {
		return nil, nil, xerrors.Errorf("error copying container output: %w", err)
	}

	return stdoutBuf, stderrBuf, nil
}

func (d *DockerWrapper) ensureDocker(supervisorConfigPath string, timeout time.Duration) error {
	if supervisorConfigPath == "" {
		// no supervisor, assume docker is already running.
		if !d.isDockerReady() {
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
		d.logger.Infof("supervisord: output: \n%s", stdoutBuf.String())
		if stderrBuf.Len() > 0 {
			d.logger.Warnf("supervidord: stderr: \n%s", stderrBuf.String())
		}
	}()

	// Wait for dockerd to be ready
	dockerReady := make(chan bool)
	go func() {
		for {
			if d.cli == nil {
				cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
				if err != nil {
					continue
				}
				d.cli = cli
			}

			if d.isDockerReady() {
				close(dockerReady)
				return
			}
			time.Sleep(1 * time.Second)
		}
	}()

	select {
	case <-dockerReady:
		d.logger.Infof("Docker is ready in %v!", time.Since(st))
		return nil
	case err := <-errCh:
		return xerrors.Errorf("supervisord exited unexpectedly: %w", err)
	case <-time.After(timeout):
		return xerrors.Errorf("timeout: %v waiting for Docker to be ready", timeout)
	}
}

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

func (o *DockerOpts) String() string {
	var args []string

	// AutoRemove
	if o.AutoRemove {
		args = append(args, "--rm")
	}

	// ContainerName
	if o.ContainerName != "" {
		args = append(args, "--name", o.ContainerName)
	}

	// Network
	if o.Network != "" {
		args = append(args, "--network", o.Network)
	}

	// Volumes (handled with -v)
	if len(o.Volumes) > 0 {
		var volumeKeys []string
		for hostPath := range o.Volumes {
			volumeKeys = append(volumeKeys, hostPath)
		}
		sort.Strings(volumeKeys)
		for _, hostPath := range volumeKeys {
			containerPath := o.Volumes[hostPath]
			args = append(args, "-v", fmt.Sprintf("%s:%s", hostPath, containerPath))
		}
	}

	// Mounts (handled with --mount)
	if len(o.Mounts) > 0 {
		sort.Slice(o.Mounts, func(i, j int) bool {
			ikey := fmt.Sprintf("%s/%s/%s", o.Mounts[i].Type, o.Mounts[i].Source, o.Mounts[i].Target)
			jkey := fmt.Sprintf("%s/%s/%s", o.Mounts[j].Type, o.Mounts[j].Source, o.Mounts[j].Target)
			return ikey < jkey
		})
		for _, m := range o.Mounts {
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
	if len(o.Env) > 0 {
		sort.Strings(o.Env)
		for _, envVar := range o.Env {
			args = append(args, "-e", envVar)
		}
	}

	// Log Driver and Options
	if o.LogDriver != "" {
		args = append(args, "--log-driver", o.LogDriver)
		if len(o.LogOptions) > 0 {
			var logOptKeys []string
			for key := range o.LogOptions {
				logOptKeys = append(logOptKeys, key)
			}
			sort.Strings(logOptKeys)
			for _, key := range logOptKeys {
				value := o.LogOptions[key]
				args = append(args, "--log-opt", fmt.Sprintf("%s=%s", key, value))
			}
		}
	}

	// Attach options
	var attachOptions []string
	if o.AttachStderr {
		attachOptions = append(attachOptions, "stderr")
	}
	if o.AttachStdout {
		attachOptions = append(attachOptions, "stdout")
	}
	if len(attachOptions) > 0 {
		sort.Strings(attachOptions)
		for _, attach := range attachOptions {
			args = append(args, "--attach", attach)
		}
	}

	// Image
	if o.Image != "" {
		args = append(args, o.Image)
	}

	// Command
	if len(o.Command) > 0 {
		args = append(args, o.Command...)
	}

	cmd := append([]string{"docker", "run"}, args...)

	return strings.Join(cmd, " ")
}
