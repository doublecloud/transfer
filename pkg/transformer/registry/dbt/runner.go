package dbt

import (
	"bytes"
	"context"
	"fmt"
	"os"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/mount"
	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/docker"
	"github.com/doublecloud/transfer/pkg/runtime/shared/pod"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"go.ytsaurus.tech/library/go/core/log"
	"gopkg.in/yaml.v3"
)

type runner struct {
	dst SupportedDestination
	cfg *Config

	transfer *model.Transfer

	dw *docker.DockerWrapper
}

func newRunner(dst SupportedDestination, cfg *Config, transfer *model.Transfer) (*runner, error) {
	dockerWrapper, err := docker.NewDockerWrapper(nil)
	if err != nil {
		return nil, err
	}

	return &runner{
		dst: dst,
		cfg: cfg,

		transfer: transfer,

		dw: dockerWrapper,
	}, nil
}

func (r *runner) Run(ctx context.Context) error {
	r.cleanupConfiguration()
	if err := r.initializeDocker(ctx); err != nil {
		return xerrors.Errorf("failed to initialize docker for DBT: %w", err)
	}
	if err := r.initializeConfiguration(ctx); err != nil {
		return xerrors.Errorf("failed to initialize DBT configuration files: %w", err)
	}
	defer r.cleanupConfiguration()
	if err := r.run(ctx); err != nil {
		return xerrors.Errorf("failed to run DBT: %w", err)
	}
	return nil
}

func (r *runner) initializeDocker(ctx context.Context) error {
	if err := r.dw.Pull(ctx, r.fullImageID(), types.ImagePullOptions{}); err != nil {
		return xerrors.Errorf("docker initialization failed: %w", err)
	}
	return nil
}

func (r *runner) fullImageID() string {
	return fmt.Sprintf("%s:%s", dockerRegistryID(), r.dockerImageTag())
}

func dockerRegistryID() string {
	return fmt.Sprintf("%s/%s", os.Getenv("DBT_CONTAINER_REGISTRY"), `data-transfer-dbt`)
}

func (r *runner) dockerImageTag() string {
	// the tag can be made customizable in the DBT common configuration, r.g. to control the DBT version
	// the tag is currently produced by a manual push of the image with the use of the `datacloud/Makefile` `release-dbt-...` targets
	return os.Getenv("DBT_IMAGE_TAG")
}

func (r *runner) initializeConfiguration(ctx context.Context) error {
	if err := os.MkdirAll(dataDirectory(), (os.ModeDir | 0700)); err != nil {
		return xerrors.Errorf("failed to create the DBT data directory %q: %w", dataDirectory(), err)
	}

	destinationConfiguration, err := r.dst.DBTConfiguration(ctx)
	if err != nil {
		return xerrors.Errorf("failed to compose a DBT configuration of the destination database: %w", err)
	}
	marshalledDestinationConfiguration, err := yaml.Marshal(
		map[string]any{
			r.cfg.ProfileName: map[string]any{
				"target": "dev",
				"outputs": map[string]any{
					"dev": destinationConfiguration,
				},
			},
		},
	)
	if err != nil {
		return xerrors.Errorf("failed to marshal the DBT configuration of the destination database into YAML: %w", err)
	}
	if err := os.WriteFile(pathProfiles(), marshalledDestinationConfiguration, 0644); err != nil {
		return xerrors.Errorf("failed to write the profile file to '%s': %w", pathProfiles(), err)
	}

	outBuf := new(bytes.Buffer)
	opts := r.gitCloneCommands()
	opts.Progress = outBuf

	if _, err := git.PlainClone(pathProject(), false, opts); err != nil {
		return xerrors.Errorf("failed to clone a remote repository: %w", err)
	}
	logger.Log.Info(fmt.Sprintf("successfully executed `git clone %s %s`",
		r.cfg.GitRepositoryLink, pathProject()), log.String("stdout", outBuf.String()))

	return nil
}

func dataDirectory() string {
	return fmt.Sprintf("%s/%s", GlobalDataDirectory(), "dbt_data")
}

func GlobalDataDirectory() string {
	if baseDir, ok := os.LookupEnv("BASE_DIR"); ok {
		return baseDir
	}
	return pod.SharedDir
}

func pathProfiles() string {
	return fmt.Sprintf("%s/%s", dataDirectory(), "profiles.yml")
}

func pathProject() string {
	return fmt.Sprintf("%s/%s", dataDirectory(), "project")
}

func (r *runner) gitCloneCommands() *git.CloneOptions {
	opts := &git.CloneOptions{
		URL:   r.cfg.GitRepositoryLink,
		Depth: 1,
	}

	if branch := r.cfg.GitBranch; len(branch) > 0 {
		opts.ReferenceName = plumbing.NewBranchReferenceName(branch)
		opts.SingleBranch = true
	}

	return opts
}

func (r *runner) cleanupConfiguration() {
	if err := os.RemoveAll(pathProject()); err != nil {
		logger.Log.Warn("DBT project cleanup failed", log.Error(err))
	}
	if err := os.Remove(pathProfiles()); err != nil {
		logger.Log.Warn("DBT profiles cleanup failed", log.Error(err))
	}
}

func (r *runner) run(ctx context.Context) error {
	opts := docker.DockerOpts{
		Volumes:   map[string]string{},
		LogDriver: "local",
		LogOptions: map[string]string{
			"max-size": "100m",
			"max-file": "3",
		},
		Image:   r.fullImageID(),
		Network: "host",
		Command: []string{
			r.cfg.Operation,
		},
		Env: []string{
			"AWS_EC2_METADATA_DISABLED=true",
		},
		AutoRemove: true,
		Mounts: []mount.Mount{
			{
				Type:   mount.TypeBind,
				Source: pathProject(),
				Target: "/usr/app",
			},
			{
				Type:   mount.TypeBind,
				Source: pathProfiles(),
				Target: "/root/.dbt/profiles.yml",
			},
		},
		AttachStdout: true,
		AttachStderr: true,
	}

	if _, _, err := r.dw.Run(ctx, opts); err != nil {
		return xerrors.Errorf("docker run failed: %w", err)
	}

	return nil
}
