package dbt

import (
	"context"
	"fmt"
	"os"
	"os/exec"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	server "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/runtime/shared/pod"
	"go.ytsaurus.tech/library/go/core/log"
	"gopkg.in/yaml.v3"
)

type runner struct {
	dst SupportedDestination
	cfg *Config

	transfer *server.Transfer
}

func newRunner(dst SupportedDestination, cfg *Config, transfer *server.Transfer) *runner {
	return &runner{
		dst: dst,
		cfg: cfg,

		transfer: transfer,
	}
}

func (r *runner) Run(ctx context.Context) error {
	r.cleanupConfiguration(ctx)
	if err := r.initializeDocker(ctx); err != nil {
		return xerrors.Errorf("failed to initialize docker for DBT: %w", err)
	}
	if err := r.initializeConfiguration(ctx); err != nil {
		return xerrors.Errorf("failed to initialize DBT configuration files: %w", err)
	}
	defer r.cleanupConfiguration(ctx)
	if err := r.run(ctx); err != nil {
		return xerrors.Errorf("failed to run DBT: %w", err)
	}
	return nil
}

func (r *runner) initializeDocker(ctx context.Context) error {
	if err := executeCommand(ctx, "docker", "pull", r.fullImageID()); err != nil {
		return xerrors.Errorf("docker initialization failed: %w", err)
	}
	return nil
}

// executeCommand executes the given command, automatically logs its stdout and stderr and returns a detailed error if a command failed.
//
// The command itself is logged partially: only its name and the first arg are logged. Stdout and stderr are logged completely.
func executeCommand(ctx context.Context, name string, args ...string) error {
	commandNameToLog := name
	if len(args) > 0 {
		commandNameToLog = commandNameToLog + " " + args[0]
	}
	cmd := exec.CommandContext(ctx, name, args...)
	output, err := cmd.CombinedOutput() // note this method also executes the command!
	if err != nil {
		logger.Log.Error(fmt.Sprintf("failed to execute `%s`\nstdout:%s", commandNameToLog, string(output)), log.String("stdout", string(output)), log.Error(err))
		return xerrors.Errorf("failed to execute `%s`, see logs for the detailed command output. Direct cause: %w", commandNameToLog, err)
	}
	logger.Log.Info(fmt.Sprintf("successfully executed `%s`", commandNameToLog), log.String("stdout", string(output)))
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

	if err := executeCommand(ctx, "git", r.gitCloneCommands()...); err != nil {
		return xerrors.Errorf("failed to clone a remote repository: %w", err)
	}

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

func (r *runner) gitCloneCommands() []string {
	result := []string{"clone", "--depth", "1"}
	if branch := r.cfg.GitBranch; len(branch) > 0 {
		result = append(result, "--branch", branch)
	}
	result = append(result, r.cfg.GitRepositoryLink, pathProject())
	return result
}

func (r *runner) cleanupConfiguration(ctx context.Context) {
	if err := executeCommand(ctx, "rm", "-r", "-f", pathProject()); err != nil {
		logger.Log.Warn("DBT project cleanup failed", log.Error(err))
	}
	if err := executeCommand(ctx, "rm", "-f", pathProfiles()); err != nil {
		logger.Log.Warn("DBT profiles cleanup failed", log.Error(err))
	}
}

func (r *runner) run(ctx context.Context) error {
	if err := executeCommand(
		ctx,
		"docker", "run",
		"--rm",
		"--network", "host",
		"--add-host", "host.docker.internal:host-gateway",
		"--mount", fmt.Sprintf("type=bind,source=%s,target=/usr/app", pathProject()),
		"--mount", fmt.Sprintf("type=bind,source=%s,target=/root/.dbt/profiles.yml", pathProfiles()),
		"--env", "AWS_EC2_METADATA_DISABLED=true", // this will disable going into metadata for IAM tokens
		"--log-driver", "local", // log driver options are needed to avoid disk overfill by container logs
		"--log-opt", "max-size=100m",
		"--log-opt", "max-file=3",
		r.fullImageID(),
		r.cfg.Operation,
	); err != nil {
		return xerrors.Errorf("docker run failed: %w", err)
	}
	return nil
}
