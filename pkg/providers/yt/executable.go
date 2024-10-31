package yt

import (
	"context"
	"io"
	"math/rand"
	"os"
	"time"

	"github.com/doublecloud/transfer/internal/logger"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/library/go/test/yatest"
	"github.com/doublecloud/transfer/pkg/cleanup"
	"github.com/doublecloud/transfer/pkg/config/env"
	ytclient "github.com/doublecloud/transfer/pkg/providers/yt/client"
	"github.com/doublecloud/transfer/pkg/randutil"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

var (
	ExePath    ypath.Path
	exeVersion string
)

// InitExe uploads exe and initializes related variables
func InitExe() {
	if !env.IsTest() {
		return
	}
	for _, arg := range os.Args[1:] {
		if arg == "-test.list" {
			logger.Log.Infof("%q argument found, skipping initialization", arg)
			return
		}
	}

	if err := uploadLightExe(); err != nil {
		logger.Log.Error("unable to upload light exe", log.Error(err))
		panic(err)
	}
}

func InitContainerExe() {
	if !env.IsTest() {
		return
	}
	for _, arg := range os.Args[1:] {
		if arg == "-test.list" {
			logger.Log.Infof("%q argument found, skipping initialization", arg)
			return
		}
	}

	if err := uploadContainerLightExe(); err != nil {
		logger.Log.Error("unable to upload light exe", log.Error(err))
		panic(err)
	}
}

// uploadContainerLightExe need to have transfer_manager/go/pkg/providers/yt/lightexe compiled as
// `GOOS=linux GOARCH=arm64  go build  .`
func uploadContainerLightExe() error {
	lightExePath := "transfer_manager/go/pkg/providers/yt/lightexe/lightexe"
	binaryPath := yatest.SourcePath(lightExePath)
	logger.Log.Info("starting light exe upload")
	err := uploadExe("light_exe_", binaryPath)
	if err != nil {
		return xerrors.Errorf("unable to upload light exe: %w", err)
	}
	logger.Log.Infof("light exe was successfully uploaded to %q", ExePath)
	return nil
}

func uploadLightExe() error {
	lightExePath := "transfer_manager/go/pkg/providers/yt/lightexe/lightexe"
	binaryPath, err := yatest.BinaryPath(lightExePath)
	if err != nil {
		return xerrors.Errorf("unable to get light exe binary path %q: %w", lightExePath, err)
	}
	logger.Log.Info("starting light exe upload")
	err = uploadExe("light_exe_", binaryPath)
	if err != nil {
		return xerrors.Errorf("unable to upload light exe: %w", err)
	}
	logger.Log.Infof("light exe was successfully uploaded to %q", ExePath)
	return nil
}

func dataplaneDir(cluster string) ypath.Path {
	if cluster == "vanga" {
		return "//home/transfer-manager/data-plane"
	}
	return "//home/data-transfer/data-plane"
}

func DataplaneExecutablePath(cluster, revision string) ypath.Path {
	return dataplaneDir(cluster).Child(revision)
}

func uploadExe(exePrefix, exePath string) error {
	client, err := ytclient.NewYtClientWrapper(ytclient.HTTP, nil, new(yt.Config))
	if err != nil {
		return xerrors.Errorf("unable to initialize yt client: %w", err)
	}
	defer client.Stop()

	rand.Seed(time.Now().UnixNano())
	exeVersion = exePrefix + randutil.GenerateAlphanumericString(8)
	ExePath = DataplaneExecutablePath("", exeVersion)
	if _, err := client.CreateNode(context.Background(), ExePath, yt.NodeFile, &yt.CreateNodeOptions{Recursive: true}); err != nil {
		return xerrors.Errorf("unable to create node %q: %w", ExePath, err)
	}

	exeFile, err := os.Open(exePath)
	if err != nil {
		return xerrors.Errorf("unable to open file %q: %w", exePath, err)
	}
	defer cleanup.Close(exeFile, logger.Log)

	writer, err := client.WriteFile(context.Background(), ExePath, &yt.WriteFileOptions{})
	if err != nil {
		return xerrors.Errorf("unable to initialize writer for file %q: %w", ExePath, err)
	}
	defer cleanup.Close(writer, logger.Log)

	if _, err := io.Copy(writer, exeFile); err != nil {
		return xerrors.Errorf("unable to copy file %q to path %q: %w", exePath, ExePath, err)
	}

	pathToUdfs := dataplaneDir("").Child("udfs").Child(exeVersion)
	if _, err = client.CreateNode(context.Background(), pathToUdfs, yt.NodeMap, &yt.CreateNodeOptions{Recursive: true}); err != nil {
		return xerrors.Errorf("unable to create udfs directory %q: %w", pathToUdfs, err)
	}

	return nil
}
