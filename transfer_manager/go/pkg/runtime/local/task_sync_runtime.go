package local

import (
	"context"
	"fmt"
	"runtime/pprof"
	"sync"

	"github.com/doublecloud/tross/library/go/core/log"
	"github.com/doublecloud/tross/library/go/core/metrics/solomon"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/coordinator"
	server "github.com/doublecloud/tross/transfer_manager/go/pkg/abstract/model"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/worker/tasks"
)

type SyncTask struct {
	task     *server.TransferOperation
	logger   log.Logger
	transfer server.Transfer
	wg       *sync.WaitGroup
	cp       coordinator.Coordinator
}

func (s *SyncTask) Stop() {
	s.wg.Wait()
}

func (s *SyncTask) Runtime() abstract.Runtime {
	return new(abstract.LocalRuntime)
}

func (s *SyncTask) run() {
	defer s.wg.Done()
	runnableTaskType, _ := s.task.TaskType.Task.(abstract.RunnableTask)

	err := tasks.Run(
		context.Background(),
		*s.task,
		runnableTaskType,
		s.cp,
		s.transfer,
		s.task.Params,
		solomon.NewRegistry(solomon.NewRegistryOpts()),
	)
	if err := s.cp.FinishOperation(s.task.OperationID, s.transfer.CurrentJobIndex(), err); err != nil {
		s.logger.Error("unable to call finish operation", log.Error(err))
	}
}

// NewSyncTask only used for local debug, can operate properly only on single machine transfer server installation
// with enable `all_in_one_binary` flag
func NewSyncTask(
	task *server.TransferOperation,
	cp coordinator.Coordinator,
	workflow server.OperationWorkflow,
	transfer server.Transfer,
) (*SyncTask, error) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	st := &SyncTask{
		task:     task,
		cp:       cp,
		logger:   logger.Log,
		transfer: transfer,
		wg:       wg,
	}

	if task.Status == server.NewTask {
		if err := workflow.OnStart(task); err != nil {
			st.Stop()
			return nil, xerrors.Errorf("unable to start task workflow: %w", err)
		}
		rt, ok := transfer.Runtime.(*abstract.LocalRuntime)
		if ok && rt.WorkersNum() > 1 {
			for i := 1; i <= rt.WorkersNum(); i++ {
				subTr := st.transfer
				subTr.Runtime = &abstract.LocalRuntime{
					Host:       rt.Host,
					CurrentJob: i,
					ShardingUpload: abstract.ShardUploadParams{
						JobCount:     rt.ShardingUpload.JobCount,
						ProcessCount: rt.ShardingUpload.ProcessCount,
					},
				}
				wg.Add(1)
				sst := &SyncTask{
					wg:       wg,
					task:     task,
					cp:       cp,
					logger:   logger.Log,
					transfer: subTr,
				}
				labels := pprof.Labels("dt_job_id", fmt.Sprint(i))
				go pprof.Do(context.Background(), labels, func(ctx context.Context) {
					sst.run()
				})
			}
		}
		labels := pprof.Labels("dt_job_id", "0")
		go pprof.Do(context.Background(), labels, func(ctx context.Context) {
			st.run()
		})
	} else {
		return nil, abstract.NewFatalError(xerrors.New("task already running"))
	}
	return st, nil
}
