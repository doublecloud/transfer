package model

type OperationWorker struct {
	OperationID string
	WorkerIndex int
	Completed   bool
	Err         string
	Progress    *AggregatedProgress
}

func NewOperationWorker() *OperationWorker {
	return &OperationWorker{
		OperationID: "",
		WorkerIndex: 0,
		Completed:   false,
		Err:         "",
		Progress:    nil,
	}
}
