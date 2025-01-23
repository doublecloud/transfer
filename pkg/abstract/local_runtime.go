package abstract

type LocalRuntime struct {
	Host           string            `json:"host"`
	CurrentJob     int               `json:"current_job"`
	ShardingUpload ShardUploadParams `json:"sharding_upload"`
}

func (*LocalRuntime) Type() RuntimeType {
	return LocalRuntimeType
}

func (l *LocalRuntime) NeedRestart(runtime Runtime) bool {
	return false
}

func (l *LocalRuntime) WithDefaults() {
}

func (l *LocalRuntime) Validate() error {
	return nil
}

func (l *LocalRuntime) WorkersNum() int          { return l.ShardingUpload.JobCount }
func (l *LocalRuntime) ThreadsNumPerWorker() int { return l.ShardingUpload.ProcessCount }
func (l *LocalRuntime) CurrentJobIndex() int     { return l.CurrentJob }
func (l *LocalRuntime) IsMain() bool             { return l.CurrentJob == 0 }
func (l *LocalRuntime) SetVersion(runtimeSpecificVersion string, versionProperties *string) error {
	return nil
}
