package resources

type NoResources struct{}

func (r *NoResources) GetResource(string) (string, error) {
	return "", nil
}

func (r *NoResources) RunWatcher() {
}

func (r *NoResources) OutdatedCh() <-chan struct{} {
	return nil
}

func (r *NoResources) Close() {
}
