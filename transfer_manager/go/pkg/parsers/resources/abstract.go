package resources

type AbstractResources interface {
	GetResource(string) (string, error)

	RunWatcher()
	OutdatedCh() <-chan struct{}
	Close()
}

type Resourceable interface {
	ResourcesObj() AbstractResources
}
