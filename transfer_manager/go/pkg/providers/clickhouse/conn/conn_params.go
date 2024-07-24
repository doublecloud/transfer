package conn

type ConnParams interface {
	User() string
	Password() string
	ResolvePassword() (string, error)
	Database() string
	HTTPPort() int
	NativePort() int
	SSLEnabled() bool
	PemFileContent() string
	RootCertPaths() []string
}
