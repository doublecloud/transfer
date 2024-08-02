package model

// ---
// ch

type ChStorageParams struct {
	MdbClusterID   string
	Hosts          []string
	ChClusterName  string
	NativePort     int
	Secure         bool
	PemFileContent string
	Database       string
	User           string
	Password       string
	Token          string
	BufferSize     uint64
	HTTPPort       int
	Shards         map[string][]string
	IOHomoFormat   ClickhouseIOFormat // one of - https://clickhouse.com/docs/en/interfaces/formats
}

func (c *ChStorageParams) IsManaged() bool {
	return c.MdbClusterID != ""
}

func (s *ChSource) ToStorageParams() *ChStorageParams {
	secure := s.SSLEnabled || s.MdbClusterID != ""
	shards := s.ToSinkParams().Shards()
	hosts := make([]string, 0)
	for _, currHosts := range shards {
		hosts = currHosts
	}
	return &ChStorageParams{
		MdbClusterID:   s.MdbClusterID,
		Hosts:          hosts,
		ChClusterName:  s.ChClusterName,
		NativePort:     s.NativePort,
		Secure:         secure,
		PemFileContent: s.PemFileContent,
		Database:       s.Database,
		User:           s.User,
		Password:       string(s.Password),
		Token:          s.Token,
		BufferSize:     s.BufferSize,
		HTTPPort:       s.HTTPPort,
		Shards:         shards,
		IOHomoFormat:   s.IOHomoFormat,
	}
}

func (d *ChDestination) ToStorageParams() *ChStorageParams {
	secure := d.SSLEnabled || d.MdbClusterID != ""
	shards := d.Shards()
	hosts := make([]string, 0)
	for _, currHosts := range shards {
		hosts = currHosts
	}
	return &ChStorageParams{
		MdbClusterID:   d.MdbClusterID,
		ChClusterName:  d.ChClusterName,
		Hosts:          hosts,
		NativePort:     d.NativePort,
		Secure:         secure,
		PemFileContent: d.PemFileContent,
		Database:       d.Database,
		User:           d.User,
		Password:       string(d.Password),
		Token:          d.Token,
		BufferSize:     20 * 1024 * 1024,
		HTTPPort:       d.HTTPPort,
		Shards:         shards,
		IOHomoFormat:   "",
	}
}

// dataagent/ch.ConnConfig interface impl

type connConfigWrapper struct {
	p *ChStorageParams
}

func (w connConfigWrapper) RootCertPaths() []string {
	return nil
}

func (w connConfigWrapper) User() string {
	return w.p.User
}

func (w connConfigWrapper) Password() string {
	return w.p.Password
}
func (w connConfigWrapper) ResolvePassword() (string, error) {
	password, err := ResolvePassword(w.p.MdbClusterID, w.p.User, w.p.Password)
	return password, err
}

func (w connConfigWrapper) Database() string {
	return w.p.Database
}

func (w connConfigWrapper) HTTPPort() int {
	return w.p.HTTPPort
}

func (w connConfigWrapper) NativePort() int {
	return w.p.NativePort
}

func (w connConfigWrapper) SSLEnabled() bool {
	return w.p.Secure
}

func (w connConfigWrapper) PemFileContent() string {
	return w.p.PemFileContent
}

func (c *ChStorageParams) ToConnParams() connConfigWrapper {
	return connConfigWrapper{p: c}
}
