package connection

var _ ManagedConnection = (*ConnectionCH)(nil)

type ConnectionCH struct {
}

func (pg *ConnectionCH) GetUsername() string {
	return ""
}

func (pg *ConnectionCH) GetClusterID() string {
	return ""
}

func (pg *ConnectionCH) GetDataBases() []string {
	return nil
}

func (pg *ConnectionCH) HostNames() []string {
	return nil
}
