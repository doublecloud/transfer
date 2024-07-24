package ydb

import (
	"net"
	"strconv"
)

type connAddr struct {
	addr string
	port int
}

func connAddrFromString(addr string) connAddr {
	host, port, err := splitHostPort(addr)
	if err != nil {
		panic(err)
	}
	return connAddr{
		addr: host,
		port: port,
	}
}

func (c connAddr) String() string {
	return net.JoinHostPort(c.addr, strconv.Itoa(c.port))
}
