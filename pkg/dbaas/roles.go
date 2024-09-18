package dbaas

type Role string

const (
	MASTER  = Role("MASTER")
	REPLICA = Role("REPLICA")
	ANY     = Role("ANY")
	KAFKA   = Role("KAFKA")
)

type Health string

const (
	ALIVE = Health("ALIVE")
)
