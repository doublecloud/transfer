package packer

type SessionPackers interface {
	Packer(isKey bool) Packer
}

type DefaultSessionPackers struct {
	key Packer
	val Packer
}

func (p *DefaultSessionPackers) Packer(isKey bool) Packer {
	if isKey {
		return p.key
	} else {
		return p.val
	}
}

func NewDefaultSessionPackers(k, v Packer) *DefaultSessionPackers {
	return &DefaultSessionPackers{
		key: k,
		val: v,
	}
}
