package abstract

type Includeable interface {
	// Include returns true if the given table is included
	Include(tID TableID) bool
}

type IncludeTableList interface {
	Includeable
	IncludeTableList() ([]TableID, error)
}

type intersectionIncludeable struct {
	a Includeable
	b Includeable
}

func NewIntersectionIncludeable(a, b Includeable) Includeable {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	return &intersectionIncludeable{
		a: a,
		b: b,
	}
}

func (i intersectionIncludeable) Include(tID TableID) bool {
	return i.a.Include(tID) && i.b.Include(tID)
}
