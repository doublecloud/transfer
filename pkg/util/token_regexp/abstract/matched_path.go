package abstract

type MatchedPath struct {
	path         []*MatchedOp
	lengthTokens int
}

func (r *MatchedPath) Length() int {
	return r.lengthTokens
}

func (r *MatchedPath) CapturingGroupArr() [][]*MatchedOp {
	isParentMatched := func(checkingOp *MatchedOp, potentialParent Op) bool {
		currentOp := checkingOp.op
		for {
			if currentOp.Parent() == potentialParent {
				return true
			}
			currentOp = currentOp.Parent()
			if currentOp == nil {
				return false
			}
		}
	}

	result := make([][]*MatchedOp, 0, len(r.path))
	currBucket := make([]*MatchedOp, 0)
	var newCapturingGroupAddr Op = nil
	for _, el := range r.path {
		if newCapturingGroupAddr != nil && isParentMatched(el, newCapturingGroupAddr) {
			currBucket = append(currBucket, el)
		}
		if _, ok := el.op.(IsCapturingGroup); ok {
			// flush existing bucket
			if len(currBucket) != 0 {
				result = append(result, currBucket)
				currBucket = make([]*MatchedOp, 0)
			}
			currBucket = append(currBucket, el)
			newCapturingGroupAddr = el.op
		}
	}

	// flush existing bucket
	if len(currBucket) != 0 {
		result = append(result, currBucket)
	}

	return result
}

func NewEmptyMatchedPath() *MatchedPath {
	return &MatchedPath{
		path:         nil,
		lengthTokens: 0,
	}
}

func NewMatchedPathSimple(matchedOp *MatchedOp) *MatchedPath {
	return &MatchedPath{
		path:         []*MatchedOp{matchedOp},
		lengthTokens: len(matchedOp.tokens),
	}
}

func NewMatchedPathChild(parentOp *MatchedOp, childPath *MatchedPath) *MatchedPath {
	currPath := make([]*MatchedOp, 0, len(childPath.path)+1)
	currPath = append(currPath, parentOp)
	currPath = append(currPath, childPath.path...)
	return &MatchedPath{
		path:         currPath,
		lengthTokens: len(parentOp.tokens) + childPath.lengthTokens,
	}
}
