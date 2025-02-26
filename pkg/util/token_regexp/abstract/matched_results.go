package abstract

type MatchedResults struct {
	paths []*MatchedPath
}

func (r *MatchedResults) AddMatchedPath(matchedPath *MatchedPath) {
	r.paths = append(r.paths, matchedPath)
}

func (r *MatchedResults) AddMatchedPathsAfterConsumeComplex(op Op) {
	newPaths := make([]*MatchedPath, 0, len(r.paths))
	for _, currPath := range r.paths {
		matchedOp := NewMatchedOp(op, nil) // COMPLEX OP ALWAYS MATCHED TO 0 TOKENS
		newPath := NewMatchedPathChild(matchedOp, currPath)
		newPaths = append(newPaths, newPath)
	}
	r.paths = newPaths
}

func (r *MatchedResults) AddMatchedPathsAfterSimpleConsume(lengths []int, op Op, allLeastTokens []*Token) {
	if lengths == nil {
		return
	}
	for _, currLength := range lengths {
		matchedOp := NewMatchedOp(op, allLeastTokens[0:currLength])
		r.paths = append(r.paths, NewMatchedPathSimple(matchedOp))
	}
}

func (r *MatchedResults) AddLocalPaths(collector *MatchedResults, op Op, opTokens []*Token) {
	if len(collector.paths) == 0 {
		return
	}
	newPaths := make([]*MatchedPath, 0, len(collector.paths))
	for _, path := range collector.paths {
		matchedOp := NewMatchedOp(op, opTokens)
		newPaths = append(newPaths, NewMatchedPathChild(matchedOp, path))
	}
	r.paths = append(r.paths, newPaths...)
}

func (r *MatchedResults) AddAllPaths(collector *MatchedResults) {
	r.paths = append(r.paths, collector.paths...)
}

func (r *MatchedResults) Size() int {
	return len(r.paths)
}

func (r *MatchedResults) Index(index int) *MatchedPath {
	return r.paths[index]
}

func (r *MatchedResults) IsMatched() bool {
	return len(r.paths) != 0
}

func (r *MatchedResults) MatchedSubstring(originalStr string) string {
	if r == nil {
		return ""
	}
	if len(r.paths) == 0 {
		return ""
	}
	currPath := r.paths[0]
	return ResolveMatchedOps(originalStr, currPath.path)
}

func NewMatchedResults() *MatchedResults {
	return &MatchedResults{
		paths: make([]*MatchedPath, 0),
	}
}
