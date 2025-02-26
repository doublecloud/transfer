package abstract

type MatchedOp struct {
	op     Op
	tokens []*Token
}

func (r *MatchedOp) MatchedSubstring(originalStr string) (string, bool) {
	currMin, currMax, isFound := tokensMinMaxPos(r.tokens)
	if !isFound {
		return "", false
	}
	return originalStr[currMin : currMax+1], true
}

func NewMatchedOp(op Op, tokens []*Token) *MatchedOp {
	return &MatchedOp{
		op:     op,
		tokens: tokens,
	}
}
