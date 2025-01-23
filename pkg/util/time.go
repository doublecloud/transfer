package util

import "time"

// AnnoDominiBeginning returns the first time instant of the AD (modern) epoch in the given location.
func AnnoDominiBeginning(loc *time.Location) time.Time {
	return time.Date(1, 1, 1, 0, 0, 0, 0, loc)
}

// BeforeChristEnding returns the first time instant AFTER the end of the BC (pre-modern) epoch in the given location.
func BeforeChristEnding(loc *time.Location) time.Time {
	return time.Date(0, 1, 1, 0, 0, 0, 0, loc)
}
