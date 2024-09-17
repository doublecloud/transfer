package model

import (
	"fmt"
)

type CleanupType string

const (
	Drop            CleanupType = "Drop"
	Truncate        CleanupType = "Truncate"
	DisabledCleanup CleanupType = "Disabled"
)

func (ct CleanupType) IsValid() error {
	switch ct {
	case Drop, Truncate, DisabledCleanup:
		return nil
	}
	return fmt.Errorf("invalid cleanup type: %v", ct)
}
