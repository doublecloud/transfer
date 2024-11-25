package yatool

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
)

var (
	arcadiaRoot     string
	arcadiaRootErr  error
	arcadiaRootOnce sync.Once

	arcInstanceRoot     string
	arcInstanceRootErr  error
	arcInstanceRootOnce sync.Once
)

// FindArcadiaRoot searches Arcadia root for the target path
//
// Implementation reference https://github.com/doublecloud/transfer/arc/trunk/arcadia/library/python/find_root
func FindArcadiaRoot(arcPath string) (string, error) {
	isRoot := func(arcPath string) bool {
		if _, err := os.Stat(filepath.Join(arcPath, ".arcadia.root")); err == nil {
			return true
		}

		if _, err := os.Stat(filepath.Join(arcPath, "devtools", "ya", "ya.conf.json")); err == nil {
			return true
		}

		return false
	}

	arcPath, err := filepath.Abs(arcPath)
	if err != nil {
		return "", err
	}

	current := filepath.Clean(arcPath)
	for {
		if isRoot(current) {
			return current, nil
		}

		next := filepath.Dir(current)
		if next == current {
			return "", errors.New("can't find arcadia root")
		}

		current = next
	}
}

// ArcadiaRoot returns the current Arcadia root
func ArcadiaRoot() (string, error) {
	arcadiaRootOnce.Do(func() {
		arcadiaRoot, arcadiaRootErr = FindArcadiaRoot(".")
	})

	return arcadiaRoot, arcadiaRootErr
}

func FindArcInstanceRoot(arcPath string, arcRootFiles []string) (string, error) {
	isRoot := func(arcPath string) bool {
		for _, rootFile := range arcRootFiles {
			if _, err := os.Stat(filepath.Join(arcPath, rootFile)); err == nil {
				return true
			}
		}
		return false
	}

	arcPath, err := filepath.Abs(arcPath)
	if err != nil {
		return "", err
	}

	current := filepath.Clean(arcPath)
	for {
		if isRoot(current) {
			return current, nil
		}

		next := filepath.Dir(current)
		if next == current {
			return "", errors.New("can't find arcadia root")
		}

		current = next
	}
}

func ArcInstanceRoot() (string, error) {
	arcInstanceRootOnce.Do(func() {
		arcInstanceRoot, arcInstanceRootErr = FindArcInstanceRoot(".", []string{".arcadia.root", ".cloudia.root"})
	})

	return arcInstanceRoot, arcInstanceRootErr
}
