package cmd

import (
	"errors"
	"os"
	"path/filepath"
)

// Globals
var (
	// List of folders containing components
	ComponentFolders []string

	// Paths within ComponentFolders to ignore
	ExcludeFolders []string
)

// Navigate to the root of the repo
func cwdToRepoRoot() error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	cwd, err = filepath.Abs(cwd)
	if err != nil {
		return err
	}
	for {
		stat, err := os.Stat(filepath.Join(cwd, ".git"))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}

		if err == nil && stat != nil && stat.IsDir() {
			return os.Chdir(cwd)
		}

		// Go one level up
		cwd = filepath.Dir(cwd)
		if cwd == "" || cwd == "." || cwd == string(filepath.Separator) {
			return errors.New("could not find the repository's root")
		}
	}
}
