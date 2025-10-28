//go:build ci
// +build ci

package ci

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"
	"testing"
)

func TestGoModTidy(t *testing.T) {
	modified, err := modifiedFiles()
	if err != nil {
		t.Fatal(err)
	}

	if len(modified) > 0 {
		t.Fatalf("Modified files detected before running `go mod tidy`: \n%s", strings.Join(modified, "\n"))
	}

	output, err := exec.Command("go", "mod", "tidy").CombinedOutput()
	if err != nil {
		t.Fatalf("go mod tidy err: %s %s", err, output)
	}

	modified, err = modifiedFiles()
	if err != nil {
		t.Fatal(err)
	}

	if len(modified) > 0 {
		t.Fatalf("Modified files detected from running `go mod tidy`: %v", modified)
	}
}

func modifiedFiles() ([]string, error) {
	var modifiedFiles []string

	status, err := exec.Command("git", "status", "-s").CombinedOutput()
	if err != nil {
		fmt.Printf("%s\n", status)
		return nil, err
	}

	lines := bytes.Split(status, []byte("\n"))
	for _, line := range lines {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		if bytes.HasPrefix(line, []byte("??")) {
			// ignore untracked files
			continue
		}
		modifiedFiles = append(modifiedFiles, string(line))
	}

	return modifiedFiles, nil
}
