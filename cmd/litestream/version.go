package main

import (
	"context"
	"flag"
	"fmt"
	"runtime"
	"runtime/debug"
)

// defaultVersion is the placeholder used when no version was injected at
// build time via -ldflags "-X main.Version=...".
const defaultVersion = "(development build)"

// resolveVersion returns the most descriptive version string available:
// the ldflags-injected value, the VCS-stamped module version, the VCS
// revision, or the development-build placeholder, in that order.
func resolveVersion(injected string, bi *debug.BuildInfo) string {
	if injected != "" && injected != defaultVersion {
		return injected
	}
	if bi == nil {
		return defaultVersion
	}

	if v := bi.Main.Version; v != "" && v != "(devel)" {
		return v
	}

	revision, modified, _ := vcsSettings(bi)
	if revision == "" {
		return defaultVersion
	}
	if len(revision) > 12 {
		revision = revision[:12]
	}
	if modified {
		revision += "-dirty"
	}
	return fmt.Sprintf("(development build %s)", revision)
}

// vcsSettings extracts VCS metadata from embedded build info settings.
// The vcs.time value is the commit timestamp, not the build time.
func vcsSettings(bi *debug.BuildInfo) (revision string, modified bool, commitTime string) {
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			revision = s.Value
		case "vcs.modified":
			modified = s.Value == "true"
		case "vcs.time":
			commitTime = s.Value
		}
	}
	return revision, modified, commitTime
}

// VersionCommand represents a command to print the current version.
type VersionCommand struct{}

// Run executes the command.
func (c *VersionCommand) Run(_ context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("litestream-version", flag.ContinueOnError)
	verbose := fs.Bool("verbose", false, "print detailed build metadata")
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	}

	if !*verbose {
		fmt.Println(Version)
		return nil
	}

	fmt.Printf("Version:     %s\n", Version)
	fmt.Printf("Go Version:  %s\n", runtime.Version())
	if bi, ok := debug.ReadBuildInfo(); ok {
		revision, modified, commitTime := vcsSettings(bi)
		if revision != "" {
			if modified {
				revision += " (dirty)"
			}
			fmt.Printf("Git Commit:  %s\n", revision)
		}
		if commitTime != "" {
			fmt.Printf("Commit Time: %s\n", commitTime)
		}
	}
	fmt.Printf("OS/Arch:     %s/%s\n", runtime.GOOS, runtime.GOARCH)

	return nil
}

// Usage prints the help screen to STDOUT.
func (c *VersionCommand) Usage() {
	fmt.Println(`
Prints the version.

Usage:

	litestream version [arguments]

Arguments:

	-verbose
	    Print detailed build metadata (Go version, commit, build time, OS/arch).
`[1:])
}
