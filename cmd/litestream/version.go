package main

import (
	"context"
	"flag"
	"fmt"
)

// VersionCommand represents a command to print the current version.
type VersionCommand struct{}

// Run executes the command.
func (c *VersionCommand) Run(_ context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("litestream-version", flag.ContinueOnError)
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	}

	fmt.Println(Version)

	return nil
}

// Usage prints the help screen to STDOUT.
func (c *VersionCommand) Usage() {
	fmt.Println(`
Prints the version.

Usage:

	litestream version
`[1:])
}
