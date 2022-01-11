package main

import (
	"context"
	"flag"
	"fmt"
	"io"
)

// VersionCommand represents a command to print the current version.
type VersionCommand struct {
	stdin  io.Reader
	stdout io.Writer
	stderr io.Writer
}

// NewVersionCommand returns a new instance of VersionCommand.
func NewVersionCommand(stdin io.Reader, stdout, stderr io.Writer) *VersionCommand {
	return &VersionCommand{
		stdin:  stdin,
		stdout: stdout,
		stderr: stderr,
	}
}

// Run executes the command.
func (c *VersionCommand) Run(ctx context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("litestream-version", flag.ContinueOnError)
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	}

	fmt.Fprintln(c.stdout, Version)

	return nil
}

// Usage prints the help screen to STDOUT.
func (c *VersionCommand) Usage() {
	fmt.Fprintln(c.stdout, `
Prints the version.

Usage:

	litestream version
`[1:])
}
