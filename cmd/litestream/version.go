package main

import (
	"context"
	"flag"
	"fmt"
)

type VersionCommand struct{}

func (c *VersionCommand) Run(ctx context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("litestream-version", flag.ContinueOnError)
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	}

	fmt.Println(Version)

	return nil
}

func (c *VersionCommand) Usage() {
	fmt.Println(`
Prints the version.

Usage:

	litestream version
`[1:])
}
