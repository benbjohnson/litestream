package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/superfly/ltx"
)

// VerifyCommand represents a command to verify the integrity of LTX files.
type VerifyCommand struct{}

// NewVerifyCommand returns a new instance of VerifyCommand.
func NewVerifyCommand() *VerifyCommand {
	return &VerifyCommand{}
}

// Run executes the command.
func (c *VerifyCommand) Run(ctx context.Context, args []string) (ret error) {
	fs := flag.NewFlagSet("ltx-verify", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Println(`
The verify command reads one or more LTX files and verifies its integrity.

Usage:

	ltx verify PATH [PATH...]

`[1:],
		)
	}
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() == 0 {
		return fmt.Errorf("at least one LTX file must be specified")
	}

	var okN, errorN int
	for _, filename := range fs.Args() {
		if err := c.verifyFile(ctx, filename); err != nil {
			errorN++
			fmt.Printf("%s: %s\n", filename, err)
			continue
		}

		okN++
	}

	if errorN != 0 {
		return fmt.Errorf("%d ok, %d invalid", okN, errorN)
	}

	fmt.Println("ok")
	return nil
}

func (c *VerifyCommand) verifyFile(_ context.Context, filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	return ltx.NewDecoder(f).Verify()
}
