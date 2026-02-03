package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/superfly/ltx"
)

// ChecksumCommand represents a command to compute the LTX checksum of a database file.
type ChecksumCommand struct{}

// NewChecksumCommand returns a new instance of ChecksumCommand.
func NewChecksumCommand() *ChecksumCommand {
	return &ChecksumCommand{}
}

// Run executes the command.
func (c *ChecksumCommand) Run(ctx context.Context, args []string) (ret error) {
	fs := flag.NewFlagSet("ltx-checksum", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Println(`
The checksum command computes the LTX checksum for a database file.

Usage:

	ltx checksum PATH
`[1:],
		)
	}
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() == 0 {
		fs.Usage()
		return flag.ErrHelp
	} else if fs.NArg() > 1 {
		return fmt.Errorf("too many arguments")
	}

	f, err := os.Open(fs.Arg(0))
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	// Read database header to determine page size.
	buf := make([]byte, 100)
	if _, err := io.ReadFull(f, buf); err != nil {
		return err
	}
	pageSize := int(binary.BigEndian.Uint16(buf[16:18]))
	if pageSize == 1 {
		pageSize = 65536
	}

	// Reseek to beginning and compute checksum.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	chksum, err := ltx.ChecksumReader(f, pageSize)
	if err != nil {
		return err
	}

	fmt.Println(chksum)
	return nil
}
