package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/superfly/ltx"
)

// ApplyCommand represents a command to apply a series of LTX files to a database file.
type ApplyCommand struct{}

// NewApplyCommand returns a new instance of ApplyCommand.
func NewApplyCommand() *ApplyCommand {
	return &ApplyCommand{}
}

// Run executes the command.
func (c *ApplyCommand) Run(ctx context.Context, args []string) (ret error) {
	fs := flag.NewFlagSet("ltx-apply", flag.ContinueOnError)
	dbPath := fs.String("db", "", "database path")
	fs.Usage = func() {
		fmt.Println(`
The apply command applies one or more LTX files to a database file.

Usage:

	ltx apply [arguments] PATH [PATH...]

Arguments:
`[1:])
		fs.PrintDefaults()
		fmt.Println()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() == 0 {
		fs.Usage()
		return flag.ErrHelp
	} else if *dbPath == "" {
		return fmt.Errorf("required: -db PATH")
	}

	// Open database file. Create if it doesn't exist.
	dbFile, err := os.OpenFile(*dbPath, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		return err
	}
	defer func() { _ = dbFile.Close() }()

	// Apply LTX files in order.
	for _, filename := range fs.Args() {
		if err := c.applyLTXFile(ctx, dbFile, filename); err != nil {
			return fmt.Errorf("%s: %s", filename, err)
		}
	}

	// Sync and close resulting database file.
	if err := dbFile.Sync(); err != nil {
		return err
	}
	return dbFile.Close()
}

func (c *ApplyCommand) applyLTXFile(_ context.Context, dbFile *os.File, filename string) error {
	ltxFile, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer func() { _ = ltxFile.Close() }()

	// Read LTX header and verify initial checksum matches.
	dec := ltx.NewDecoder(ltxFile)
	if err := dec.DecodeHeader(); err != nil {
		return fmt.Errorf("decode ltx header: %w", err)
	}

	// Read checksum before applying.
	if _, err := dbFile.Seek(0, io.SeekStart); err != nil {
		return err
	}
	preApplyChecksum, err := ltx.ChecksumReader(dbFile, int(dec.Header().PageSize))
	if err != nil {
		return fmt.Errorf("compute pre-apply checksum: %w", err)
	} else if preApplyChecksum != dec.Header().PreApplyChecksum {
		return fmt.Errorf("pre-apply checksum mismatch: %s <> %s", preApplyChecksum, dec.Header().PreApplyChecksum)
	}

	// Apply each page to the database.
	data := make([]byte, dec.Header().PageSize)
	for {
		var pageHeader ltx.PageHeader
		if err := dec.DecodePage(&pageHeader, data); err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("decode ltx page: %w", err)
		}

		offset := int64(pageHeader.Pgno-1) * int64(dec.Header().PageSize)
		if _, err := dbFile.WriteAt(data, offset); err != nil {
			return fmt.Errorf("write database page: %w", err)
		}
	}

	// Close & verify file, print trailer.
	if err := dec.Close(); err != nil {
		return fmt.Errorf("close ltx file: %w", err)
	}

	// Recalculate database checksum and ensure it matches the LTX checksum.
	if _, err := dbFile.Seek(0, io.SeekStart); err != nil {
		return err
	}
	postApplyChecksum, err := ltx.ChecksumReader(dbFile, int(dec.Header().PageSize))
	if err != nil {
		return fmt.Errorf("compute post-apply checksum: %w", err)
	} else if postApplyChecksum != dec.Trailer().PostApplyChecksum {
		return fmt.Errorf("post-apply checksum mismatch: %s <> %s", postApplyChecksum, dec.Trailer().PostApplyChecksum)
	}

	return nil
}
