package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"text/tabwriter"
	"time"

	"github.com/superfly/ltx"
)

// ListCommand represents a command to print the header/trailer of one or more
// LTX files in a table.
type ListCommand struct{}

// NewListCommand returns a new instance of ListCommand.
func NewListCommand() *ListCommand {
	return &ListCommand{}
}

// Run executes the command.
func (c *ListCommand) Run(ctx context.Context, args []string) (ret error) {
	fs := flag.NewFlagSet("ltx-list", flag.ContinueOnError)
	tsv := fs.Bool("tsv", false, "output as tab-separated values")
	fs.Usage = func() {
		fmt.Println(`
The list command lists header & trailer information for a set of LTX files.

Usage:

	ltx list [arguments] PATH [PATH...]

Arguments:
`[1:])
		fs.PrintDefaults()
		fmt.Println()
	}
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() == 0 {
		return fmt.Errorf("at least one LTX file is required")
	}

	var w io.Writer = os.Stdout
	if !*tsv {
		tw := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
		defer func() { _ = tw.Flush() }()
		w = tw
	}

	_, _ = fmt.Fprintln(w, "min_txid\tmax_txid\tcommit\tpages\tpreapply\tpostapply\ttimestamp\twal_offset\twal_size\twal_salt")
	for _, arg := range fs.Args() {
		if err := c.printFile(w, arg); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "%s: %s\n", arg, err)
		}
	}

	return nil
}

func (c *ListCommand) printFile(w io.Writer, filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	dec := ltx.NewDecoder(f)
	if err := dec.Verify(); err != nil {
		return err
	}

	// Only show timestamp if it is actually set.
	timestamp := time.UnixMilli(dec.Header().Timestamp).UTC().Format(time.RFC3339)
	if dec.Header().Timestamp == 0 {
		timestamp = ""
	}

	_, _ = fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%s\t%s\t%s\t%d\t%d\t%08x %08x\n",
		dec.Header().MinTXID.String(),
		dec.Header().MaxTXID.String(),
		dec.Header().Commit,
		dec.PageN(),
		dec.Header().PreApplyChecksum,
		dec.Trailer().PostApplyChecksum,
		timestamp,
		dec.Header().WALOffset,
		dec.Header().WALSize,
		dec.Header().WALSalt1, dec.Header().WALSalt2,
	)

	return nil
}
