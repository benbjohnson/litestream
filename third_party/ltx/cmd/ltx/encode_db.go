package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/superfly/ltx"
)

const (
	SQLITE_DATABASE_HEADER_STRING = "SQLite format 3\x00"
	SQLITE_DATABASE_HEADER_SIZE   = 100
)

// EncodeDBCommand represents a command to encode an SQLite database file as a single LTX file.
type EncodeDBCommand struct{}

// NewEncodeDBCommand returns a new instance of EncodeDBCommand.
func NewEncodeDBCommand() *EncodeDBCommand {
	return &EncodeDBCommand{}
}

// Run executes the command.
func (c *EncodeDBCommand) Run(ctx context.Context, args []string) (ret error) {
	fs := flag.NewFlagSet("ltx-encode-db", flag.ContinueOnError)
	outPath := fs.String("o", "", "output path")
	fs.Usage = func() {
		fmt.Println(`
The encode-db command encodes an SQLite database into an LTX file.

Usage:

	ltx encode-db [arguments] PATH

Arguments:
`[1:])
		fs.PrintDefaults()
		fmt.Println()
	}
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() == 0 {
		return fmt.Errorf("filename required")
	} else if fs.NArg() > 1 {
		return fmt.Errorf("too many arguments")
	} else if *outPath == "" {
		return fmt.Errorf("required: -o PATH")
	}

	db, err := os.Open(fs.Arg(0))
	if err != nil {
		return fmt.Errorf("open DB file: %w", err)
	}
	defer func() { _ = db.Close() }()

	out, err := os.OpenFile(*outPath, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open output file: %w", err)
	}
	defer func() { _ = out.Close() }()

	rd, hdr, err := c.readSQLiteDatabaseHeader(db)
	if err != nil {
		return fmt.Errorf("read database header: %w", err)
	}

	var flags uint32
	var postApplyChecksum ltx.Checksum
	enc, err := ltx.NewEncoder(out)
	if err != nil {
		return fmt.Errorf("create ltx encoder: %w", err)
	}
	if err := enc.EncodeHeader(ltx.Header{
		Version:   1,
		Flags:     flags,
		PageSize:  hdr.pageSize,
		Commit:    hdr.pageN,
		MinTXID:   ltx.TXID(1),
		MaxTXID:   ltx.TXID(1),
		Timestamp: time.Now().UnixMilli(),
	}); err != nil {
		return fmt.Errorf("encode ltx header: %w", err)
	}

	buf := make([]byte, hdr.pageSize)
	for pgno := uint32(1); pgno <= hdr.pageN; pgno++ {
		if _, err := io.ReadFull(rd, buf); err != nil {
			return fmt.Errorf("read page %d: %w", pgno, err)
		}

		if pgno == ltx.LockPgno(hdr.pageSize) {
			continue
		}

		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, buf); err != nil {
			return fmt.Errorf("encode page %d: %w", pgno, err)
		}

		postApplyChecksum = ltx.ChecksumFlag | (postApplyChecksum ^ ltx.ChecksumPage(pgno, buf))
	}

	enc.SetPostApplyChecksum(postApplyChecksum)
	if err := enc.Close(); err != nil {
		return fmt.Errorf("close ltx encoder: %w", err)
	} else if err := out.Sync(); err != nil {
		return fmt.Errorf("sync ltx file: %w", err)
	} else if err := out.Close(); err != nil {
		return fmt.Errorf("close ltx file: %w", err)
	}

	return nil
}

type sqliteDatabaseHeader struct {
	pageSize uint32
	pageN    uint32
}

func (c *EncodeDBCommand) readSQLiteDatabaseHeader(rd io.Reader) (ord io.Reader, hdr sqliteDatabaseHeader, err error) {
	b := make([]byte, SQLITE_DATABASE_HEADER_SIZE)
	if _, err := io.ReadFull(rd, b); err == io.ErrUnexpectedEOF {
		return ord, hdr, fmt.Errorf("invalid database header")
	} else if err == io.EOF {
		return ord, hdr, fmt.Errorf("empty database")
	} else if err != nil {
		return ord, hdr, err
	} else if !bytes.Equal(b[:len(SQLITE_DATABASE_HEADER_STRING)], []byte(SQLITE_DATABASE_HEADER_STRING)) {
		return ord, hdr, fmt.Errorf("invalid database header")
	}

	hdr.pageSize = uint32(binary.BigEndian.Uint16(b[16:]))
	hdr.pageN = binary.BigEndian.Uint32(b[28:])
	if hdr.pageSize == 1 {
		hdr.pageSize = 65536
	}

	ord = io.MultiReader(bytes.NewReader(b), rd)

	return ord, hdr, nil
}
