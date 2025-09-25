package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"strings"
)

var (
	Version = "development"
	Commit  = ""
)

func main() {
	m := NewMain()
	if err := m.Run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type Main struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

func NewMain() *Main {
	return &Main{
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

func (m *Main) Run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("litestream-test", flag.ExitOnError)
	fs.Usage = m.Usage
	if err := fs.Parse(args); err != nil {
		return err
	}

	if fs.NArg() == 0 || fs.Arg(0) == "help" {
		m.Usage()
		return nil
	}

	switch fs.Arg(0) {
	case "populate":
		return (&PopulateCommand{Main: m}).Run(ctx, fs.Args()[1:])
	case "load":
		return (&LoadCommand{Main: m}).Run(ctx, fs.Args()[1:])
	case "shrink":
		return (&ShrinkCommand{Main: m}).Run(ctx, fs.Args()[1:])
	case "validate":
		return (&ValidateCommand{Main: m}).Run(ctx, fs.Args()[1:])
	case "version":
		return (&VersionCommand{Main: m}).Run(ctx, fs.Args()[1:])
	default:
		return fmt.Errorf("unknown command: %s", fs.Arg(0))
	}
}

func (m *Main) Usage() {
	fmt.Fprintln(m.Stdout, `
litestream-test is a testing harness for Litestream database replication.

Usage:

	litestream-test <command> [arguments]

Commands:

	populate    Quickly populate a database to a target size
	load        Generate continuous load on a database
	shrink      Shrink a database by deleting data
	validate    Validate replication integrity
	version     Show version information

Use "litestream-test <command> -h" for more information about a command.
`[1:])
}

type VersionCommand struct {
	Main *Main
}

func (c *VersionCommand) Run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("litestream-test version", flag.ExitOnError)
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	}

	fmt.Fprintf(c.Main.Stdout, "litestream-test %s\n", Version)
	if Commit != "" {
		fmt.Fprintf(c.Main.Stdout, "commit: %s\n", Commit)
	}
	fmt.Fprintf(c.Main.Stdout, "go: %s\n", strings.TrimPrefix(runtime.Version(), "go"))
	return nil
}

func (c *VersionCommand) Usage() {
	fmt.Fprintln(c.Main.Stdout, `
Show version information for litestream-test.

Usage:

	litestream-test version
`[1:])
}

func init() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)
}
