package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
)

// Build information.
var (
	Version = "(development build)"
)

func main() {
	log.SetFlags(0)

	m := NewMain()
	if err := m.Run(context.Background(), os.Args[1:]); err == flag.ErrHelp {
		os.Exit(1)
	} else if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type Main struct{}

func NewMain() *Main {
	return &Main{}
}

func (m *Main) Run(ctx context.Context, args []string) (err error) {
	var cmd string
	if len(args) > 0 {
		cmd, args = args[0], args[1:]
	}

	switch cmd {
	case "replicate":
		return (&ReplicateCommand{}).Run(ctx, args)
	case "version":
		return (&VersionCommand{}).Run(ctx, args)
	default:
		if cmd == "" || cmd == "help" || strings.HasPrefix(cmd, "-") {
			m.Usage()
			return flag.ErrHelp
		}
		return fmt.Errorf("litestream %s: unknown command", cmd)
	}
}

func (m *Main) Usage() {
	fmt.Println(`
litestream is a tool for replicating SQLite databases.

Usage:

	litestream <command> [arguments]

The commands are:

	replicate   runs a server to replicate databases
	version     prints the version
`[1:])
}
