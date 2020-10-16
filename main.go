package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

func main() {
	m := NewMain()
	if err := m.Run(os.Args[1:]); err == flag.ErrHelp {
		os.Exit(1)
	} else if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type Main struct {
	logger *log.Logger

	SourcePath string
	MountPath  string
}

func NewMain() *Main {
	return &Main{
		logger: log.New(ioutil.Discard, "", log.LstdFlags),
	}
}

func (m *Main) Run(args []string) (err error) {
	flagSet := flag.NewFlagSet("litestream", flag.ContinueOnError)
	verbose := flagSet.Bool("v", false, "verbose")
	flagSet.Usage = m.usage
	if err := flagSet.Parse(args); err != nil {
		return err
	}
	if m.SourcePath = flagSet.Arg(0); m.SourcePath == "" {
		return errors.New("source path required")
	} else if m.MountPath = flagSet.Arg(1); m.MountPath == "" {
		return errors.New("mount path required")
	}

	// Setup logging, if verbose specified.
	if *verbose {
		m.logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	// Mount FUSE filesystem.
	conn, err := fuse.Mount(m.MountPath, fuse.FSName("litestream"), fuse.Subtype("litestreamfs"))
	if err != nil {
		return err
	}
	defer fuse.Unmount(m.MountPath)
	defer conn.Close()

	m.logger.Printf("mounted")

	s := fs.New(conn, &fs.Config{
		Debug: debug,
	})
	return s.Serve(&FS{SourcePath: m.SourcePath})
}

func (m *Main) usage() {
	fmt.Println(`
Litestream is a FUSE file system that automatically replicates SQLite databases.

Usage:

	litestream [arguments] source_dir mount_dir

Arguments:

	-v
	    Enable verbose logging.

`[1:])
}

// debug is a function that can be used for fs.Config.Debug.
// It marshals the msg to JSON and prints to the log.
func debug(msg interface{}) {
	buf, err := json.Marshal(msg)
	if err != nil {
		println("debug: marshal error: %v", err)
		return
	}
	log.Print("DEBUG ", string(buf))
}
