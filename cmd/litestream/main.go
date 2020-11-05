package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/benbjohnson/litestream"
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

	TargetPath string
	Path       string
}

func NewMain() *Main {
	return &Main{
		logger: log.New(ioutil.Discard, "", log.LstdFlags),
	}
}

func (m *Main) Run(args []string) (err error) {
	flagSet := flag.NewFlagSet("litestream", flag.ContinueOnError)
	flagSet.StringVar(&m.TargetPath, "target", "", "target directory")
	verbose := flagSet.Bool("v", false, "verbose")
	flagSet.Usage = m.usage
	if err := flagSet.Parse(args); err != nil {
		return err
	}

	// Ensure mount path is specified.
	if flagSet.NArg() > 1 {
		return errors.New("too many arguments, only specify mount path")
	} else if m.Path = flagSet.Arg(0); m.Path == "" {
		return errors.New("mount path required")
	}

	// Ensure mount path exists & is a directory.
	if fi, err := os.Stat(m.Path); err != nil {
		return err
	} else if !fi.IsDir() {
		return fmt.Errorf("mount path must be a directory")
	}

	// If no target is specified, default to a hidden directory based on the mount path.
	if m.TargetPath == "" {
		m.TargetPath = filepath.Join(filepath.Dir(m.Path), "."+filepath.Base(m.Path))

		if err := m.ensureTargetPath(); err != nil {
			return err
		}
	}

	// Setup logging, if verbose specified.
	var config fs.Config
	if *verbose {
		config.Debug = debug
		m.logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	// Mount FUSE filesystem.
	conn, err := fuse.Mount(m.Path)
	if err != nil {
		return err
	}
	defer fuse.Unmount(m.Path)
	defer conn.Close()

	m.logger.Printf("mounted %s; target=%s", m.Path, m.TargetPath)

	fileSystem := litestream.NewFileSystem(m.TargetPath)
	if err := fileSystem.Open(); err != nil {
		return err
	}
	defer fileSystem.Close()

	s := fs.New(conn, &config)
	return s.Serve(fileSystem)
}

func (m *Main) ensureTargetPath() error {
	// Check if target path exists, exit if it does.
	if _, err := os.Stat(m.TargetPath); err == nil {
		return nil
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}

	// Create target path with the same permissions as the mount path.
	fi, err := os.Stat(m.Path)
	if err != nil {
		return err
	}
	return os.Mkdir(m.TargetPath, fi.Mode())
}

func (m *Main) usage() {
	fmt.Println(`
Litestream is a FUSE file system that replicates SQLite databases.

Usage:

	litestream [arguments] PATH

Arguments:

	-target PATH
	    Specifies the directory to store data.
	    Defaults to a hidden directory next to PATH.
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
