package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"path/filepath"

	"github.com/benbjohnson/litestream"
)

func main() {
	// Setup signal handler.
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() { <-c; cancel() }()

	// Initialize program and read flags/config.
	m := NewMain()
	if err := m.ParseFlags(os.Args[1:]); err == flag.ErrHelp {
		os.Exit(1)
	} else if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// Start monitoring databases.
	if err := m.Run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// Wait for signal to stop program.
	<-ctx.Done()
	signal.Reset()

	// Gracefully close
	if err := m.Close(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type Main struct {
	ConfigPath string
	Config     Config
}

func NewMain() *Main {
	return &Main{}
}

// ParseFlags parses the flag set from args & loads the configuration.
func (m *Main) ParseFlags(ctx context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("litestream", flag.ContinueOnError)
	fs.StringVar(&m.ConfigPath, "config", "", "configuration path")
	fs.Usage = m.usage
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Load configuration.
	if m.ConfigPath == "" {
		return errors.New("-config required")
	} else if m.Config, err = ReadConfigFile(m.ConfigPath); err != nil {
		return err
	}

	return nil
}

// Run loads all databases specified in the configuration.
func (m *Main) Run(ctx context.Context) (err error) {
	if len(m.Config.DBs) == 0 {
		return errors.New("configuration must specify at least one database")
	}

	for _, dbc := range m.Config.DBs {
		db := litestream.NewDB()
		db.Path = dbc.Path
		if err := db.Open(); err != nil {
			return err
		}
		m.DBs = append(m.DBs, db)
	}

	return nil
}

// Close closes all open databases.
func (m *Main) Close() (err error) {
	for _, db := range m.DBs {
		if e := db.Close(); e != nil {
			log.Printf("error closing db: path=%s err=%s", db.Path, e)
			if err == nil {
				err = e
			}
		}
	}
	return err
}

func (m *Main) Usage() {
	fmt.Println(`
Litestream is a daemon for replicating SQLite databases.

Usage:

	litestream [arguments]

Arguments:

	-config PATH
	    Specifies the configuration file. Required.

`[1:])
}
