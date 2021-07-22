// +build !windows

package main

import (
	"context"
	"os"
	"syscall"
)

const defaultConfigPath = "/etc/litestream.yml"

func isWindowsService() (bool, error) {
	return false, nil
}

func runWindowsService(ctx context.Context) error {
	panic("cannot run windows service as unix process")
}

var notifySignals = []os.Signal{syscall.SIGINT, syscall.SIGTERM}
