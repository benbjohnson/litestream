//go:build windows

package main

import (
	"os"
	"os/exec"
	"os/signal"
)

func notifySignals() (<-chan os.Signal, func()) {
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, os.Interrupt)
	return ch, func() {
		signal.Stop(ch)
	}
}

func configureProcess(cmd *exec.Cmd) {
	cmd.Cancel = func() error { return signalProcess(cmd, os.Kill) }
}

func signalProcess(cmd *exec.Cmd, signal os.Signal) error {
	return cmd.Process.Signal(signal)
}
