//go:build windows

package main

import (
	"os"
	"os/signal"
)

func notifySignals() (<-chan os.Signal, func()) {
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, os.Interrupt)
	return ch, func() {
		signal.Stop(ch)
	}
}
