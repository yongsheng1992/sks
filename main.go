package main

import (
	"errors"
	"github.com/yongsheng1992/sks/server"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	proposeC := make(chan []byte)
	shutDownC := make(chan struct{})
	defer close(proposeC)

	errorC := make(chan error)
	go server.Serve(proposeC, shutDownC, errorC)

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGKILL, syscall.SIGTERM)

	<-ch

	errorC <- errors.New("killed")
	shutDownC <- struct{}{}
}
