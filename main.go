package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/gridsx/datagos/server"
)

func main() {

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, os.Kill)

	server.Serve()
	select {
	case sig := <-c:
		fmt.Printf("Got %s signal. Aborting...", sig)
	}
}
