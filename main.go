package main

import (
	"fmt"
	"os"

	"github.com/sinmetalcraft/gcptoolbox/cmd"
	"github.com/sinmetalcraft/gcptoolbox/cmd/server"
)

func main() {
	port := os.Getenv("PORT")
	cmdMode := os.Getenv("GCPTOOLBOX_CMD_MODE")
	if port != "" && cmdMode != "true" {
		server.Run(port)
	}

	if err := cmd.RootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", os.Args[0], err)
		os.Exit(1)
	}
	os.Exit(0)
}
