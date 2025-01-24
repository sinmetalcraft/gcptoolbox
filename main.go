package main

import (
	"context"
	"fmt"
	"os"

	"github.com/sinmetalcraft/gcptoolbox/cmd"
	"github.com/sinmetalcraft/gcptoolbox/cmd/server"
)

func main() {
	ctx := context.Background()

	port := os.Getenv("PORT")
	cmdMode := os.Getenv("GCPTOOLBOX_CMD_MODE")
	if port != "" && cmdMode != "true" {
		if err := server.Run(ctx, port); err != nil {
			fmt.Fprintf(os.Stderr, "failed run server. %v\n", err)
			os.Exit(1)
		}
	}

	if err := cmd.RootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", os.Args[0], err)
		os.Exit(1)
	}
	os.Exit(0)
}
