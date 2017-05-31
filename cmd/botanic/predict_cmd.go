package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

func predictCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "predict",
		Short: "Print the version number of botanic",
		Long:  `All software has versions. This is botanic's`,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("botanic v%d.%d.%d\n", VersionMajor, VersionMinor, VersionPatch)
		},
	}
}
