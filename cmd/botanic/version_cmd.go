package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

const (
	// VersionMajor is the major number in botanic's version
	VersionMajor = 0
	// VersionMinor is the minor number in botanic's version
	VersionMinor = 0
	// VersionPatch is the patch number in botanic's version
	VersionPatch = 1
)

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the version number of botanic",
		Long:  `All software has versions. This is botanic's`,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("botanic v%d.%d.%d\n", VersionMajor, VersionMinor, VersionPatch)
		},
	}
}
