package main

import "github.com/spf13/cobra"

func setCmd(config *rootCmdConfig) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set",
		Short: "Manage sets of data",
		Long:  `Manage sets of data`,
	}
	cmd.AddCommand(splitCmd(config))
	return cmd
}
