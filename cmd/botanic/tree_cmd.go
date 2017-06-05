package main

import "github.com/spf13/cobra"

func treeCmd(config *rootCmdConfig) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "tree",
		Short: "Manage regression trees",
		Long:  `Grow and test regression trees and use them to predict values for samples`,
	}
	cmd.AddCommand(growCmd(config), testCmd(config), predictCmd(config))
	return cmd
}
