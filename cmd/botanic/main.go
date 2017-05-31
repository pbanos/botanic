package main

import (
	"os"

	"github.com/spf13/cobra"
)

type rootCmdConfig struct {
	verbose bool
}

func main() {
	if err := cliParser().Execute(); err != nil {
		os.Exit(1)
	}
}

func cliParser() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "botanic",
		Short: "botanic is a tool to perform tree-regression",
		Long:  `A tool to grow regression trees from your data, test them, and use them to make predictions`,
	}
	config := &rootCmdConfig{}
	rootCmd.PersistentFlags().BoolVarP(&(config.verbose), "verbose", "v", false, "")
	rootCmd.AddCommand(versionCmd(), growCmd(config), testCmd(config))
	return rootCmd
}
