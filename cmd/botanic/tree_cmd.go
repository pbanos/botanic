package main

import (
	"context"
	"fmt"
	"os"

	"github.com/pbanos/botanic/feature"
	"github.com/pbanos/botanic/feature/yaml"
	"github.com/pbanos/botanic/tree"
	"github.com/pbanos/botanic/tree/json"
	"github.com/spf13/cobra"
)

type treeCmdConfig struct {
	*rootCmdConfig
	treeInput     string
	metadataInput string
	ctx           context.Context
	cancelFunc    context.CancelFunc
}

func treeCmd(rootConfig *rootCmdConfig) *cobra.Command {
	config := &treeCmdConfig{rootCmdConfig: rootConfig}
	cmd := &cobra.Command{
		Use:   "tree",
		Short: "Manage regression trees",
		Long:  `Manage regression trees and use them to predict values for samples`,
		Run: func(cmd *cobra.Command, args []string) {
			err := config.Validate()
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			config.Context()
			config.Logf("Reading features from metadata at %s...", config.metadataInput)
			features, err := yaml.ReadFeaturesFromFile(config.metadataInput)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(2)
			}
			config.Logf("Features from metadata read")
			tree, err := loadTree(context.Background(), config.treeInput, features)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(3)
			}
			fmt.Println(tree)
		},
	}
	cmd.PersistentFlags().StringVarP(&(config.metadataInput), "metadata", "m", "", "path to a YML file with metadata describing the different features used on a tree or available on an input set (required)")
	cmd.AddCommand(growCmd(config), testCmd(config), predictCmd(config))
	cmd.Flags().StringVarP(&(config.treeInput), "tree", "t", "", "path to a file from which the tree to show will be read and parsed as JSON (required)")
	return cmd
}

func (tcc *treeCmdConfig) Validate() error {
	if tcc.treeInput == "" {
		return fmt.Errorf("required tree flag was not set")
	}
	if tcc.metadataInput == "" {
		return fmt.Errorf("required metadata flag was not set")
	}
	return nil
}

func loadTree(ctx context.Context, filepath string, features []feature.Feature) (*tree.Tree, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("reading tree in JSON from %s: %v", filepath, err)
	}
	defer f.Close()
	t := &tree.Tree{NodeStore: tree.NewMemoryNodeStore()}
	err = json.ReadJSONTree(ctx, t, features, f)
	if err != nil {
		err = fmt.Errorf("parsing tree in JSON from %s: %v", filepath, err)
	}
	return t, err
}

func (tcc *treeCmdConfig) setContextAndCancelFunc() {
	if tcc.ctx == nil {
		tcc.ctx, tcc.cancelFunc = context.WithCancel(context.Background())
	}
}

func (tcc *treeCmdConfig) Context() context.Context {
	tcc.setContextAndCancelFunc()
	return tcc.ctx
}

func (tcc *treeCmdConfig) ContextCancelFunc() context.CancelFunc {
	tcc.setContextAndCancelFunc()
	return tcc.cancelFunc
}
