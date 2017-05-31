package main

import (
	"fmt"
	"os"

	"github.com/pbanos/botanic/pkg/bio"
	"github.com/pbanos/botanic/pkg/botanic"
	"github.com/spf13/cobra"
)

type testCmdConfig struct {
	treeInput     string
	dataInput     string
	metadataInput string
	classFeature  string
}

func testCmd(rootConfig *rootCmdConfig) *cobra.Command {
	config := &testCmdConfig{}
	cmd := &cobra.Command{
		Use:   "test",
		Short: "Test the performance of a tree",
		Long:  `Test the performance of a tree against a test data set`,
		Run: func(cmd *cobra.Command, args []string) {
			err := config.Validate()
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			l := logger(rootConfig.verbose)
			features, err := bio.ReadYMLFeaturesFromFile(config.metadataInput)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(2)
			}
			var f *os.File
			if config.dataInput == "" {
				f = os.Stdin
			} else {
				f, err = os.Open(config.dataInput)
				if err != nil {
					err = fmt.Errorf("reading training set from %s: %v", config.dataInput, err)
					fmt.Fprintln(os.Stderr, err)
					os.Exit(3)
				}
			}
			defer f.Close()
			testSet, err := bio.ReadCSVSet(f, features)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(4)
			}
			var classFeature botanic.Feature
			for i, f := range features {
				if f.Name() == config.classFeature {
					classFeature = f
					features[i], features[len(features)-1] = features[len(features)-1], features[i]
					break
				}
			}
			if classFeature == nil {
				fmt.Fprintf(os.Stderr, "class feature '%s' is not defined\n", config.classFeature)
				os.Exit(5)
			}
			tree, err := loadTree(config.treeInput)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(4)
			}
			l.Logf("Testing tree against testset with %d samples...", testSet.Count())
			successRate, errorCount := tree.Test(testSet, classFeature)
			l.Logf("Done")
			fmt.Printf("%f success rate, %d errors\n", successRate, errorCount)
		},
	}
	cmd.PersistentFlags().StringVarP(&(config.dataInput), "input", "i", "", "path to an input CSV file with data to use to grow the tree (defaults to STDIN)")
	cmd.PersistentFlags().StringVarP(&(config.metadataInput), "metadata", "m", "", "path to a YML file with metadata describing the different features available available on the input file (required)")
	cmd.PersistentFlags().StringVarP(&(config.treeInput), "tree", "t", "", "path to a file from which the tree to test will be read and parsed as JSON (required)")
	cmd.PersistentFlags().StringVarP(&(config.classFeature), "class-feature", "c", "", "name of the feature the generated tree should predict (required)")
	return cmd
}

func (tcc *testCmdConfig) Validate() error {
	if tcc.treeInput == "" {
		return fmt.Errorf("required tree flag was not set")
	}
	if tcc.metadataInput == "" {
		return fmt.Errorf("required metadata flag was not set")
	}
	if tcc.classFeature == "" {
		return fmt.Errorf("required class-feature flag was not set")
	}
	return nil
}

func loadTree(filepath string) (*botanic.Tree, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("reading tree in JSON from %s: %v", filepath, err)
	}
	defer f.Close()
	t, err := bio.ReadJSONTree(f)
	if err != nil {
		err = fmt.Errorf("parsing tree in JSON from %s: %v", filepath, err)
	}
	return t, err
}
