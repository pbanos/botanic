package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/pbanos/botanic/dataset"
	"github.com/pbanos/botanic/dataset/csv"
	"github.com/pbanos/botanic/dataset/dbdataset"
	"github.com/pbanos/botanic/dataset/dbdataset/pgadapter"
	"github.com/pbanos/botanic/dataset/dbdataset/sqlite3adapter"
	"github.com/pbanos/botanic/feature"
	"github.com/pbanos/botanic/feature/yaml"
	"github.com/spf13/cobra"
)

type testCmdConfig struct {
	*treeCmdConfig
	dataInput string
}

func testCmd(treeConfig *treeCmdConfig) *cobra.Command {
	config := &testCmdConfig{treeCmdConfig: treeConfig}
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
			config.Context()
			features, err := yaml.ReadFeaturesFromFile(config.metadataInput)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(2)
			}

			testingSet, err := config.testingSet(features)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(4)
			}
			tree, err := loadTree(context.Background(), config.treeInput, features)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(4)
			}
			count, err := testingSet.Count(config.Context())
			if err != nil {
				fmt.Fprintf(os.Stderr, "counting testing dataset samples: %v\n", err)
				os.Exit(5)
			}
			config.Logf("Testing tree against testdataset with %d samples...", count)
			successRate, errorCount, err := tree.Test(config.Context(), testingSet)
			if err != nil {
				fmt.Fprintf(os.Stderr, "testing tree: %v\n", err)
				os.Exit(6)
			}
			config.Logf("Done")
			fmt.Printf("%f success rate, failed to make a prediction for %d samples\n", successRate, errorCount)
		},
	}
	cmd.PersistentFlags().StringVarP(&(config.dataInput), "input", "i", "", "path to an input CSV (.csv) or SQLite3 (.db) file, or a PostgreSQL DB connection URL with data to use to grow the tree (defaults to STDIN, interpreted as CSV)")
	cmd.PersistentFlags().StringVarP(&(config.treeInput), "tree", "t", "", "path to a file from which the tree to test will be read and parsed as JSON (required)")
	return cmd
}

func (tcc *testCmdConfig) Validate() error {
	if tcc.treeInput == "" {
		return fmt.Errorf("required tree flag was not set")
	}
	if tcc.metadataInput == "" {
		return fmt.Errorf("required metadata flag was not set")
	}
	return nil
}

func (tcc *testCmdConfig) testingSet(features []feature.Feature) (dataset.Dataset, error) {
	var f *os.File
	if tcc.dataInput == "" {
		tcc.Logf("Reading testing dataset from STDIN...")
		f = os.Stdin
	} else {
		if strings.HasPrefix(tcc.dataInput, "postgresql://") {
			return tcc.PostgreSQLTestingSet(features)
		}
		if strings.HasSuffix(tcc.dataInput, ".db") {
			return tcc.Sqlite3TestingSet(features)
		}
		tcc.Logf("Opening %s to read testing dataset...", tcc.dataInput)
		var err error
		f, err = os.Open(tcc.dataInput)
		if err != nil {
			err = fmt.Errorf("opening testing dataset at %s: %v", tcc.dataInput, err)
			return nil, err
		}
		defer f.Close()
	}
	testingSet, err := csv.ReadSet(f, features, dataset.New)
	if err != nil {
		return nil, fmt.Errorf("reading testing dataset: %v", err)
	}
	return testingSet, nil
}

func (tcc *testCmdConfig) Sqlite3TestingSet(features []feature.Feature) (dataset.Dataset, error) {
	tcc.Logf("Creating SQLite3 adapter for file %s to read testing dataset...", tcc.dataInput)
	adapter, err := sqlite3adapter.New(tcc.dataInput, 0)
	if err != nil {
		return nil, err
	}
	tcc.Logf("Opening dataset over SQLite3 adapter for file %s to read testing dataset...", tcc.dataInput)
	return dbdataset.Open(tcc.Context(), adapter, features)
}

func (tcc *testCmdConfig) PostgreSQLTestingSet(features []feature.Feature) (dataset.Dataset, error) {
	tcc.Logf("Creating PostgreSQL adapter for url %s to read testing dataset...", tcc.dataInput)
	adapter, err := pgadapter.New(tcc.dataInput)
	if err != nil {
		return nil, err
	}
	tcc.Logf("Opening dataset over PostgreSQL adapter for url %s to read testing dataset...", tcc.dataInput)
	return dbdataset.Open(tcc.Context(), adapter, features)
}
