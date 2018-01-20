package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/pbanos/botanic/feature"
	"github.com/pbanos/botanic/feature/yaml"
	"github.com/pbanos/botanic/set"
	"github.com/pbanos/botanic/set/csv"
	"github.com/pbanos/botanic/set/sqlset"
	"github.com/pbanos/botanic/set/sqlset/pgadapter"
	"github.com/pbanos/botanic/set/sqlset/sqlite3adapter"
	"github.com/pbanos/botanic/tree"
	"github.com/pbanos/botanic/tree/json"
	"github.com/spf13/cobra"
)

type testCmdConfig struct {
	*rootCmdConfig
	treeInput     string
	dataInput     string
	metadataInput string
	classFeature  string
	ctx           context.Context
	cancelFunc    context.CancelFunc
}

func testCmd(rootConfig *rootCmdConfig) *cobra.Command {
	config := &testCmdConfig{rootCmdConfig: rootConfig}
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
				fmt.Fprintf(os.Stderr, "counting testing set samples: %v\n", err)
				os.Exit(5)
			}
			rootConfig.Logf("Testing tree against testset with %d samples...", count)
			successRate, errorCount, err := tree.Test(config.Context(), testingSet)
			if err != nil {
				fmt.Fprintf(os.Stderr, "testing tree: %v\n", err)
				os.Exit(6)
			}
			rootConfig.Logf("Done")
			fmt.Printf("%f success rate, failed to make a prediction for %d samples\n", successRate, errorCount)
		},
	}
	cmd.PersistentFlags().StringVarP(&(config.dataInput), "input", "i", "", "path to an input CSV (.csv) or SQLite3 (.db) file, or a PostgreSQL DB connection URL with data to use to grow the tree (defaults to STDIN, interpreted as CSV)")
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

func (tcc *testCmdConfig) testingSet(features []feature.Feature) (set.Set, error) {
	var f *os.File
	if tcc.dataInput == "" {
		tcc.Logf("Reading testing set from STDIN...")
		f = os.Stdin
	} else {
		if strings.HasPrefix(tcc.dataInput, "postgresql://") {
			return tcc.PostgreSQLTestingSet(features)
		}
		if strings.HasSuffix(tcc.dataInput, ".db") {
			return tcc.Sqlite3TestingSet(features)
		}
		tcc.Logf("Opening %s to read testing set...", tcc.dataInput)
		var err error
		f, err = os.Open(tcc.dataInput)
		if err != nil {
			err = fmt.Errorf("opening testing set at %s: %v", tcc.dataInput, err)
			return nil, err
		}
		defer f.Close()
	}
	testingSet, err := csv.ReadSet(f, features, set.New)
	if err != nil {
		return nil, fmt.Errorf("reading testing set: %v", err)
	}
	return testingSet, nil
}

func (tcc *testCmdConfig) Sqlite3TestingSet(features []feature.Feature) (set.Set, error) {
	tcc.Logf("Creating SQLite3 adapter for file %s to read testing set...", tcc.dataInput)
	adapter, err := sqlite3adapter.New(tcc.dataInput, 0)
	if err != nil {
		return nil, err
	}
	tcc.Logf("Opening set over SQLite3 adapter for file %s to read testing set...", tcc.dataInput)
	return sqlset.OpenSet(tcc.Context(), adapter, features)
}

func (tcc *testCmdConfig) PostgreSQLTestingSet(features []feature.Feature) (set.Set, error) {
	tcc.Logf("Creating PostgreSQL adapter for url %s to read testing set...", tcc.dataInput)
	adapter, err := pgadapter.New(tcc.dataInput)
	if err != nil {
		return nil, err
	}
	tcc.Logf("Opening set over PostgreSQL adapter for url %s to read testing set...", tcc.dataInput)
	return sqlset.OpenSet(tcc.Context(), adapter, features)
}

func (tcc *testCmdConfig) Context() context.Context {
	tcc.setContextAndCancelFunc()
	return tcc.ctx
}

func (tcc *testCmdConfig) ContextCancelFunc() context.CancelFunc {
	tcc.setContextAndCancelFunc()
	return tcc.cancelFunc
}

func (tcc *testCmdConfig) setContextAndCancelFunc() {
	if tcc.ctx == nil {
		tcc.ctx, tcc.cancelFunc = context.WithCancel(context.Background())
	}
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
