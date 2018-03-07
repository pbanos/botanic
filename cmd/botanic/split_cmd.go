package main

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/pbanos/botanic/dataset"
	"github.com/pbanos/botanic/dataset/csv"
	"github.com/pbanos/botanic/dataset/dbdataset"
	"github.com/pbanos/botanic/dataset/dbdataset/pgadapter"
	"github.com/pbanos/botanic/dataset/dbdataset/sqlite3adapter"
	"github.com/pbanos/botanic/feature"
	"github.com/pbanos/botanic/feature/yaml"
	"github.com/spf13/cobra"
)

type splitCmdConfig struct {
	*datasetCmdConfig
	splitOutput      string
	splitProbability int
}

func splitCmd(datasetConfig *datasetCmdConfig) *cobra.Command {
	config := &splitCmdConfig{datasetCmdConfig: datasetConfig}
	cmd := &cobra.Command{
		Use:   "split",
		Short: "Split a dataset into two datasets",
		Long:  `Split a dataset into an output dataset and a split dataset`,
		Run: func(cmd *cobra.Command, args []string) {
			err := datasetConfig.Validate()
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			err = config.Validate()
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			config.Context()
			config.Logf("Reading features from metadata at %s...", datasetConfig.metadataInput)
			features, err := yaml.ReadFeaturesFromFile(datasetConfig.metadataInput)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(2)
			}
			config.Logf("Features from metadata read")

			output, err := config.OutputWriter(features)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(4)
			}

			splitOutput, err := config.SplitOutputWriter(features)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(6)
			}

			inputStream, errStream, err := datasetConfig.InputStream(features)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(7)
			}

			randomizer := rand.New(rand.NewSource(time.Now().UnixNano()))
			var outputCount, splitCount int
			for s := range inputStream {
				var n int
				if (100 * randomizer.Float32()) > float32(config.splitProbability) {
					n, err = output.Write(config.Context(), []dataset.Sample{s})
					outputCount += n
				} else {
					n, err = splitOutput.Write(config.Context(), []dataset.Sample{s})
					splitCount += n
				}
				if err != nil {
					config.ContextCancelFunc()
					break
				}
			}
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(8)
			}
			err = <-errStream
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(9)
			}

			config.Logf("Flushing output dataset...")
			err = output.Flush()
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(10)
			}
			config.Logf("Flushing split dataset...")
			err = splitOutput.Flush()
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(11)
			}
			config.Logf("Done")
			config.Logf("Input dataset with %d samples was split into datasets with %d and %d samples", outputCount+splitCount, outputCount, splitCount)
		},
	}
	cmd.PersistentFlags().IntVarP(&(config.splitProbability), "split-probability", "p", 20, "probability as percent integer that a sample of the dataset will be assigned to the split dataset")
	cmd.PersistentFlags().StringVarP(&(config.splitOutput), "split-output", "s", "", "path to a CSV (.csv) or SQLite3 (.db) file, or a PostgreSQL DB connection URL to dump the output of the split dataset (required)")
	return cmd
}

func (scc *splitCmdConfig) SplitOutputWriter(features []feature.Feature) (writableSet, error) {
	var splitOutputFile *os.File
	if strings.HasPrefix(scc.splitOutput, "postgresql://") {
		return scc.PostgreSQLSplitOutputWriter(features)
	}
	if strings.HasSuffix(scc.splitOutput, ".db") {
		return scc.Sqlite3SplitOutputWriter(features)
	}
	scc.Logf("Creating %s to dump split dataset...", scc.splitOutput)
	splitOutputFile, err := os.Create(scc.splitOutput)
	if err != nil {
		return nil, err
	}
	scc.Logf("Preparing to write split output dataset...")
	splitOutput, err := csv.NewWriter(splitOutputFile, features)
	if err != nil {
		return nil, err
	}
	return splitOutput, nil
}

func (scc *splitCmdConfig) Validate() error {
	if scc.splitOutput == "" {
		return fmt.Errorf("required split-output flag was not set")
	}
	if scc.splitProbability <= 0 || scc.splitProbability > 100 {
		return fmt.Errorf("split-percent flag was set to an invalid value: it must be set to an integer between 1 and 100")
	}
	return nil
}

func (scc *splitCmdConfig) Sqlite3SplitOutputWriter(features []feature.Feature) (writableSet, error) {
	scc.Logf("Creating SQLite3 adapter for file %s to dump split dataset...", scc.splitOutput)
	adapter, err := sqlite3adapter.New(scc.splitOutput, 0)
	if err != nil {
		return nil, err
	}
	scc.Logf("Opening dataset over SQLite3 adapter for file %s to dump split dataset...", scc.splitOutput)
	dataset, err := dbdataset.Create(scc.Context(), adapter, features)
	if err != nil {
		return nil, err
	}
	return &flushableSampleWriter{dataset}, nil
}

func (scc *splitCmdConfig) PostgreSQLSplitOutputWriter(features []feature.Feature) (writableSet, error) {
	scc.Logf("Creating PostgreSQL adapter for url %s to dump split dataset...", scc.splitOutput)
	adapter, err := pgadapter.New(scc.splitOutput)
	if err != nil {
		return nil, err
	}
	scc.Logf("Opening dataset over PostgreSQL adapter for url %s to dump split dataset...", scc.splitOutput)
	dataset, err := dbdataset.Create(scc.Context(), adapter, features)
	if err != nil {
		return nil, err
	}
	return &flushableSampleWriter{dataset}, nil
}
