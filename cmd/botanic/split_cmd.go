package main

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/pbanos/botanic/pkg/bio"
	"github.com/pbanos/botanic/pkg/bio/sql"
	"github.com/pbanos/botanic/pkg/bio/sql/pgadapter"
	"github.com/pbanos/botanic/pkg/bio/sql/sqlite3adapter"
	"github.com/pbanos/botanic/pkg/botanic"
	"github.com/spf13/cobra"
)

type splitCmdConfig struct {
	*setCmdConfig
	splitOutput      string
	splitProbability int
}

func splitCmd(setConfig *setCmdConfig) *cobra.Command {
	config := &splitCmdConfig{setCmdConfig: setConfig}
	cmd := &cobra.Command{
		Use:   "split",
		Short: "Split a set into two sets",
		Long:  `Split a set into an output set and a split set`,
		Run: func(cmd *cobra.Command, args []string) {
			err := setConfig.Validate()
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
			config.Logf("Reading features from metadata at %s...", setConfig.metadataInput)
			features, err := bio.ReadYMLFeaturesFromFile(setConfig.metadataInput)
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

			inputStream, errStream, err := setConfig.InputStream(features)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(7)
			}

			randomizer := rand.New(rand.NewSource(time.Now().UnixNano()))
			var outputCount, splitCount int
			for s := range inputStream {
				var n int
				if (100 * randomizer.Float32()) > float32(config.splitProbability) {
					n, err = output.Write(config.Context(), []botanic.Sample{s})
					outputCount += n
				} else {
					n, err = splitOutput.Write(config.Context(), []botanic.Sample{s})
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

			config.Logf("Flushing output set...")
			err = output.Flush()
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(10)
			}
			config.Logf("Flushing split set...")
			err = splitOutput.Flush()
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(11)
			}
			config.Logf("Done")
			config.Logf("Input set with %d samples was split into sets with %d and %d samples", outputCount+splitCount, outputCount, splitCount)
		},
	}
	cmd.PersistentFlags().IntVarP(&(config.splitProbability), "split-probability", "p", 20, "probability as percent integer that a sample of the set will be assigned to the split set")
	cmd.PersistentFlags().StringVarP(&(config.splitOutput), "split-output", "s", "", "path to a CSV (.csv) or SQLite3 (.db) file, or a PostgreSQL DB connection URL to dump the output of the split set (required)")
	return cmd
}

func (scc *splitCmdConfig) SplitOutputWriter(features []botanic.Feature) (writableSet, error) {
	var splitOutputFile *os.File
	if strings.HasPrefix(scc.splitOutput, "postgresql://") {
		return scc.PostgreSQLSplitOutputWriter(features)
	}
	if strings.HasSuffix(scc.splitOutput, ".db") {
		return scc.Sqlite3SplitOutputWriter(features)
	}
	scc.Logf("Creating %s to dump split set...", scc.splitOutput)
	splitOutputFile, err := os.Create(scc.splitOutput)
	if err != nil {
		return nil, err
	}
	scc.Logf("Preparing to write split output set...")
	splitOutput, err := bio.NewCSVWriter(splitOutputFile, features)
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

func (scc *splitCmdConfig) Sqlite3SplitOutputWriter(features []botanic.Feature) (writableSet, error) {
	scc.Logf("Creating SQLite3 adapter for file %s to dump split set...", scc.splitOutput)
	adapter, err := sqlite3adapter.New(scc.splitOutput, 0)
	if err != nil {
		return nil, err
	}
	scc.Logf("Opening set over SQLite3 adapter for file %s to dump split set...", scc.splitOutput)
	set, err := sql.CreateSet(scc.Context(), adapter, features)
	if err != nil {
		return nil, err
	}
	return &flushableSampleWriter{set}, nil
}

func (scc *splitCmdConfig) PostgreSQLSplitOutputWriter(features []botanic.Feature) (writableSet, error) {
	scc.Logf("Creating PostgreSQL adapter for url %s to dump split set...", scc.splitOutput)
	adapter, err := pgadapter.New(scc.splitOutput)
	if err != nil {
		return nil, err
	}
	scc.Logf("Opening set over PostgreSQL adapter for url %s to dump split set...", scc.splitOutput)
	set, err := sql.CreateSet(scc.Context(), adapter, features)
	if err != nil {
		return nil, err
	}
	return &flushableSampleWriter{set}, nil
}
