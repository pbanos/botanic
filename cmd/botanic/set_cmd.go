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
	"github.com/spf13/cobra"
)

type setCmdConfig struct {
	*rootCmdConfig
	setInput      string
	metadataInput string
	setOutput     string
	ctx           context.Context
	cancelFunc    context.CancelFunc
}

type sampleWriter interface {
	Write(context.Context, []set.Sample) (int, error)
}

type writableSet interface {
	sampleWriter
	Flush() error
}

type flushableSampleWriter struct {
	sampleWriter
}

func setCmd(rootConfig *rootCmdConfig) *cobra.Command {
	config := &setCmdConfig{rootCmdConfig: rootConfig}
	cmd := &cobra.Command{
		Use:   "set",
		Short: "Manage sets of data",
		Long:  `Manage sets of data`,
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

			output, err := config.OutputWriter(features)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(3)
			}

			inputStream, errStream, err := config.InputStream(features)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(7)
			}

			for s := range inputStream {
				_, err = output.Write(config.Context(), []set.Sample{s})
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
				os.Exit(9)
			}
			config.Logf("Done")
		},
	}
	cmd.PersistentFlags().StringVarP(&(config.setInput), "input", "i", "", "path to an input CSV (.csv) or SQLite3 (.db) file, or a PostgreSQL DB connection URL with data to use to grow the tree (defaults to STDIN, interpreted as CSV)")
	cmd.PersistentFlags().StringVarP(&(config.metadataInput), "metadata", "m", "", "path to a YML file with metadata describing the different features available available on the input file (required)")
	cmd.PersistentFlags().StringVarP(&(config.setOutput), "output", "o", "", "path to a CSV (.csv) or SQLite3 (.db) file, or a PostgreSQL DB connection URL to dump the output set (defaults to STDOUT in CSV)")
	cmd.AddCommand(splitCmd(config))
	return cmd
}

func (scc *setCmdConfig) Validate() error {
	if scc.metadataInput == "" {
		return fmt.Errorf("required metadata flag was not set")
	}
	return nil
}

func (scc *setCmdConfig) OutputWriter(features []feature.Feature) (writableSet, error) {
	var outputFile *os.File
	var err error
	if scc.setOutput != "" {
		if strings.HasPrefix(scc.setOutput, "postgresql://") {
			return scc.PostgreSQLOutputWriter(features)
		}
		if strings.HasSuffix(scc.setOutput, ".db") {
			return scc.Sqlite3OutputWriter(features)
		}
		scc.Logf("Creating %s to dump output set...", scc.setOutput)
		outputFile, err = os.Create(scc.setOutput)
		if err != nil {
			return nil, err
		}
	} else {
		scc.Logf("Using STDOUT to dump output set...")
		outputFile = os.Stdout
	}
	scc.Logf("Preparing to write output set...")
	output, err := csv.NewWriter(outputFile, features)
	if err != nil {
		return nil, err
	}
	return output, nil
}

func (scc *setCmdConfig) InputStream(features []feature.Feature) (<-chan set.Sample, <-chan error, error) {
	var f *os.File
	if scc.setInput == "" {
		scc.Logf("Reading input set from STDIN and dumping it into output set...")
		f = os.Stdin
	} else {
		if strings.HasPrefix(scc.setInput, "postgresql://") {
			return scc.PostgreSQLInputStream(features)
		}
		if strings.HasSuffix(scc.setInput, ".db") {
			return scc.Sqlite3InputStream(features)
		}
		scc.Logf("Opening %s to read input set...", scc.setInput)
		var err error
		f, err = os.Open(scc.setInput)
		if err != nil {
			err = fmt.Errorf("reading input set from %s: %v", scc.setInput, err)
			return nil, nil, err
		}
		scc.Logf("Dumping input set into output set...")
	}
	sampleStream := make(chan set.Sample)
	errStream := make(chan error)
	go func() {
		defer f.Close()
		err := csv.ReadSetBySample(f, features, func(i int, s set.Sample) (bool, error) {
			select {
			case <-scc.Context().Done():
				return false, nil
			case sampleStream <- s:
			}
			return true, nil
		})
		if err != nil {
			go func() {
				errStream <- err
				close(errStream)
			}()
		} else {
			close(errStream)
		}
		close(sampleStream)
	}()
	return sampleStream, errStream, nil
}

func (scc *setCmdConfig) Sqlite3InputStream(features []feature.Feature) (<-chan set.Sample, <-chan error, error) {
	scc.Logf("Creating SQLite3 adapter for file %s to read input set...", scc.setInput)
	adapter, err := sqlite3adapter.New(scc.setInput, 0)
	if err != nil {
		return nil, nil, err
	}
	scc.Logf("Opening set over SQLite3 adapter for file %s to read input set...", scc.setInput)
	set, err := sqlset.OpenSet(scc.Context(), adapter, features)
	if err != nil {
		return nil, nil, err
	}
	sampleStream, errStream := set.Read(scc.Context())
	return sampleStream, errStream, nil
}

func (scc *setCmdConfig) PostgreSQLInputStream(features []feature.Feature) (<-chan set.Sample, <-chan error, error) {
	scc.Logf("Creating PostgreSQL adapter for url %s to read input set...", scc.setInput)
	adapter, err := pgadapter.New(scc.setInput)
	if err != nil {
		return nil, nil, err
	}
	scc.Logf("Opening set over PostgreSQL adapter for url %s to read input set...", scc.setInput)
	set, err := sqlset.OpenSet(scc.Context(), adapter, features)
	if err != nil {
		return nil, nil, err
	}
	sampleStream, errStream := set.Read(scc.Context())
	return sampleStream, errStream, nil
}

func (scc *setCmdConfig) Sqlite3OutputWriter(features []feature.Feature) (writableSet, error) {
	scc.Logf("Creating SQLite3 adapter for file %s to dump output set...", scc.setOutput)
	adapter, err := sqlite3adapter.New(scc.setOutput, 0)
	if err != nil {
		return nil, err
	}
	scc.Logf("Opening set over SQLite3 adapter for file %s to dump output set...", scc.setOutput)
	set, err := sqlset.CreateSet(scc.Context(), adapter, features)
	if err != nil {
		return nil, err
	}
	return &flushableSampleWriter{set}, nil
}

func (scc *setCmdConfig) PostgreSQLOutputWriter(features []feature.Feature) (writableSet, error) {
	scc.Logf("Creating PostgreSQL adapter for url %s to dump output set...", scc.setOutput)
	adapter, err := pgadapter.New(scc.setOutput)
	if err != nil {
		return nil, err
	}
	scc.Logf("Opening set over PostgreSQL adapter for url %s to dump output set...", scc.setOutput)
	set, err := sqlset.CreateSet(scc.Context(), adapter, features)
	if err != nil {
		return nil, err
	}
	return &flushableSampleWriter{set}, nil
}

func (scc *setCmdConfig) Context() context.Context {
	scc.setContextAndCancelFunc()
	return scc.ctx
}

func (scc *setCmdConfig) ContextCancelFunc() context.CancelFunc {
	scc.setContextAndCancelFunc()
	return scc.cancelFunc
}

func (scc *setCmdConfig) setContextAndCancelFunc() {
	if scc.ctx == nil {
		scc.ctx, scc.cancelFunc = context.WithCancel(context.Background())
	}
}

func (fsw *flushableSampleWriter) Flush() error {
	return nil
}
