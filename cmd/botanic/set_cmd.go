package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/pbanos/botanic/dataset"
	"github.com/pbanos/botanic/dataset/csv"
	"github.com/pbanos/botanic/dataset/mongodataset"
	"github.com/pbanos/botanic/dataset/sqldataset"
	"github.com/pbanos/botanic/dataset/sqldataset/pgadapter"
	"github.com/pbanos/botanic/dataset/sqldataset/sqlite3adapter"
	"github.com/pbanos/botanic/feature"
	"github.com/pbanos/botanic/feature/yaml"
	"github.com/spf13/cobra"
	mgo "gopkg.in/mgo.v2"
)

type datasetCmdConfig struct {
	*rootCmdConfig
	datasetInput  string
	metadataInput string
	datasetOutput string
	ctx           context.Context
	cancelFunc    context.CancelFunc
}

type sampleWriter interface {
	Write(context.Context, []dataset.Sample) (int, error)
}

type writableSet interface {
	sampleWriter
	Flush() error
}

type flushableSampleWriter struct {
	sampleWriter
}

func datasetCmd(rootConfig *rootCmdConfig) *cobra.Command {
	config := &datasetCmdConfig{rootCmdConfig: rootConfig}
	cmd := &cobra.Command{
		Use:   "dataset",
		Short: "Manage datasets of data",
		Long:  `Manage datasets of data`,
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
				_, err = output.Write(config.Context(), []dataset.Sample{s})
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
				os.Exit(9)
			}
			config.Logf("Done")
		},
	}
	cmd.PersistentFlags().StringVarP(&(config.datasetInput), "input", "i", "", "path to an input CSV (.csv) or SQLite3 (.db) file, or a PostgreSQL or MongoDB connection URL with data to use to grow the tree (defaults to STDIN, interpreted as CSV)")
	cmd.PersistentFlags().StringVarP(&(config.metadataInput), "metadata", "m", "", "path to a YML file with metadata describing the different features available available on the input file (required)")
	cmd.PersistentFlags().StringVarP(&(config.datasetOutput), "output", "o", "", "path to a CSV (.csv) or SQLite3 (.db) file, or a PostgreSQL or MongoDB connection URL to dump the output dataset (defaults to STDOUT in CSV)")
	cmd.AddCommand(splitCmd(config))
	return cmd
}

func (scc *datasetCmdConfig) Validate() error {
	if scc.metadataInput == "" {
		return fmt.Errorf("required metadata flag was not set")
	}
	return nil
}

func (scc *datasetCmdConfig) OutputWriter(features []feature.Feature) (writableSet, error) {
	var outputFile *os.File
	var err error
	if scc.datasetOutput != "" {
		if strings.HasPrefix(scc.datasetOutput, "postgresql://") {
			return scc.PostgreSQLOutputWriter(features)
		}
		if strings.HasPrefix(scc.datasetOutput, "mongodb://") {
			return scc.MongoOutputWriter(features)
		}
		if strings.HasSuffix(scc.datasetOutput, ".db") {
			return scc.Sqlite3OutputWriter(features)
		}
		scc.Logf("Creating %s to dump output dataset...", scc.datasetOutput)
		outputFile, err = os.Create(scc.datasetOutput)
		if err != nil {
			return nil, err
		}
	} else {
		scc.Logf("Using STDOUT to dump output dataset...")
		outputFile = os.Stdout
	}
	scc.Logf("Preparing to write output dataset...")
	output, err := csv.NewWriter(outputFile, features)
	if err != nil {
		return nil, err
	}
	return output, nil
}

func (scc *datasetCmdConfig) InputStream(features []feature.Feature) (<-chan dataset.Sample, <-chan error, error) {
	var f *os.File
	if scc.datasetInput == "" {
		scc.Logf("Reading input dataset from STDIN and dumping it into output dataset...")
		f = os.Stdin
	} else {
		if strings.HasPrefix(scc.datasetInput, "postgresql://") {
			return scc.PostgreSQLInputStream(features)
		}
		if strings.HasPrefix(scc.datasetInput, "mongodb://") {
			return scc.MongoInputStream(features)
		}
		if strings.HasSuffix(scc.datasetInput, ".db") {
			return scc.Sqlite3InputStream(features)
		}
		scc.Logf("Opening %s to read input dataset...", scc.datasetInput)
		var err error
		f, err = os.Open(scc.datasetInput)
		if err != nil {
			err = fmt.Errorf("reading input dataset from %s: %v", scc.datasetInput, err)
			return nil, nil, err
		}
		scc.Logf("Dumping input dataset into output dataset...")
	}
	sampleStream := make(chan dataset.Sample)
	errStream := make(chan error)
	go func() {
		defer f.Close()
		err := csv.ReadSetBySample(f, features, func(i int, s dataset.Sample) (bool, error) {
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

func (scc *datasetCmdConfig) Sqlite3InputStream(features []feature.Feature) (<-chan dataset.Sample, <-chan error, error) {
	scc.Logf("Creating SQLite3 adapter for file %s to read input dataset...", scc.datasetInput)
	adapter, err := sqlite3adapter.New(scc.datasetInput, 0)
	if err != nil {
		return nil, nil, err
	}
	scc.Logf("Opening dataset over SQLite3 adapter for file %s to read input dataset...", scc.datasetInput)
	dataset, err := sqldataset.Open(scc.Context(), adapter, features)
	if err != nil {
		return nil, nil, err
	}
	sampleStream, errStream := dataset.Read(scc.Context())
	return sampleStream, errStream, nil
}

func (scc *datasetCmdConfig) MongoInputStream(features []feature.Feature) (<-chan dataset.Sample, <-chan error, error) {
	scc.Logf("Opening dataset over MongoDB at url %s to read input dataset...", scc.datasetInput)
	msession, err := mgo.Dial(scc.datasetInput)
	if err != nil {
		return nil, nil, err
	}
	dataset, err := mongodataset.Open(scc.Context(), msession, features)
	if err != nil {
		return nil, nil, err
	}
	sampleStream, errStream := dataset.Read(scc.Context())
	return sampleStream, errStream, nil
}

func (scc *datasetCmdConfig) PostgreSQLInputStream(features []feature.Feature) (<-chan dataset.Sample, <-chan error, error) {
	scc.Logf("Creating PostgreSQL adapter for url %s to read input dataset...", scc.datasetInput)
	adapter, err := pgadapter.New(scc.datasetInput)
	if err != nil {
		return nil, nil, err
	}
	scc.Logf("Opening dataset over PostgreSQL adapter for url %s to read input dataset...", scc.datasetInput)
	dataset, err := sqldataset.Open(scc.Context(), adapter, features)
	if err != nil {
		return nil, nil, err
	}
	sampleStream, errStream := dataset.Read(scc.Context())
	return sampleStream, errStream, nil
}

func (scc *datasetCmdConfig) Sqlite3OutputWriter(features []feature.Feature) (writableSet, error) {
	scc.Logf("Creating SQLite3 adapter for file %s to dump output dataset...", scc.datasetOutput)
	adapter, err := sqlite3adapter.New(scc.datasetOutput, 0)
	if err != nil {
		return nil, err
	}
	scc.Logf("Opening dataset over SQLite3 adapter for file %s to dump output dataset...", scc.datasetOutput)
	dataset, err := sqldataset.Create(scc.Context(), adapter, features)
	if err != nil {
		return nil, err
	}
	return &flushableSampleWriter{dataset}, nil
}

func (scc *datasetCmdConfig) PostgreSQLOutputWriter(features []feature.Feature) (writableSet, error) {
	scc.Logf("Creating PostgreSQL adapter for url %s to dump output dataset...", scc.datasetOutput)
	adapter, err := pgadapter.New(scc.datasetOutput)
	if err != nil {
		return nil, err
	}
	scc.Logf("Opening dataset over PostgreSQL adapter for url %s to dump output dataset...", scc.datasetOutput)
	dataset, err := sqldataset.Create(scc.Context(), adapter, features)
	if err != nil {
		return nil, err
	}
	return &flushableSampleWriter{dataset}, nil
}

func (scc *datasetCmdConfig) MongoOutputWriter(features []feature.Feature) (writableSet, error) {
	scc.Logf("Opening dataset over MongoDB at url %s to dump output dataset...", scc.datasetOutput)
	msession, err := mgo.Dial(scc.datasetOutput)
	if err != nil {
		return nil, err
	}
	dataset, err := mongodataset.Open(scc.Context(), msession, features)
	if err != nil {
		return nil, err
	}
	return &flushableSampleWriter{dataset}, nil
}

func (scc *datasetCmdConfig) Context() context.Context {
	scc.setContextAndCancelFunc()
	return scc.ctx
}

func (scc *datasetCmdConfig) ContextCancelFunc() context.CancelFunc {
	scc.setContextAndCancelFunc()
	return scc.cancelFunc
}

func (scc *datasetCmdConfig) setContextAndCancelFunc() {
	if scc.ctx == nil {
		scc.ctx, scc.cancelFunc = context.WithCancel(context.Background())
	}
}

func (fsw *flushableSampleWriter) Flush() error {
	return nil
}
