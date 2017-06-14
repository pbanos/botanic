package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/pbanos/botanic/pkg/bio"
	"github.com/pbanos/botanic/pkg/bio/sql"
	"github.com/pbanos/botanic/pkg/bio/sql/sqlite3adapter"
	"github.com/pbanos/botanic/pkg/botanic"
	"github.com/spf13/cobra"
)

type setCmdConfig struct {
	*rootCmdConfig
	setInput      string
	metadataInput string
	setOutput     string
}

type sampleWriter interface {
	Write([]botanic.Sample) (int, error)
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
			config.Logf("Reading features from metadata at %s...", config.metadataInput)
			features, err := bio.ReadYMLFeaturesFromFile(config.metadataInput)
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

			done := make(chan struct{})
			defer close(done)
			inputStream, errStream, err := config.InputStream(done, features)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(7)
			}

			for s := range inputStream {
				_, err = output.Write([]botanic.Sample{s})
				if err != nil {
					close(done)
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
	cmd.PersistentFlags().StringVarP(&(config.setInput), "input", "i", "", "path to an input CSV (.csv) or SQLite3 (.db) file with data to use to grow the tree (defaults to STDIN)")
	cmd.PersistentFlags().StringVarP(&(config.metadataInput), "metadata", "m", "", "path to a YML file with metadata describing the different features available available on the input file (required)")
	cmd.PersistentFlags().StringVarP(&(config.setOutput), "output", "o", "", "path to a CSV (.csv) or SQLite3 (.db) file to dump the output set (defaults to STDOUT)")
	cmd.AddCommand(splitCmd(config))
	return cmd
}

func (scc *setCmdConfig) Validate() error {
	if scc.metadataInput == "" {
		return fmt.Errorf("required metadata flag was not set")
	}
	return nil
}

func (scc *setCmdConfig) OutputWriter(features []botanic.Feature) (writableSet, error) {
	var outputFile *os.File
	var err error
	if scc.setOutput != "" {
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
	output, err := bio.NewCSVWriter(outputFile, features)
	if err != nil {
		return nil, err
	}
	return output, nil
}

func (scc *setCmdConfig) InputStream(done <-chan struct{}, features []botanic.Feature) (<-chan botanic.Sample, <-chan error, error) {
	var f *os.File
	if scc.setInput == "" {
		scc.Logf("Reading input set from STDIN and dumping it into output set...")
		f = os.Stdin
	} else {
		if strings.HasSuffix(scc.setInput, ".db") {
			return scc.Sqlite3InputStream(done, features)
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
	sampleStream := make(chan botanic.Sample)
	errStream := make(chan error)
	go func() {
		defer f.Close()
		err := bio.ReadCSVSetBySample(f, features, func(i int, s botanic.Sample) (bool, error) {
			select {
			case <-done:
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

func (scc *setCmdConfig) Sqlite3InputStream(done <-chan struct{}, features []botanic.Feature) (<-chan botanic.Sample, <-chan error, error) {
	scc.Logf("Creating SQLite3 adapter for file %s to read input set...", scc.setInput)
	adapter, err := sqlite3adapter.New(scc.setInput)
	if err != nil {
		return nil, nil, err
	}
	scc.Logf("Opening set over SQLite3 adapter for file %s to read input set...", scc.setInput)
	set, err := sql.OpenSet(adapter, features)
	if err != nil {
		return nil, nil, err
	}
	sampleStream, errStream := set.Read(done)
	return sampleStream, errStream, nil
}

func (scc *setCmdConfig) Sqlite3OutputWriter(features []botanic.Feature) (writableSet, error) {
	scc.Logf("Creating SQLite3 adapter for file %s to dump output set...", scc.setOutput)
	adapter, err := sqlite3adapter.New(scc.setOutput)
	if err != nil {
		return nil, err
	}
	scc.Logf("Opening set over SQLite3 adapter for file %s to dump output set...", scc.setOutput)
	set, err := sql.CreateSet(adapter, features)
	if err != nil {
		return nil, err
	}
	return &flushableSampleWriter{set}, nil
}

func (fsw *flushableSampleWriter) Flush() error {
	return nil
}
