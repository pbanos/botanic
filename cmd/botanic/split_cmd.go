package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/pbanos/botanic/pkg/bio"
	"github.com/pbanos/botanic/pkg/botanic"
	"github.com/spf13/cobra"
)

type splitCmdConfig struct {
	setInput         string
	metadataInput    string
	setOutput        string
	splitOutput      string
	splitProbability int
}

func splitCmd(rootConfig *rootCmdConfig) *cobra.Command {
	config := &splitCmdConfig{}
	cmd := &cobra.Command{
		Use:   "split",
		Short: "Split a set into two sets",
		Long:  `Split a set into an ouput set and a split set`,
		Run: func(cmd *cobra.Command, args []string) {
			err := config.Validate()
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			rootConfig.Logf("Reading features from metadata at %s...", config.metadataInput)
			features, err := bio.ReadYMLFeaturesFromFile(config.metadataInput)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(2)
			}
			rootConfig.Logf("Features from metadata read")

			var outputFile *os.File
			if config.setOutput != "" {
				rootConfig.Logf("Creating %s to dump output set...", config.setOutput)
				outputFile, err = os.Create(config.setOutput)
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(3)
				}
				defer outputFile.Close()
			} else {
				rootConfig.Logf("Using STDOUT to dump output set...")
				outputFile = os.Stdout
			}
			rootConfig.Logf("Preparing to write output set...")
			output, err := bio.NewCSVWriter(outputFile, features)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(4)
			}

			var splitOutputFile *os.File
			rootConfig.Logf("Creating %s to dump split set...", config.splitOutput)
			splitOutputFile, err = os.Create(config.splitOutput)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(5)
			}
			defer splitOutputFile.Close()
			rootConfig.Logf("Preparing to write split output set...")
			splitOutput, err := bio.NewCSVWriter(splitOutputFile, features)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(6)
			}

			randomizer := rand.New(rand.NewSource(time.Now().UnixNano()))
			splitter := func(i int, s botanic.Sample) (bool, error) {
				var err error
				if (100 * randomizer.Float32()) > float32(config.splitProbability) {
					err = output.Write(s)
				} else {
					err = splitOutput.Write(s)
				}
				if err != nil {
					return false, err
				}
				return true, nil
			}

			var f *os.File
			if config.setInput == "" {
				rootConfig.Logf("Reading input set from STDIN and splitting it into output and split output sets...")
				f = os.Stdin
			} else {
				rootConfig.Logf("Opening %s to read input set...", config.setInput)
				f, err = os.Open(config.setInput)
				if err != nil {
					err = fmt.Errorf("reading training set from %s: %v", config.setInput, err)
					fmt.Fprintln(os.Stderr, err)
					os.Exit(7)
				}
				rootConfig.Logf("Splitting input set into output and split output sets...")
			}
			defer f.Close()
			err = bio.ReadCSVSetBySample(f, features, splitter)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(8)
			}
			rootConfig.Logf("Flushing output set...")
			err = output.Flush()
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(9)
			}
			rootConfig.Logf("Flushing split set...")
			err = splitOutput.Flush()
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(10)
			}
			rootConfig.Logf("Done")
			rootConfig.Logf("Input set with %d samples was split into sets with %d and %d samples", output.Count()+splitOutput.Count(), output.Count(), splitOutput.Count())
		},
	}
	cmd.PersistentFlags().StringVarP(&(config.setInput), "input", "i", "", "path to an input CSV file with data to use to grow the tree (defaults to STDIN)")
	cmd.PersistentFlags().StringVarP(&(config.metadataInput), "metadata", "m", "", "path to a YML file with metadata describing the different features available available on the input file (required)")
	cmd.PersistentFlags().StringVarP(&(config.setOutput), "output", "o", "", "path to a file to dump the output set (defaults to STDOUT)")
	cmd.PersistentFlags().IntVarP(&(config.splitProbability), "split-probability", "p", 20, "probability as percent integer that a sample of the set will be assigned to the split set")
	cmd.PersistentFlags().StringVarP(&(config.splitOutput), "split-output", "s", "", "path to a file to dump the output of the split set (required)")
	return cmd
}

func (scc *splitCmdConfig) Validate() error {
	if scc.metadataInput == "" {
		return fmt.Errorf("required metadata flag was not set")
	}
	if scc.splitOutput == "" {
		return fmt.Errorf("required split-output flag was not set")
	}
	if scc.splitProbability <= 0 || scc.splitProbability > 100 {
		return fmt.Errorf("split-percent flag was set to an invalid value: it must be set to an integer between 1 and 100")
	}
	return nil
}
