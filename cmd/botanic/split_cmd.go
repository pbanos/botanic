package main

import (
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"

	"github.com/pbanos/botanic/pkg/bio"
	"github.com/pbanos/botanic/pkg/botanic"
	"github.com/spf13/cobra"
)

type splitCmdConfig struct {
	setInput      string
	metadataInput string
	setOutput     string
	splitOutput   string
	splitPercent  int
}

func splitCmd(rootConfig *rootCmdConfig) *cobra.Command {
	config := &splitCmdConfig{}
	cmd := &cobra.Command{
		Use:   "split",
		Short: "Split a set into two sets",
		Long:  `Split a set into two sets`,
		Run: func(cmd *cobra.Command, args []string) {
			err := config.Validate()
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			features, err := bio.ReadYMLFeaturesFromFile(config.metadataInput)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(2)
			}
			var f *os.File
			if config.setInput == "" {
				f = os.Stdin
			} else {
				f, err = os.Open(config.setInput)
				if err != nil {
					err = fmt.Errorf("reading training set from %s: %v", config.setInput, err)
					fmt.Fprintln(os.Stderr, err)
					os.Exit(3)
				}
			}
			defer f.Close()
			inputSet, err := bio.ReadCSVSet(f, features, bio.SetGenerator(botanic.NewSet))
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(4)
			}
			rootConfig.Logf("Loaded set with %d samples...", inputSet.Count())
			var outputSet, splittedSet botanic.Set
			rootConfig.Logf("Splitting set into sets with %d%% and %d%% samples", 100-config.splitPercent, config.splitPercent)
			outputSet, splittedSet = splitSet(inputSet, config.splitPercent, bio.SetGenerator(botanic.NewSet))
			rootConfig.Logf("Generated sets with %d and %d samples respectively", outputSet.Count(), splittedSet.Count())
			rootConfig.Logf("Dumping output set...")
			var outputFile *os.File
			if config.setOutput != "" {
				outputFile, err = os.Create(config.setOutput)
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(5)
				}
				defer outputFile.Close()
			} else {
				outputFile = os.Stdout
			}
			err = dumpCSVSet(outputSet, features, outputFile)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(6)
			}
			if splittedSet != nil && config.splitOutput != "" {
				rootConfig.Logf("Dumping split set...")
				splitOutputFile, err := os.Create(config.splitOutput)
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(7)
				}
				err = dumpCSVSet(splittedSet, features, splitOutputFile)
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(8)
				}
			}
		},
	}
	cmd.PersistentFlags().StringVarP(&(config.setInput), "input", "i", "", "path to an input CSV file with data to use to grow the tree (defaults to STDIN)")
	cmd.PersistentFlags().StringVarP(&(config.metadataInput), "metadata", "m", "", "path to a YML file with metadata describing the different features available available on the input file (required)")
	cmd.PersistentFlags().StringVarP(&(config.setOutput), "output", "o", "", "path to a file to dump the output set (defaults to STDOUT)")
	cmd.PersistentFlags().IntVarP(&(config.splitPercent), "split-percent", "p", 20, "percent of the set that will be dumped to the output")
	cmd.PersistentFlags().StringVarP(&(config.splitOutput), "split-output", "s", "", "path to a file to dump the output of the split set (defaults to no output)")
	return cmd
}

func (scc *splitCmdConfig) Validate() error {
	if scc.metadataInput == "" {
		return fmt.Errorf("required metadata flag was not set")
	}
	if scc.splitPercent <= 0 || scc.splitPercent > 100 {
		return fmt.Errorf("split-percent flag was set to an invalid value: it must be set to an integer between 1 and 100")
	}
	return nil
}

func splitSet(set botanic.Set, percent int, sg bio.SetGenerator) (botanic.Set, botanic.Set) {
	samples := set.Samples()
	for i := range samples {
		j := rand.Intn(i + 1)
		samples[i], samples[j] = samples[j], samples[i]
	}
	threshold := int(math.Ceil(float64(percent*len(samples)) / 100))
	set1 := sg(samples[threshold:])
	set2 := sg(samples[:threshold-1])
	return set1, set2
}

func dumpCSVSet(set botanic.Set, features []botanic.Feature, w io.Writer) error {
	bio.WriteCSVSet(w, set, features)
	return nil
}
