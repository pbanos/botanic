package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/pbanos/botanic/pkg/bio"
	"github.com/pbanos/botanic/pkg/botanic"
	"github.com/spf13/cobra"
)

type growCmdConfig struct {
	dataInput     string
	metadataInput string
	output        string
	classFeature  string
	pruneStrategy string
}

func growCmd(rootConfig *rootCmdConfig) *cobra.Command {
	config := &growCmdConfig{}
	cmd := &cobra.Command{
		Use:   "grow",
		Short: "Grow a tree from a set of data",
		Long:  `Grow a tree from a set of data to predict a certain feature.`,
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
			trainingSet, err := bio.ReadCSVSet(f, features)
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
			pruner, err := pruningStrategy(config.pruneStrategy)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(6)
			}
			p := botanic.New(features[0:len(features)-1], classFeature, pruner)
			l.Logf("Growing tree from a set with %d samples and %d features to predict %s ...", trainingSet.Count(), len(features)-1, classFeature.Name())
			t := p.Grow(trainingSet)
			l.Logf("Done")
			l.Logf("%v", t)
			err = outputTree(config.output, t)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(7)
			}
		},
	}
	cmd.PersistentFlags().StringVarP(&(config.dataInput), "input", "i", "", "path to an input CSV file with data to use to grow the tree (defaults to STDIN)")
	cmd.PersistentFlags().StringVarP(&(config.metadataInput), "metadata", "m", "", "path to a YML file with metadata describing the different features available available on the input file (required)")
	cmd.PersistentFlags().StringVarP(&(config.output), "output", "o", "", "path to a file to which the generated tree will be written in JSON format (defaults to STDOUT)")
	cmd.PersistentFlags().StringVarP(&(config.classFeature), "class-feature", "c", "", "name of the feature the generated tree should predict (required)")
	cmd.PersistentFlags().StringVarP(&(config.pruneStrategy), "prune", "p", "default", "pruning strategy to apply, the following are valid: default, minimum-information-gain:[VALUE], none")
	return cmd
}

func (gcc *growCmdConfig) Validate() error {
	if gcc.metadataInput == "" {
		return fmt.Errorf("required metadata flag was not set")
	}
	if gcc.classFeature == "" {
		return fmt.Errorf("required class-feature flag was not set")
	}
	return nil
}

func outputTree(outputPath string, tree *botanic.Tree) error {
	var f *os.File
	var err error
	if outputPath == "" {
		f = os.Stdout
	} else {
		f, err = os.Create(outputPath)
		if err != nil {
			return err
		}
	}
	defer f.Close()
	return bio.WriteJSONTree(f, tree)
}

func pruningStrategy(ps string) (botanic.Pruner, error) {
	parsedPS := strings.Split(ps, ":")
	ps = parsedPS[0]
	psParams := parsedPS[1:]
	switch ps {
	case "default":
		return botanic.DefaultPruner(), nil
	case "none":
		return botanic.NoPruner(), nil
	case "minimum-information-gain":
		minimum, err := strconv.ParseFloat(psParams[0], 64)
		if err != nil {
			return nil, fmt.Errorf("parsing minimum-information-gain parameter: %v", err)
		}
		return botanic.FixedInformationGainPruner(minimum), nil
	}
	return nil, fmt.Errorf("unknown pruning strategy %s", ps)
}
