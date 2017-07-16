package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/pbanos/botanic/pkg/bio"
	"github.com/pbanos/botanic/pkg/bio/sql"
	"github.com/pbanos/botanic/pkg/bio/sql/pgadapter"
	"github.com/pbanos/botanic/pkg/bio/sql/sqlite3adapter"
	"github.com/pbanos/botanic/pkg/botanic"
	"github.com/spf13/cobra"
)

type growCmdConfig struct {
	*rootCmdConfig
	dataInput          string
	metadataInput      string
	output             string
	classFeature       string
	pruneStrategy      string
	cpuIntensiveSet    bool
	memoryIntensiveSet bool
	maxDBConns         int
}

func growCmd(rootConfig *rootCmdConfig) *cobra.Command {
	config := &growCmdConfig{rootCmdConfig: rootConfig}
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
			features, err := bio.ReadYMLFeaturesFromFile(config.metadataInput)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(2)
			}

			trainingSet, err := config.trainingSet(features)
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
			p := botanic.New(features[0:len(features)-1], classFeature, pruner, 0)
			count, err := trainingSet.Count()
			if err != nil {
				fmt.Fprintf(os.Stderr, "counting training set samples: %v\n", err)
				os.Exit(7)
			}
			config.Logf("Growing tree from a set with %d samples and %d features to predict %s ...", count, len(features)-1, classFeature.Name())
			t, err := p.Grow(trainingSet)
			if err != nil {
				fmt.Fprintf(os.Stderr, "growing the tree: %v\n", err)
				os.Exit(8)
			}
			config.Logf("Done")
			config.Logf("%v", t)
			err = outputTree(config.output, t)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(9)
			}
		},
	}
	cmd.PersistentFlags().StringVarP(&(config.dataInput), "input", "i", "", "path to an input CSV (.csv) or SQLite3 (.db) file, or a PostgreSQL DB connection URL with data to use to grow the tree (defaults to STDIN, interpreted as CSV)")
	cmd.PersistentFlags().StringVarP(&(config.metadataInput), "metadata", "m", "", "path to a YML file with metadata describing the different features available available on the input file (required)")
	cmd.PersistentFlags().StringVarP(&(config.output), "output", "o", "", "path to a file to which the generated tree will be written in JSON format (defaults to STDOUT)")
	cmd.PersistentFlags().StringVarP(&(config.classFeature), "class-feature", "c", "", "name of the feature the generated tree should predict (required)")
	cmd.PersistentFlags().StringVarP(&(config.pruneStrategy), "prune", "p", "default", "pruning strategy to apply, the following are valid: default, minimum-information-gain:[VALUE], none")
	cmd.PersistentFlags().BoolVar(&(config.memoryIntensiveSet), "memory-intensive", false, "force the use of memory-intensive subsetting to decrease time at the cost of increasing memory use")
	cmd.PersistentFlags().BoolVar(&(config.cpuIntensiveSet), "cpu-intensive", false, "force the use of cpu-intensive subsetting to decrease memory use at the cost of increasing time")
	cmd.PersistentFlags().IntVar(&(config.maxDBConns), "max-db-conns", 0, "limit to DB connections opened at a time (defaults to 0: no limit)")
	return cmd
}

func (gcc *growCmdConfig) Validate() error {
	if gcc.metadataInput == "" {
		return fmt.Errorf("required metadata flag was not set")
	}
	if gcc.classFeature == "" {
		return fmt.Errorf("required class-feature flag was not set")
	}
	if gcc.cpuIntensiveSet && gcc.memoryIntensiveSet {
		return fmt.Errorf("cannot set both memory-intensive and cpu-intensive flags at the same time")
	}
	return nil
}

func (gcc *growCmdConfig) setGenerator() bio.SetGenerator {
	if gcc.memoryIntensiveSet {
		return bio.SetGenerator(botanic.NewMemoryIntensiveSet)
	}
	if gcc.cpuIntensiveSet {
		return bio.SetGenerator(botanic.NewCPUIntensiveSet)
	}
	return bio.SetGenerator(botanic.NewSet)
}

func (gcc *growCmdConfig) trainingSet(features []botanic.Feature) (botanic.Set, error) {
	var f *os.File
	if gcc.dataInput == "" {
		gcc.Logf("Reading training set from STDIN...")
		f = os.Stdin
	} else {
		if strings.HasPrefix(gcc.dataInput, "postgresql://") {
			return gcc.PostgreSQLTrainingSet(features)
		}
		if strings.HasSuffix(gcc.dataInput, ".db") {
			return gcc.Sqlite3TrainingSet(features)
		}
		gcc.Logf("Opening %s to read training set...", gcc.dataInput)
		var err error
		f, err = os.Open(gcc.dataInput)
		if err != nil {
			err = fmt.Errorf("opening training set at %s: %v", gcc.dataInput, err)
			return nil, err
		}
		defer f.Close()
	}
	trainingSet, err := bio.ReadCSVSet(f, features, gcc.setGenerator())
	if err != nil {
		return nil, fmt.Errorf("reading training set: %v", err)
	}
	return trainingSet, nil
}

func (gcc *growCmdConfig) Sqlite3TrainingSet(features []botanic.Feature) (botanic.Set, error) {
	gcc.Logf("Creating SQLite3 adapter for file %s to read training set...", gcc.dataInput)
	adapter, err := sqlite3adapter.New(gcc.dataInput, gcc.maxDBConns)
	if err != nil {
		return nil, err
	}
	gcc.Logf("Opening set over SQLite3 adapter for file %s to read training set...", gcc.dataInput)
	return sql.OpenSet(adapter, features)
}

func (gcc *growCmdConfig) PostgreSQLTrainingSet(features []botanic.Feature) (botanic.Set, error) {
	gcc.Logf("Creating PostgreSQL adapter for url %s to read training set...", gcc.dataInput)
	adapter, err := pgadapter.New(gcc.dataInput)
	if err != nil {
		return nil, err
	}
	gcc.Logf("Opening set over PostgreSQL adapter for url %s to read training set...", gcc.dataInput)
	return sql.OpenSet(adapter, features)
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
