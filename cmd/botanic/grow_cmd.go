package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pbanos/botanic"
	"github.com/pbanos/botanic/dataset"
	"github.com/pbanos/botanic/dataset/csv"
	"github.com/pbanos/botanic/dataset/dbdataset"
	"github.com/pbanos/botanic/dataset/dbdataset/pgadapter"
	"github.com/pbanos/botanic/dataset/dbdataset/sqlite3adapter"
	"github.com/pbanos/botanic/feature"
	"github.com/pbanos/botanic/feature/yaml"
	"github.com/pbanos/botanic/queue"
	"github.com/pbanos/botanic/tree"
	"github.com/pbanos/botanic/tree/json"
	"github.com/spf13/cobra"
)

type growCmdConfig struct {
	*treeCmdConfig
	dataInput          string
	output             string
	label              string
	pruneStrategy      string
	cpuIntensiveSet    bool
	memoryIntensiveSet bool
	concurrency        int
	ctx                context.Context
}

func growCmd(treeConfig *treeCmdConfig) *cobra.Command {
	config := &growCmdConfig{treeCmdConfig: treeConfig}
	cmd := &cobra.Command{
		Use:   "grow",
		Short: "Grow a tree from a dataset",
		Long:  `Grow a tree from a dataset to predict a certain feature.`,
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

			trainingSet, err := config.trainingSet(features)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(4)
			}
			var label feature.Feature
			for i, f := range features {
				if f.Name() == config.label {
					label = f
					features[i], features[len(features)-1] = features[len(features)-1], features[i]
					break
				}
			}
			if label == nil {
				fmt.Fprintf(os.Stderr, "label feature '%s' is not defined\n", config.label)
				os.Exit(5)
			}
			pruner, err := pruningStrategy(config.pruneStrategy)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(6)
			}
			q := queue.New()
			ns := tree.NewMemoryNodeStore()
			t, err := botanic.Seed(config.Context(), label, features[0:len(features)-1], trainingSet, q, ns)
			count, err := trainingSet.Count(config.Context())
			if err != nil {
				fmt.Fprintf(os.Stderr, "counting training dataset samples: %v\n", err)
				os.Exit(7)
			}
			config.Logf("Growing tree from a dataset with %d samples and %d features to predict %s ...", count, len(features)-1, label.Name())
			ctx, cancel := context.WithCancel(config.Context())
			for i := 0; i < config.concurrency; i++ {
				go func(n int) {
					err := botanic.Work(ctx, t, q, pruner, time.Second)
					if err != nil {
						config.Logf("Worker %d came across an error: %v", n, err)
						cancel()
					}
				}(i)
			}
			err = queue.WaitFor(ctx, q)
			cancel()
			if err != nil {
				fmt.Fprintf(os.Stderr, "growing the tree: %v\n", err)
				os.Exit(8)
			}
			config.Logf("Done")
			config.Logf("%v", t)
			err = outputTree(config.Context(), config.output, t)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(9)
			}
		},
	}
	cmd.PersistentFlags().StringVarP(&(config.dataInput), "input", "i", "", "path to an input CSV (.csv) or SQLite3 (.db) file, or a PostgreSQL DB connection URL with data to use to grow the tree (defaults to STDIN, interpreted as CSV)")
	cmd.PersistentFlags().StringVarP(&(config.output), "output", "o", "", "path to a file to which the generated tree will be written in JSON format (defaults to STDOUT)")
	cmd.PersistentFlags().StringVarP(&(config.label), "label", "l", "", "name of the feature the generated tree should predict (required)")
	cmd.PersistentFlags().StringVarP(&(config.pruneStrategy), "prune", "p", "default", "pruning strategy to apply, the following are valid: default, minimum-information-gain:[VALUE], none")
	cmd.PersistentFlags().BoolVar(&(config.memoryIntensiveSet), "memory-intensive", false, "force the use of memory-intensive subsetting to decrease time at the cost of increasing memory use")
	cmd.PersistentFlags().BoolVar(&(config.cpuIntensiveSet), "cpu-intensive", false, "force the use of cpu-intensive subsetting to decrease memory use at the cost of increasing time")
	cmd.PersistentFlags().IntVar(&(config.concurrency), "concurrency", 1, "limit to concurrent workers on the tree and on DB connections opened at a time (defaults to 1)")
	return cmd
}

func (gcc *growCmdConfig) Validate() error {
	if gcc.metadataInput == "" {
		return fmt.Errorf("required metadata flag was not set")
	}
	if gcc.label == "" {
		return fmt.Errorf("required label flag was not set")
	}
	if gcc.cpuIntensiveSet && gcc.memoryIntensiveSet {
		return fmt.Errorf("cannot set both memory-intensive and cpu-intensive flags at the same time")
	}
	if gcc.concurrency < 1 {
		return fmt.Errorf("cannot grow a tree without workers")
	}
	return nil
}

func (gcc *growCmdConfig) datasetGenerator() csv.SetGenerator {
	if gcc.memoryIntensiveSet {
		return csv.SetGenerator(dataset.NewMemoryIntensive)
	}
	if gcc.cpuIntensiveSet {
		return csv.SetGenerator(dataset.NewCPUIntensive)
	}
	return csv.SetGenerator(dataset.New)
}

func (gcc *growCmdConfig) trainingSet(features []feature.Feature) (dataset.Dataset, error) {
	var f *os.File
	if gcc.dataInput == "" {
		gcc.Logf("Reading training dataset from STDIN...")
		f = os.Stdin
	} else {
		if strings.HasPrefix(gcc.dataInput, "postgresql://") {
			return gcc.PostgreSQLTrainingSet(features)
		}
		if strings.HasSuffix(gcc.dataInput, ".db") {
			return gcc.Sqlite3TrainingSet(features)
		}
		gcc.Logf("Opening %s to read training dataset...", gcc.dataInput)
		var err error
		f, err = os.Open(gcc.dataInput)
		if err != nil {
			err = fmt.Errorf("opening training dataset at %s: %v", gcc.dataInput, err)
			return nil, err
		}
		defer f.Close()
	}
	trainingSet, err := csv.ReadSet(f, features, gcc.datasetGenerator())
	if err != nil {
		return nil, fmt.Errorf("reading training dataset: %v", err)
	}
	return trainingSet, nil
}

func (gcc *growCmdConfig) Sqlite3TrainingSet(features []feature.Feature) (dataset.Dataset, error) {
	gcc.Logf("Creating SQLite3 adapter for file %s to read training dataset...", gcc.dataInput)
	adapter, err := sqlite3adapter.New(gcc.dataInput, gcc.concurrency)
	if err != nil {
		return nil, err
	}
	gcc.Logf("Opening dataset over SQLite3 adapter for file %s to read training dataset...", gcc.dataInput)
	return dbdataset.Open(gcc.Context(), adapter, features)
}

func (gcc *growCmdConfig) PostgreSQLTrainingSet(features []feature.Feature) (dataset.Dataset, error) {
	gcc.Logf("Creating PostgreSQL adapter for url %s to read training dataset...", gcc.dataInput)
	adapter, err := pgadapter.New(gcc.dataInput)
	if err != nil {
		return nil, err
	}
	gcc.Logf("Opening dataset over PostgreSQL adapter for url %s to read training dataset...", gcc.dataInput)
	return dbdataset.Open(gcc.Context(), adapter, features)
}

func (gcc *growCmdConfig) Context() context.Context {
	if gcc.ctx == nil {
		gcc.ctx = context.Background()
	}
	return gcc.ctx
}

func outputTree(ctx context.Context, outputPath string, tree *tree.Tree) error {
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
	return json.WriteJSONTree(ctx, tree, f)
}

func pruningStrategy(ps string) (*botanic.PruningStrategy, error) {
	parsedPS := strings.Split(ps, ":")
	ps = parsedPS[0]
	psParams := parsedPS[1:]
	switch ps {
	case "default":
		return &botanic.PruningStrategy{Pruner: botanic.DefaultPruner(), MinimumEntropy: 0}, nil
	case "none":
		return &botanic.PruningStrategy{Pruner: botanic.NoPruner(), MinimumEntropy: 0}, nil
	case "minimum-information-gain":
		minimum, err := strconv.ParseFloat(psParams[0], 64)
		if err != nil {
			return nil, fmt.Errorf("parsing minimum-information-gain parameter: %v", err)
		}
		return &botanic.PruningStrategy{Pruner: botanic.FixedInformationGainPruner(minimum), MinimumEntropy: 0}, nil
	}
	return nil, fmt.Errorf("unknown pruning strategy %s", ps)
}
