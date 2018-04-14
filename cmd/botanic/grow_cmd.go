package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pbanos/botanic"
	"github.com/pbanos/botanic/dataset"
	"github.com/pbanos/botanic/dataset/csv"
	jsond "github.com/pbanos/botanic/dataset/json"
	"github.com/pbanos/botanic/dataset/mongodataset"
	"github.com/pbanos/botanic/dataset/sqldataset"
	"github.com/pbanos/botanic/dataset/sqldataset/pgadapter"
	"github.com/pbanos/botanic/dataset/sqldataset/sqlite3adapter"
	"github.com/pbanos/botanic/feature"
	jsonf "github.com/pbanos/botanic/feature/json"
	"github.com/pbanos/botanic/feature/yaml"
	"github.com/pbanos/botanic/queue"
	jsonq "github.com/pbanos/botanic/queue/json"
	"github.com/pbanos/botanic/queue/redisq"
	"github.com/pbanos/botanic/tree"
	jsont "github.com/pbanos/botanic/tree/json"
	"github.com/pbanos/botanic/tree/redisstore"
	"github.com/spf13/cobra"
	mgo "gopkg.in/mgo.v2"
	redis "gopkg.in/redis.v5"
)

type closerQ struct {
	queue.Queue
	closeFn func() error
}

type closerNS struct {
	tree.NodeStore
	closeFn func() error
}

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
	queueBackend       string
	nodeStore          string
	worker             bool
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
			ns, err := config.NodeStore(features)
			if err != nil {
				fmt.Fprintf(os.Stderr, "invalid node store: %v\n", err)
				os.Exit(7)
			}
			defer ns.Close(config.Context())
			q, err := config.Queue(features, ns, trainingSet)
			if err != nil {
				fmt.Fprintf(os.Stderr, "invalid queue backend: %v\n", err)
				os.Exit(8)
			}
			defer q.Stop()
			var t *tree.Tree
			if config.worker {
				t = &tree.Tree{NodeStore: ns, RootID: "", Label: label}
			} else {
				t, err = botanic.Seed(config.Context(), label, features[0:len(features)-1], trainingSet, q, ns)
				if err != nil {
					fmt.Fprintf(os.Stderr, "starting growth of tree: %v\n", err)
					os.Exit(9)
				}
			}
			count, err := trainingSet.Count(config.Context())
			if err != nil {
				fmt.Fprintf(os.Stderr, "counting training dataset samples: %v\n", err)
				os.Exit(10)
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
			if config.verbose {
				go config.reportQueueStats(ctx, q, time.Minute)
			}
			err = queue.WaitFor(ctx, q)
			cancel()
			if err != nil {
				fmt.Fprintf(os.Stderr, "growing the tree: %v\n", err)
				os.Exit(11)
			}
			config.Logf("Done")
			if config.worker {
				return
			}
			config.Logf("%v", t)
			err = outputTree(config.Context(), config.output, features, t)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(12)
			}
		},
	}
	cmd.PersistentFlags().StringVarP(&(config.dataInput), "input", "i", "", "path to an input CSV (.csv) or SQLite3 (.db) file, or a PostgreSQL or MongoDB connection URL with data to use to grow the tree (defaults to STDIN, interpreted as CSV)")
	cmd.PersistentFlags().StringVarP(&(config.output), "output", "o", "", "path to a file to which the generated tree will be written in JSON format (defaults to STDOUT)")
	cmd.PersistentFlags().StringVarP(&(config.label), "label", "l", "", "name of the feature the generated tree should predict (required)")
	cmd.PersistentFlags().StringVarP(&(config.pruneStrategy), "prune", "p", "default", "pruning strategy to apply, the following are valid: default, minimum-information-gain:[VALUE], none")
	cmd.PersistentFlags().StringVar(&(config.queueBackend), "queue-backend", "", "URI for a redis key (that will be used as prefix) so that a redis DB is used as backend for a queue for the tasks needed to develop the tree. If not given an in-memory queue will be used")
	cmd.PersistentFlags().StringVar(&(config.nodeStore), "node-store", "", "URI for a redis key (that will be used as prefix) so that a redis DB is used as backend for a temporary store for the nodes of the tree. If not given an in-memory node store will be used")
	cmd.PersistentFlags().BoolVar(&(config.memoryIntensiveSet), "memory-intensive", false, "force the use of memory-intensive subsetting to decrease time at the cost of increasing memory use")
	cmd.PersistentFlags().BoolVar(&(config.cpuIntensiveSet), "cpu-intensive", false, "force the use of cpu-intensive subsetting to decrease memory use at the cost of increasing time")
	cmd.PersistentFlags().BoolVar(&(config.worker), "contribute", false, "rather than start growing a tree, contribute to an ongoing tree growth by working with an external queue and node store. Requires setting --queue-backend and --node-store")
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
	if gcc.concurrency < 0 {
		return fmt.Errorf("number of workers needs to be greater or equal than 0")
	}
	if gcc.concurrency == 0 && gcc.worker {
		return fmt.Errorf("cannot contribute to an ongoing tree growth with 0 workers")
	}
	if gcc.concurrency == 0 && (gcc.nodeStore == "" || gcc.queueBackend == "") {
		return fmt.Errorf("tree will never be fully grown with 0 concurrent workers and without external node store and queue backend")
	}
	if gcc.queueBackend != "" && !strings.HasPrefix(gcc.queueBackend, "redis://") {
		return fmt.Errorf("unsupported queue backend %q: supported backend must be a redis URI starting with 'redis://' or not provided at all", gcc.queueBackend)
	}
	if gcc.nodeStore != "" && !strings.HasPrefix(gcc.nodeStore, "redis://") {
		return fmt.Errorf("unsupported node store %q: supported backend must be a redis URI starting with 'redis://' or not provided at all", gcc.nodeStore)
	}
	if gcc.worker && gcc.queueBackend == "" {
		return fmt.Errorf("cannot contribute to an ongoing tree growth without an external queue backend")
	}
	if gcc.worker && gcc.nodeStore == "" {
		return fmt.Errorf("cannot contribute to an ongoing tree growth without an external node store")
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
		if strings.HasPrefix(gcc.dataInput, "mongodb://") {
			return gcc.MongoTrainingSet(features)
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
	return sqldataset.Open(gcc.Context(), adapter, features)
}

func (gcc *growCmdConfig) PostgreSQLTrainingSet(features []feature.Feature) (dataset.Dataset, error) {
	gcc.Logf("Creating PostgreSQL adapter for url %s to read training dataset...", gcc.dataInput)
	adapter, err := pgadapter.New(gcc.dataInput)
	if err != nil {
		return nil, err
	}
	gcc.Logf("Opening dataset over PostgreSQL adapter for url %s to read training dataset...", gcc.dataInput)
	return sqldataset.Open(gcc.Context(), adapter, features)
}

func (gcc *growCmdConfig) MongoTrainingSet(features []feature.Feature) (dataset.Dataset, error) {
	gcc.Logf("Opening dataset over MongoDB at url %s to read training dataset...", gcc.dataInput)
	msession, err := mgo.Dial(gcc.dataInput)
	if err != nil {
		return nil, err
	}
	return mongodataset.Open(gcc.Context(), msession, features)
}

func (gcc *growCmdConfig) Context() context.Context {
	if gcc.ctx == nil {
		gcc.ctx = context.Background()
	}
	return gcc.ctx
}

func outputTree(ctx context.Context, outputPath string, features []feature.Feature, tree *tree.Tree) error {
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
	nencdec := jsont.NewNodeEncodeDecoder(jsonf.NewCriteriaEncodeDecoder(features), features)
	return jsont.WriteJSONTree(ctx, tree, nencdec, f)
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

func (gcc *growCmdConfig) Queue(features []feature.Feature, ns tree.NodeStore, ds dataset.Dataset) (queue.Queue, error) {
	if gcc.queueBackend == "" {
		return queue.New(), nil
	}
	rc, keyPrefix, err := parseRedisURI(gcc.queueBackend)
	if err != nil {
		return nil, fmt.Errorf("invalid queue backend %q: %v", gcc.queueBackend, err)
	}
	ced := jsonf.NewCriteriaEncodeDecoder(features)
	ded := jsond.New(ds, gcc.dataInput, ced)
	ted := jsonq.New(features, ded, ns)
	q := redisq.New(keyPrefix, rc, 10*time.Second, time.Second, ted)
	return &closerQ{q, rc.Close}, nil
}

func (cq *closerQ) Stop() error {
	err := cq.Queue.Stop()
	if err != nil {
		return err
	}
	return cq.closeFn()
}

func (cns *closerNS) Close(ctx context.Context) error {
	err := cns.NodeStore.Close(ctx)
	if err != nil {
		return err
	}
	return cns.closeFn()
}

func parseRedisURI(uri string) (*redis.Client, string, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, "", fmt.Errorf("invalid URL: %v", err)
	}
	passwd, _ := u.User.Password()
	pathSegments := strings.Split(u.Path, "/")
	if len(pathSegments) != 3 {
		return nil, "", fmt.Errorf("expected path to a key in a redis DB as path")
	}
	db, err := strconv.Atoi(pathSegments[1])
	if err != nil {
		return nil, "", fmt.Errorf("parsing DB segment %q: %v", pathSegments[1], err)
	}
	return redis.NewClient(&redis.Options{
		Addr:     u.Host,
		Password: passwd,
		DB:       db,
	}), pathSegments[2], nil
}

func (gcc *growCmdConfig) NodeStore(features []feature.Feature) (tree.NodeStore, error) {
	if gcc.nodeStore == "" {
		return tree.NewMemoryNodeStore(), nil
	}
	rc, keyPrefix, err := parseRedisURI(gcc.nodeStore)
	if err != nil {
		return nil, fmt.Errorf("invalid node store %q: %v", gcc.queueBackend, err)
	}
	nencdec := jsont.NewNodeEncodeDecoder(jsonf.NewCriteriaEncodeDecoder(features), features)
	return &closerNS{redisstore.New(rc, keyPrefix, nencdec), rc.Close}, nil
}

func (gcc *growCmdConfig) reportQueueStats(ctx context.Context, q queue.Queue, interval time.Duration) {
	for {
		r, p, err := q.Count(ctx)
		if err != nil {
			gcc.Logf("cannot get queue stats: %v", err)
		} else {
			gcc.Logf("Queue stats: %d pending, %d running", r, p)
		}
		select {
		case <-time.After(interval):
		case <-ctx.Done():
			return
		}
	}
}
